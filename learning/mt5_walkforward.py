"""
learning/mt5_walkforward.py
Walk-forward validator over MT5 autopilot journal (train vs forward split).

Purpose:
- Validate live/forward performance before increasing risk.
- Produce canary decision + bounded risk sizing recommendation.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import config


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


@dataclass
class WalkForwardDecision:
    canary_mode: bool
    canary_reason: str
    risk_multiplier: float
    train_trades: int
    forward_trades: int
    forward_win_rate: float
    forward_mae: Optional[float]


class MT5WalkForwardValidator:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        cfg_path = str(getattr(config, "MT5_AUTOPILOT_DB_PATH", "") or "").strip()
        self.db_path = Path(db_path or cfg_path or (data_dir / "mt5_autopilot.db"))
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path), timeout=15)

    def _window_metrics(self, conn: sqlite3.Connection, account_key: str, start_iso: str, end_iso: str) -> dict:
        row = conn.execute(
            """
            SELECT COUNT(*),
                   AVG(CASE WHEN outcome IS NOT NULL THEN outcome END),
                   SUM(CASE WHEN pnl IS NOT NULL THEN pnl ELSE 0 END),
                   AVG(CASE WHEN prediction_error IS NOT NULL THEN ABS(prediction_error) END)
              FROM mt5_execution_journal
             WHERE account_key=?
               AND resolved=1
               AND closed_at>=?
               AND closed_at<?
               AND mt5_status IN ('filled','dry_run')
            """,
            (account_key, start_iso, end_iso),
        ).fetchone()
        return {
            "trades": _safe_int(row[0] if row else 0, 0),
            "win_rate": _safe_float(row[1] if row else 0.0, 0.0),
            "net_pnl": _safe_float(row[2] if row else 0.0, 0.0),
            "mae": (None if (not row or row[3] is None) else round(_safe_float(row[3], 0.0), 4)),
        }

    def build_report(self, account_key: str, *, train_days: int = 30, forward_days: int = 7) -> dict:
        out = {
            "ok": False,
            "account_key": account_key,
            "train_days": max(1, int(train_days)),
            "forward_days": max(1, int(forward_days)),
            "train": {},
            "forward": {},
            "canary": {},
            "error": "",
        }
        if not account_key:
            out["error"] = "missing account_key"
            return out
        if not self.db_path.exists():
            out["error"] = "autopilot db not found"
            return out
        now = _utc_now()
        forward_start = now - timedelta(days=out["forward_days"])
        train_start = forward_start - timedelta(days=out["train_days"])
        with self._lock:
            with closing(self._connect()) as conn:
                train = self._window_metrics(conn, account_key, _iso(train_start), _iso(forward_start))
                forward = self._window_metrics(conn, account_key, _iso(forward_start), _iso(now + timedelta(seconds=1)))
        out["train"] = train
        out["forward"] = forward
        out["canary"] = self._evaluate_canary(train, forward)
        out["ok"] = True
        return out

    def _evaluate_canary(self, train: dict, forward: dict) -> dict:
        min_forward = max(3, _safe_int(getattr(config, "MT5_WF_MIN_FORWARD_TRADES", 5), 5))
        min_win_rate = max(0.0, min(1.0, _safe_float(getattr(config, "MT5_WF_MIN_FORWARD_WIN_RATE", 0.45), 0.45)))
        max_mae = max(0.05, min(1.0, _safe_float(getattr(config, "MT5_WF_MAX_FORWARD_MAE", 0.45), 0.45)))
        train_floor = max(3, _safe_int(getattr(config, "MT5_WF_MIN_TRAIN_TRADES", 8), 8))
        train_trades = _safe_int(train.get("trades", 0), 0)
        fwd_trades = _safe_int(forward.get("trades", 0), 0)
        fwd_wr = _safe_float(forward.get("win_rate", 0.0), 0.0)
        fwd_mae = forward.get("mae")
        fwd_mae_val = None if fwd_mae is None else _safe_float(fwd_mae, 0.0)

        canary = False
        reason = "insufficient_samples"
        if train_trades >= train_floor and fwd_trades >= min_forward:
            if fwd_wr >= min_win_rate and (fwd_mae_val is None or fwd_mae_val <= max_mae):
                canary = True
                reason = "forward_pass"
            elif fwd_wr < min_win_rate:
                reason = "low_forward_win_rate"
            else:
                reason = "high_forward_prediction_error"

        mult = self._adaptive_risk_multiplier(train, forward, canary)
        return {
            "canary_mode": not canary,
            "canary_pass": canary,
            "reason": reason,
            "risk_multiplier": round(mult, 4),
            "thresholds": {
                "min_train_trades": train_floor,
                "min_forward_trades": min_forward,
                "min_forward_win_rate": min_win_rate,
                "max_forward_mae": max_mae,
            },
        }

    def _adaptive_risk_multiplier(self, train: dict, forward: dict, canary_pass: bool) -> float:
        if not bool(getattr(config, "MT5_ADAPTIVE_SIZING_ENABLED", True)):
            return 1.0
        mult = 1.0
        min_mult = max(0.1, _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_MIN_MULT", 0.25), 0.25))
        max_mult = max(min_mult, _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_MAX_MULT", 1.25), 1.25))
        canary_mult = max(min_mult, min(max_mult, _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_CANARY_MULT", 0.35), 0.35)))
        target_wr = max(0.1, min(0.9, _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_TARGET_WIN_RATE", 0.52), 0.52)))
        target_mae = max(0.05, min(1.0, _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_TARGET_MAE", 0.35), 0.35)))

        fwd_trades = _safe_int(forward.get("trades", 0), 0)
        fwd_wr = _safe_float(forward.get("win_rate", 0.0), 0.0)
        fwd_mae = forward.get("mae")
        mae = None if fwd_mae is None else _safe_float(fwd_mae, 0.0)

        if not canary_pass:
            mult = min(mult, canary_mult)
        if fwd_trades > 0:
            mult *= max(0.5, min(1.5, 1.0 + ((fwd_wr - target_wr) * 1.5)))
            if mae is not None:
                # Larger MAE => worse calibration => reduce size.
                mult *= max(0.4, min(1.3, target_mae / max(0.05, mae)))
        # Gentle boost if train set strong and forward sample is still small.
        if fwd_trades < max(3, _safe_int(getattr(config, "MT5_WF_MIN_FORWARD_TRADES", 5), 5)):
            t_wr = _safe_float(train.get("win_rate", 0.0), 0.0)
            mult *= max(0.7, min(1.05, 0.9 + (t_wr * 0.2)))

        return max(min_mult, min(max_mult, mult))

    def decision(self, account_key: str, *, train_days: int = 30, forward_days: int = 7) -> WalkForwardDecision:
        rpt = self.build_report(account_key, train_days=train_days, forward_days=forward_days)
        if not rpt.get("ok"):
            canary_mult = _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_CANARY_MULT", 0.35), 0.35)
            return WalkForwardDecision(True, str(rpt.get("error") or "walkforward_unavailable"), canary_mult, 0, 0, 0.0, None)
        canary = dict(rpt.get("canary", {}) or {})
        train = dict(rpt.get("train", {}) or {})
        forward = dict(rpt.get("forward", {}) or {})
        return WalkForwardDecision(
            canary_mode=bool(canary.get("canary_mode", True)),
            canary_reason=str(canary.get("reason", "unknown")),
            risk_multiplier=float(canary.get("risk_multiplier", 1.0) or 1.0),
            train_trades=_safe_int(train.get("trades", 0), 0),
            forward_trades=_safe_int(forward.get("trades", 0), 0),
            forward_win_rate=_safe_float(forward.get("win_rate", 0.0), 0.0),
            forward_mae=(None if forward.get("mae") is None else _safe_float(forward.get("mae"), 0.0)),
        )


mt5_walkforward = MT5WalkForwardValidator()

