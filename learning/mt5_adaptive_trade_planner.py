"""
learning/mt5_adaptive_trade_planner.py
Adaptive MT5 execution planner (bounded, explainable).

Purpose:
- Replace fixed TP/SL/RR assumptions with symbol-aware, regime-aware execution planning.
- Use both live execution context (spread, tick price) and recent realized outcomes
  from mt5_autopilot forward-test journal.
- Keep changes bounded to avoid destabilizing the strategy.
- Integrate neural probability from NeuralBrain to further adjust TP/SL/size.
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import config

logger = logging.getLogger(__name__)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_float_opt(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(v)))


@dataclass
class AdaptiveExecutionPlan:
    ok: bool
    applied: bool
    reason: str
    signal_symbol: str = ""
    broker_symbol: str = ""
    account_key: str = ""
    rr_target: Optional[float] = None
    rr_base: Optional[float] = None
    stop_scale: float = 1.0
    size_multiplier: float = 1.0
    entry: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None
    take_profit_2: Optional[float] = None
    take_profit_3: Optional[float] = None
    factors: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "ok": bool(self.ok),
            "applied": bool(self.applied),
            "reason": str(self.reason or ""),
            "signal_symbol": str(self.signal_symbol or ""),
            "broker_symbol": str(self.broker_symbol or ""),
            "account_key": str(self.account_key or ""),
            "rr_target": self.rr_target,
            "rr_base": self.rr_base,
            "stop_scale": self.stop_scale,
            "size_multiplier": self.size_multiplier,
            "entry": self.entry,
            "stop_loss": self.stop_loss,
            "take_profit_1": self.take_profit_1,
            "take_profit_2": self.take_profit_2,
            "take_profit_3": self.take_profit_3,
            "factors": dict(self.factors or {}),
        }


class MT5AdaptiveTradePlanner:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        cfg = str(getattr(config, "MT5_AUTOPILOT_DB_PATH", "") or "").strip()
        self.db_path = Path(db_path or cfg or (data_dir / "mt5_autopilot.db"))
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[float, dict]] = {}
        self._cache_ttl_sec = 60.0

    @property
    def enabled(self) -> bool:
        return bool(getattr(config, "MT5_ADAPTIVE_EXECUTION_ENABLED", True))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10)
        conn.execute("PRAGMA query_only=1")
        return conn

    @staticmethod
    def _symbol_family(signal_symbol: str, broker_symbol: str) -> str:
        s = str(signal_symbol or broker_symbol or "").upper()
        b = str(broker_symbol or signal_symbol or "").upper()
        if "XAU" in s or "XAU" in b or "XAG" in s or "XAG" in b:
            return "metal"
        if "/" in s and s.endswith("/USDT"):
            return "crypto"
        if any(b.endswith(x) for x in ("USD", "USDT")) and any(b.startswith(x) for x in (
            "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "BNB", "LTC", "BCH", "DOT", "LINK", "TRX", "UNI", "ATOM", "POL", "HBAR", "PEPE", "SHIB", "PAXG",
        )):
            return "crypto"
        if b in {"US500", "US30", "USTEC", "US2000", "UK100", "JP225", "DE40", "GER40", "SPX500", "NAS100"}:
            return "index"
        if len(b) in (6, 7) and any(b.endswith(x) for x in ("USD", "JPY", "EUR", "GBP", "CHF", "AUD", "NZD", "CAD")):
            return "fx"
        if "." in s or "." in b:
            return "stock"
        return "other"

    def symbol_family(self, signal_symbol: str, broker_symbol: str) -> str:
        return self._symbol_family(signal_symbol, broker_symbol)

    def _symbol_stats(self, account_key: str, signal_symbol: str, broker_symbol: str, lookback_days: int) -> dict:
        if not account_key or not self.db_path.exists():
            return {"samples": 0}
        key = f"{account_key}|{str(signal_symbol).upper()}|{str(broker_symbol).upper()}|{int(lookback_days)}"
        now_ts = _utc_now().timestamp()
        cached = self._cache.get(key)
        if cached and (now_ts - float(cached[0])) <= self._cache_ttl_sec:
            return dict(cached[1] or {})

        since = _iso(_utc_now() - timedelta(days=max(1, int(lookback_days))))
        out = {
            "samples": 0,
            "win_rate": None,
            "tp_rate": None,
            "sl_rate": None,
            "mae": None,
            "avg_rr": None,
            "avg_conf": None,
            "avg_pnl": None,
        }
        try:
            with self._lock:
                with closing(self._connect()) as conn:
                    row = conn.execute(
                        """
                        SELECT COUNT(*),
                               AVG(CASE WHEN outcome IS NOT NULL THEN outcome END),
                               AVG(CASE WHEN close_reason='TP' THEN 1.0 ELSE 0.0 END),
                               AVG(CASE WHEN close_reason='SL' THEN 1.0 ELSE 0.0 END),
                               AVG(CASE WHEN prediction_error IS NOT NULL THEN ABS(prediction_error) END),
                               AVG(CASE WHEN risk_reward IS NOT NULL THEN risk_reward END),
                               AVG(CASE WHEN confidence IS NOT NULL THEN confidence END),
                               AVG(CASE WHEN pnl IS NOT NULL THEN pnl END)
                          FROM mt5_execution_journal
                         WHERE account_key=?
                           AND resolved=1
                           AND closed_at>=?
                           AND (
                                UPPER(COALESCE(broker_symbol,''))=?
                                OR UPPER(COALESCE(signal_symbol,''))=?
                           )
                        """,
                        (
                            account_key,
                            since,
                            str(broker_symbol or "").upper(),
                            str(signal_symbol or "").upper(),
                        ),
                    ).fetchone()
            if row:
                out["samples"] = _safe_int(row[0], 0)
                out["win_rate"] = (None if row[1] is None else round(_safe_float(row[1], 0.0), 4))
                out["tp_rate"] = (None if row[2] is None else round(_safe_float(row[2], 0.0), 4))
                out["sl_rate"] = (None if row[3] is None else round(_safe_float(row[3], 0.0), 4))
                out["mae"] = (None if row[4] is None else round(_safe_float(row[4], 0.0), 4))
                out["avg_rr"] = (None if row[5] is None else round(_safe_float(row[5], 0.0), 4))
                out["avg_conf"] = (None if row[6] is None else round(_safe_float(row[6], 0.0), 2))
                out["avg_pnl"] = (None if row[7] is None else round(_safe_float(row[7], 0.0), 6))
        except Exception as e:
            logger.debug("[MT5AdaptiveExec] stats query failed: %s", e)

        self._cache[key] = (now_ts, dict(out))
        return out

    def symbol_behavior_stats(self, account_key: str, signal_symbol: str, broker_symbol: str, lookback_days: int = 45) -> dict:
        return self._symbol_stats(
            account_key=str(account_key or ""),
            signal_symbol=str(signal_symbol or ""),
            broker_symbol=str(broker_symbol or ""),
            lookback_days=max(1, int(lookback_days or 45)),
        )

    def _symbol_direction_stats(
        self,
        account_key: str,
        signal_symbol: str,
        broker_symbol: str,
        direction: str,
        lookback_days: int,
    ) -> dict:
        if not account_key or not self.db_path.exists():
            return {"samples": 0}
        dir_key = str(direction or "").strip().lower()
        if dir_key not in {"long", "short"}:
            return {"samples": 0}
        key = (
            f"dir|{account_key}|{str(signal_symbol).upper()}|{str(broker_symbol).upper()}|"
            f"{dir_key}|{int(lookback_days)}"
        )
        now_ts = _utc_now().timestamp()
        cached = self._cache.get(key)
        if cached and (now_ts - float(cached[0])) <= self._cache_ttl_sec:
            return dict(cached[1] or {})

        since = _iso(_utc_now() - timedelta(days=max(1, int(lookback_days))))
        out = {"samples": 0, "win_rate": None, "avg_pnl": None, "avg_conf": None}
        try:
            with self._lock:
                with closing(self._connect()) as conn:
                    row = conn.execute(
                        """
                        SELECT COUNT(*),
                               AVG(CASE WHEN outcome IS NOT NULL THEN outcome END),
                               AVG(CASE WHEN pnl IS NOT NULL THEN pnl END),
                               AVG(CASE WHEN confidence IS NOT NULL THEN confidence END)
                          FROM mt5_execution_journal
                         WHERE account_key=?
                           AND resolved=1
                           AND closed_at>=?
                           AND LOWER(COALESCE(direction,''))=?
                           AND (
                                UPPER(COALESCE(broker_symbol,''))=?
                                OR UPPER(COALESCE(signal_symbol,''))=?
                           )
                        """,
                        (
                            account_key,
                            since,
                            dir_key,
                            str(broker_symbol or "").upper(),
                            str(signal_symbol or "").upper(),
                        ),
                    ).fetchone()
            if row:
                out["samples"] = _safe_int(row[0], 0)
                out["win_rate"] = (None if row[1] is None else round(_safe_float(row[1], 0.0), 4))
                out["avg_pnl"] = (None if row[2] is None else round(_safe_float(row[2], 0.0), 6))
                out["avg_conf"] = (None if row[3] is None else round(_safe_float(row[3], 0.0), 2))
        except Exception as e:
            logger.debug("[MT5AdaptiveExec] direction stats query failed: %s", e)

        self._cache[key] = (now_ts, dict(out))
        return out

    @staticmethod
    def _pattern_bucket(pattern: str) -> str:
        p = str(pattern or "").strip().upper()
        if not p:
            return "UNKNOWN"
        if "SCALP_FLOW_FORCE" in p:
            return "SCALP_FORCE"
        if "BEHAVIOR" in p:
            return "BEHAVIORAL"
        if "SWEEP" in p:
            return "SWEEP"
        if "BREAKOUT" in p:
            return "BREAKOUT"
        if "RETEST" in p:
            return "RETEST"
        if "MEAN" in p:
            return "MEAN_REVERT"
        tok = p.replace("-", "_").split("_")
        return (tok[0] if tok else p)[:24]

    @staticmethod
    def _session_bucket(session: str) -> str:
        s = str(session or "").strip().lower().replace(" ", "_")
        if not s:
            return "unknown"
        if "overlap" in s:
            return "overlap"
        if "new_york" in s or "ny" in s:
            return "new_york"
        if "london" in s:
            return "london"
        if "asian" in s or "tokyo" in s:
            return "asian"
        return s[:24]

    @staticmethod
    def _parse_utc_hour_windows(spec: str) -> list[tuple[int, int]]:
        windows: list[tuple[int, int]] = []
        raw = str(spec or "").strip()
        if not raw:
            return windows
        for part in raw.split(","):
            txt = str(part or "").strip()
            if not txt:
                continue
            if "-" not in txt:
                try:
                    h = int(float(txt))
                except Exception:
                    continue
                if 0 <= h <= 23:
                    windows.append((h, h + 1))
                continue
            a, b = txt.split("-", 1)
            try:
                start = int(float(a.strip()))
                end = int(float(b.strip()))
            except Exception:
                continue
            if not (0 <= start <= 23 and 0 <= end <= 23):
                continue
            if start == end:
                windows.append((0, 24))
            elif start < end:
                windows.append((start, end))
            else:
                windows.append((start, 24))
                windows.append((0, end))
        return windows

    @staticmethod
    def _hour_in_windows(hour_utc: int, windows: list[tuple[int, int]]) -> bool:
        h = int(hour_utc) % 24
        for lo, hi in windows:
            if int(lo) <= h < int(hi):
                return True
        return False

    @staticmethod
    def _behavior_tokens(
        *,
        source: str,
        direction: str,
        pattern: str,
        session: str,
        trend: str,
        raw_scores: dict,
        use_volume_profile: bool,
    ) -> set[str]:
        raw = dict(raw_scores or {})
        toks: set[str] = set()
        src = str(source or "").strip().lower()
        dr = str(direction or "").strip().lower()
        if src:
            toks.add(f"src:{src}")
        if dr in {"long", "short"}:
            toks.add(f"dir:{dr}")

        toks.add(f"setup:{MT5AdaptiveTradePlanner._pattern_bucket(pattern)}")
        toks.add(f"session:{MT5AdaptiveTradePlanner._session_bucket(session)}")

        t = str(trend or raw.get("scalp_force_trend_h1") or "").strip().lower()
        if t:
            if "bull" in t:
                toks.add("trend:bullish")
            elif "bear" in t:
                toks.add("trend:bearish")
            elif "range" in t:
                toks.add("trend:ranging")

        if "scalp_force_mode" in raw:
            toks.add(f"force:{1 if bool(raw.get('scalp_force_mode')) else 0}")
        if "scalp_m1_micro_entry" in raw:
            toks.add(f"m1_micro:{1 if bool(raw.get('scalp_m1_micro_entry')) else 0}")

        if dr == "long" and ("scalp_force_m1_aligned_long" in raw):
            toks.add(f"m1_align:{1 if bool(raw.get('scalp_force_m1_aligned_long')) else 0}")
        elif dr == "short" and ("scalp_force_m1_aligned_short" in raw):
            toks.add(f"m1_align:{1 if bool(raw.get('scalp_force_m1_aligned_short')) else 0}")

        momentum = _safe_float_opt(raw.get("scalp_force_momentum"))
        snap = raw.get("scalp_m1_snapshot")
        if isinstance(snap, dict):
            if momentum is None:
                momentum = _safe_float_opt(snap.get("momentum"))
            close_ = _safe_float_opt(snap.get("close"))
            ema9_ = _safe_float_opt(snap.get("ema9"))
            atr14_ = abs(_safe_float_opt(snap.get("atr14")) or 0.0)
            rsi_ = _safe_float_opt(snap.get("rsi14"))
            if close_ is not None and ema9_ is not None:
                toks.add("m1_close_ema9:above" if close_ >= ema9_ else "m1_close_ema9:below")
                if atr14_ > 1e-9:
                    dist = abs(close_ - ema9_) / atr14_
                    if dist < 0.20:
                        toks.add("m1_dist_ema9:near")
                    elif dist < 0.55:
                        toks.add("m1_dist_ema9:mid")
                    else:
                        toks.add("m1_dist_ema9:far")
            if rsi_ is not None:
                if rsi_ >= 60.0:
                    toks.add("m1_rsi:high")
                elif rsi_ <= 40.0:
                    toks.add("m1_rsi:low")
                else:
                    toks.add("m1_rsi:mid")

        if momentum is not None:
            if momentum > 0.05:
                toks.add("m1_mom:up")
            elif momentum < -0.05:
                toks.add("m1_mom:down")
            else:
                toks.add("m1_mom:flat")

        if "xau_event_shock_active" in raw:
            toks.add(f"shock_active:{1 if bool(raw.get('xau_event_shock_active')) else 0}")
        if "xau_event_shock_kill_switch" in raw:
            toks.add(f"shock_kill:{1 if bool(raw.get('xau_event_shock_kill_switch')) else 0}")
        score = _safe_float_opt(raw.get("xau_event_shock_score"))
        if score is not None:
            if score >= 12:
                toks.add("shock_score:high")
            elif score >= 8:
                toks.add("shock_score:mid")
            else:
                toks.add("shock_score:low")

        # Volume profile proxy / liquidity map context (only if scanner emits it).
        if use_volume_profile:
            liq_prob = _safe_float_opt(raw.get("xau_liq_sweep_prob"))
            if liq_prob is not None:
                if liq_prob >= 75:
                    toks.add("liq_sweep_prob:high")
                elif liq_prob >= 45:
                    toks.add("liq_sweep_prob:mid")
                else:
                    toks.add("liq_sweep_prob:low")
            if "xau_guard_near_round" in raw:
                toks.add(f"near_round:{1 if bool(raw.get('xau_guard_near_round')) else 0}")
            if "xau_guard_no_chase" in raw:
                toks.add(f"no_chase:{1 if bool(raw.get('xau_guard_no_chase')) else 0}")
            if "xau_guard_sweep" in raw:
                toks.add(f"sweep:{1 if bool(raw.get('xau_guard_sweep')) else 0}")
        return toks

    def _behavior_layer_stats(
        self,
        *,
        account_key: str,
        signal,
        broker_symbol: str,
        source: str,
        direction: str,
        family: str,
    ) -> dict:
        out = {
            "enabled": bool(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_ENABLED", True)),
            "applied": False,
            "edge": 0.0,
            "rr_adj": 0.0,
            "size_adj": 0.0,
            "samples": 0,
            "matched_tokens": 0,
            "tokens_used": [],
            "source_mode": "none",
            "volume_profile_enabled": bool(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_USE_VOLUME_PROFILE", True)),
        }
        if not out["enabled"] or (not account_key) or (not self.db_path.exists()):
            return out

        lookback_days = max(7, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_LOOKBACK_DAYS", 30) or 30))
        min_trades = max(6, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MIN_TRADES", 10) or 10))
        min_token_samples = max(3, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MIN_TOKEN_SAMPLES", 5) or 5))
        max_rr_adj = _clamp(float(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MAX_RR_ADJ", 0.12) or 0.12), 0.02, 0.30)
        max_size_adj = _clamp(float(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_MAX_SIZE_ADJ", 0.22) or 0.22), 0.05, 0.45)
        target_wr = _clamp(float(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_TARGET_WIN_RATE", 0.50) or 0.50), 0.35, 0.75)
        strict_source = bool(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_SOURCE_STRICT", True))

        sig_symbol = str(getattr(signal, "symbol", "") or "")
        src_key = str(source or "").strip().lower()
        dir_key = str(direction or "").strip().lower()
        if dir_key not in {"long", "short"}:
            return out

        cache_key = (
            f"bg|{account_key}|{str(sig_symbol).upper()}|{str(broker_symbol).upper()}|{dir_key}|"
            f"{src_key}|{lookback_days}|{int(strict_source)}"
        )
        now_ts = _utc_now().timestamp()
        cached = self._cache.get(cache_key)
        if cached and (now_ts - float(cached[0])) <= self._cache_ttl_sec:
            return dict(cached[1] or {})

        since = _iso(_utc_now() - timedelta(days=lookback_days))
        rows: list[tuple[str, str, float]] = []
        try:
            with self._lock:
                with closing(self._connect()) as conn:
                    base_sql = """
                        SELECT COALESCE(source,''), COALESCE(extra_json,''), COALESCE(pnl,0.0)
                          FROM mt5_execution_journal
                         WHERE account_key=?
                           AND resolved=1
                           AND mt5_status IN ('filled','dry_run')
                           AND closed_at>=?
                           AND LOWER(COALESCE(direction,''))=?
                           AND (
                                UPPER(COALESCE(broker_symbol,''))=?
                                OR UPPER(COALESCE(signal_symbol,''))=?
                           )
                    """
                    params = [
                        account_key,
                        since,
                        dir_key,
                        str(broker_symbol or "").upper(),
                        str(sig_symbol or "").upper(),
                    ]
                    if strict_source and src_key:
                        base_sql += " AND LOWER(COALESCE(source,''))=?"
                        params.append(src_key)
                    base_sql += " ORDER BY closed_at DESC LIMIT 1200"
                    rows = [(str(r[0] or ""), str(r[1] or ""), _safe_float(r[2], 0.0)) for r in conn.execute(base_sql, params).fetchall()]
                    if strict_source and src_key and len(rows) < min_trades:
                        params2 = [
                            account_key,
                            since,
                            dir_key,
                            str(broker_symbol or "").upper(),
                            str(sig_symbol or "").upper(),
                        ]
                        rows = [(str(r[0] or ""), str(r[1] or ""), _safe_float(r[2], 0.0)) for r in conn.execute(
                            base_sql.replace(" AND LOWER(COALESCE(source,''))=?", ""),
                            params2,
                        ).fetchall()]
                        out["source_mode"] = "fallback_all_sources"
                    else:
                        out["source_mode"] = ("strict_source" if (strict_source and src_key) else "all_sources")
        except Exception as e:
            logger.debug("[MT5AdaptiveExec] behavior layer query failed: %s", e)
            self._cache[cache_key] = (now_ts, dict(out))
            return out

        out["samples"] = len(rows)
        if len(rows) < min_trades:
            self._cache[cache_key] = (now_ts, dict(out))
            return out

        raw_now = dict(getattr(signal, "raw_scores", {}) or {})
        tokens_now = self._behavior_tokens(
            source=src_key,
            direction=dir_key,
            pattern=str(getattr(signal, "pattern", "") or ""),
            session=str(getattr(signal, "session", "") or ""),
            trend=str(getattr(signal, "trend", "") or ""),
            raw_scores=raw_now,
            use_volume_profile=bool(out["volume_profile_enabled"]),
        )
        if not tokens_now:
            self._cache[cache_key] = (now_ts, dict(out))
            return out

        token_stats: dict[str, dict] = {}
        pnl_vals: list[float] = []
        for row_source, extra_json, pnl in rows:
            payload: dict = {}
            try:
                payload = json.loads(str(extra_json or "{}"))
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            raw_hist = dict(payload.get("raw_scores") or {})
            toks = self._behavior_tokens(
                source=str(row_source or ""),
                direction=dir_key,
                pattern=str(payload.get("pattern", "") or ""),
                session=str(payload.get("session", "") or ""),
                trend=str(raw_hist.get("scalp_force_trend_h1", "") or ""),
                raw_scores=raw_hist,
                use_volume_profile=bool(out["volume_profile_enabled"]),
            )
            if not toks:
                continue
            pnl_vals.append(float(pnl))
            for tok in toks:
                st = token_stats.setdefault(tok, {"n": 0, "wins": 0, "sum_pnl": 0.0})
                st["n"] = int(st["n"]) + 1
                st["wins"] = int(st["wins"]) + (1 if float(pnl) > 0 else 0)
                st["sum_pnl"] = float(st["sum_pnl"]) + float(pnl)

        if not token_stats:
            self._cache[cache_key] = (now_ts, dict(out))
            return out

        base_scale = 20.0 if family == "metal" else 6.0
        mean_abs_pnl = (sum(abs(x) for x in pnl_vals) / len(pnl_vals)) if pnl_vals else base_scale
        pnl_scale = max(base_scale, mean_abs_pnl * 1.6)

        weighted_sum = 0.0
        total_w = 0.0
        used_tokens: list[str] = []
        for tok in sorted(tokens_now):
            st = token_stats.get(tok)
            if not st:
                continue
            n = int(st.get("n", 0) or 0)
            if n < min_token_samples:
                continue
            wr = float(st.get("wins", 0) or 0) / max(1, n)
            avg_pnl = float(st.get("sum_pnl", 0.0) or 0.0) / max(1, n)
            wr_term = _clamp((wr - target_wr) / max(0.10, (1.0 - target_wr)), -1.0, 1.0)
            pnl_term = _clamp(avg_pnl / max(1e-9, pnl_scale), -1.0, 1.0)
            edge_tok = (0.75 * wr_term) + (0.25 * pnl_term)
            # Weight by sample confidence (log-scaled, bounded).
            w = _clamp(math.log1p(n) / math.log1p(max(10, min_token_samples * 4)), 0.15, 1.0)
            weighted_sum += edge_tok * w
            total_w += w
            used_tokens.append(f"{tok}:{n}")

        if total_w <= 1e-9:
            self._cache[cache_key] = (now_ts, dict(out))
            return out

        edge = _clamp(weighted_sum / total_w, -1.0, 1.0)
        out["edge"] = round(float(edge), 4)
        out["matched_tokens"] = len(used_tokens)
        out["tokens_used"] = used_tokens[:16]
        out["rr_adj"] = round(float(_clamp(edge * max_rr_adj, -max_rr_adj, max_rr_adj)), 4)
        out["size_adj"] = round(float(_clamp(edge * max_size_adj, -max_size_adj, max_size_adj)), 4)
        out["applied"] = bool(out["matched_tokens"] >= 2)
        self._cache[cache_key] = (now_ts, dict(out))
        return out

    @staticmethod
    def _is_xau_news_calm(raw_scores: dict) -> bool:
        raw = dict(raw_scores or {})
        active = raw.get("xau_event_shock_active", None)
        kill = raw.get("xau_event_shock_kill_switch", None)
        score = _safe_float_opt(raw.get("xau_event_shock_score"))
        has_any = any(v is not None for v in (active, kill, score))
        if not has_any:
            return True
        score_val = (float(score) if score is not None else 0.0)
        return (not bool(active)) and (not bool(kill)) and (score_val <= 7.0)

    @staticmethod
    def _session_score(session: str) -> float:
        s = str(session or "").lower()
        score = 0.0
        if "overlap" in s:
            score += 0.12
        if "new_york" in s:
            score += 0.05
        if "london" in s:
            score += 0.05
        if "asian" in s and "crypto" not in s:
            score -= 0.03
        return score

    def plan_execution(
        self,
        *,
        signal,
        account_key: str,
        broker_symbol: str,
        execution_price: float,
        bid: float,
        ask: float,
        point: float,
        source: str = "",
        neural_prob: Optional[float] = None,  # 0.0-1.0 from NeuralBrain.predict_probability()
    ) -> AdaptiveExecutionPlan:
        sig_symbol = str(getattr(signal, "symbol", "") or "")
        if not self.enabled:
            return AdaptiveExecutionPlan(False, False, "disabled", signal_symbol=sig_symbol, broker_symbol=broker_symbol, account_key=account_key)
        try:
            direction = str(getattr(signal, "direction", "") or "").lower()
            if direction not in {"long", "short"}:
                return AdaptiveExecutionPlan(False, False, "invalid_direction", signal_symbol=sig_symbol, broker_symbol=broker_symbol, account_key=account_key)
            price = _safe_float(execution_price, 0.0)
            entry0 = _safe_float(getattr(signal, "entry", price), price)
            sl0 = _safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
            rr0 = _safe_float(getattr(signal, "risk_reward", 2.0), 2.0)
            if price <= 0 or sl0 <= 0:
                return AdaptiveExecutionPlan(False, False, "invalid_prices", signal_symbol=sig_symbol, broker_symbol=broker_symbol, account_key=account_key)
            base_risk = abs(entry0 - sl0)
            if base_risk <= max(1e-12, float(point or 0.0)):
                return AdaptiveExecutionPlan(False, False, "tiny_base_risk", signal_symbol=sig_symbol, broker_symbol=broker_symbol, account_key=account_key)

            family = self._symbol_family(sig_symbol, broker_symbol)
            atr = abs(_safe_float(getattr(signal, "atr", 0.0), 0.0))
            atr_pct = (atr / price * 100.0) if (atr > 0 and price > 0) else 0.0
            spread = max(0.0, _safe_float(ask, 0.0) - _safe_float(bid, 0.0))
            mid = ((ask + bid) / 2.0) if (_safe_float(ask, 0.0) > 0 and _safe_float(bid, 0.0) > 0) else price
            spread_pct = (spread / mid * 100.0) if mid > 0 else 0.0
            conf = _clamp(_safe_float(getattr(signal, "confidence", 0.0), 0.0) / 100.0, 0.0, 1.0)
            trend = str(getattr(signal, "trend", "") or "").lower()
            trend_aligned = (direction == "long" and "bull" in trend) or (direction == "short" and "bear" in trend)
            session = str(getattr(signal, "session", "") or "")
            stats = self._symbol_stats(
                account_key=str(account_key or ""),
                signal_symbol=sig_symbol,
                broker_symbol=broker_symbol,
                lookback_days=max(7, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_LOOKBACK_DAYS", 45))),
            )
            dir_stats = self._symbol_direction_stats(
                account_key=str(account_key or ""),
                signal_symbol=sig_symbol,
                broker_symbol=broker_symbol,
                direction=direction,
                lookback_days=max(7, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_LOOKBACK_DAYS", 30) or 30)),
            )
            samples = _safe_int(stats.get("samples", 0), 0)
            dir_samples = _safe_int(dir_stats.get("samples", 0), 0)
            src_key = str(source or "").strip().lower()
            raw_now = dict(getattr(signal, "raw_scores", {}) or {})
            np_now = _clamp(float(neural_prob), 0.0, 1.0) if neural_prob is not None else None

            # Family baselines and spread tolerances (heuristic, bounded).
            family_cfg = {
                "crypto": {"rr_min": 1.4, "rr_max": 2.8, "atr_ref": 1.8, "spread_warn": 0.10},
                "metal": {"rr_min": 1.4, "rr_max": 2.4, "atr_ref": 0.55, "spread_warn": 0.06},
                "fx":    {"rr_min": 1.2, "rr_max": 2.1, "atr_ref": 0.35, "spread_warn": 0.03},
                "index": {"rr_min": 1.3, "rr_max": 2.3, "atr_ref": 0.60, "spread_warn": 0.05},
                "stock": {"rr_min": 1.2, "rr_max": 2.0, "atr_ref": 1.20, "spread_warn": 0.08},
                "other": {"rr_min": 1.2, "rr_max": 2.2, "atr_ref": 1.00, "spread_warn": 0.08},
            }.get(family, {"rr_min": 1.2, "rr_max": 2.2, "atr_ref": 1.0, "spread_warn": 0.08})

            rr_floor = max(float(family_cfg["rr_min"]), float(getattr(config, "MT5_ADAPTIVE_EXECUTION_RR_MIN", 1.2)))
            rr_cap = min(float(family_cfg["rr_max"]), float(getattr(config, "MT5_ADAPTIVE_EXECUTION_RR_MAX", 2.8)))
            if rr_cap < rr_floor:
                rr_cap = rr_floor

            rr_adj = 0.0
            rr_adj += (conf - 0.70) * 0.60     # confidence quality
            rr_adj += (0.05 if trend_aligned else -0.03)
            rr_adj += self._session_score(session)
            rr_adj -= max(0.0, (spread_pct - float(family_cfg["spread_warn"])) * 1.5)

            # Volatility regime: if ATR% is high, widen stop and moderate RR to reduce premature stopouts.
            # --- Phase 6: Aggressive Volatility Exits (Fast Out) ---
            # If the market is moving fast (high ATR), we want to grab profit quickly.
            # We shrink the RR target aggressively to secure bags before a violent pullback.
            atr_ref = max(0.05, float(family_cfg["atr_ref"]))
            vol_ratio = (atr_pct / atr_ref) if atr_ref > 0 else 1.0
            
            if vol_ratio > 1.2:
                 # Widen Stop Loss moderately to give room to breathe
                 stop_scale = 1.0 + _clamp((vol_ratio - 1.0) * 0.15, 0.0, 0.25)
                 # Shrink RR Aggressively to take profit fast
                 rr_adj -= _clamp((vol_ratio - 1.2) * 0.35, 0.15, 0.45) 
                 logger.debug("[MT5AdaptiveExec] High Volatility (%.2fx). Aggressive Fast-Out padding active.", vol_ratio)
            else:
                 stop_scale = 1.0 + _clamp((vol_ratio - 1.0) * 0.12, -0.10, 0.18)
                 rr_adj -= _clamp((vol_ratio - 1.3) * 0.12, 0.0, 0.12)

            min_samples = max(1, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_MIN_SYMBOL_TRADES", 6)))
            hist_bonus = 0.0
            size_mult = 1.0
            if samples >= min_samples:
                win_rate = _safe_float(stats.get("win_rate", 0.5), 0.5)
                mae = _safe_float(stats.get("mae", 0.35), 0.35)
                tp_rate = _safe_float(stats.get("tp_rate", win_rate), win_rate)
                sl_rate = _safe_float(stats.get("sl_rate", 1.0 - win_rate), 1.0 - win_rate)
                hist_bonus += _clamp((win_rate - 0.52) * 0.55, -0.12, 0.12)
                hist_bonus += _clamp((tp_rate - sl_rate) * 0.12, -0.08, 0.08)
                hist_bonus -= _clamp((mae - 0.35) * 0.20, 0.0, 0.10)
                rr_adj += hist_bonus
                size_mult *= 1.0 + _clamp((win_rate - 0.52) * 0.75, -0.12, 0.12)
                size_mult *= 1.0 - _clamp((mae - 0.35) * 0.25, 0.0, 0.12)

            # Behavioral background layer:
            # Learn setup-context edge from real closed trades (candlestick micro-state + regime tags)
            # and overlay it on top of base statistics.
            behavior_stats = self._behavior_layer_stats(
                account_key=str(account_key or ""),
                signal=signal,
                broker_symbol=broker_symbol,
                source=src_key,
                direction=direction,
                family=family,
            )
            behavior_edge = _safe_float(behavior_stats.get("edge", 0.0), 0.0)
            behavior_rr_adj = 0.0
            behavior_size_adj = 0.0
            behavior_soft_cap_applied = False
            if bool(behavior_stats.get("applied")):
                behavior_rr_adj = _safe_float(behavior_stats.get("rr_adj", 0.0), 0.0)
                behavior_size_adj = _safe_float(behavior_stats.get("size_adj", 0.0), 0.0)
                rr_adj += behavior_rr_adj
                size_mult *= 1.0 + behavior_size_adj
                # Very negative context edge => cap size aggressively, but do not hard-block by this layer alone.
                neg_edge_cut = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_NEGATIVE_EDGE_SOFT_BLOCK", -0.42) or -0.42),
                    -1.0,
                    0.0,
                )
                if behavior_edge <= neg_edge_cut:
                    size_cap = _clamp(
                        float(getattr(config, "MT5_ADAPTIVE_EXECUTION_BEHAVIOR_LAYER_NEGATIVE_SOFT_SIZE_CAP", 0.62) or 0.62),
                        0.40,
                        0.95,
                    )
                    size_before = float(size_mult)
                    size_mult = min(size_mult, size_cap)
                    behavior_soft_cap_applied = bool(size_mult < size_before)

            # Direction-aware penalty:
            # If recent edge of this exact direction is weak (low WR + negative pnl), trim RR/size.
            dir_rr_penalty = 0.0
            dir_size_penalty = 0.0
            direction_blocked = False
            direction_softened = False
            direction_block_reason = ""
            direction_block_rescue = False
            direction_calm_rescue = False
            direction_in_block_window = True
            if bool(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTIONAL_BIAS_ENABLED", True)):
                dir_min_samples = max(2, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_MIN_SAMPLES", 4) or 4))
                dir_target_wr = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_TARGET_WIN_RATE", 0.45) or 0.45),
                    0.30,
                    0.80,
                )
                dir_wr = _safe_float(dir_stats.get("win_rate", dir_target_wr), dir_target_wr)
                dir_avg_pnl = _safe_float(dir_stats.get("avg_pnl", 0.0), 0.0)
                if dir_samples >= dir_min_samples:
                    if (dir_wr < dir_target_wr) and (dir_avg_pnl < 0):
                        shortfall = _clamp((dir_target_wr - dir_wr) / max(1e-6, dir_target_wr), 0.0, 1.0)
                        max_rr_pen = _clamp(
                            float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_MAX_RR_PENALTY", 0.18) or 0.18),
                            0.02,
                            0.50,
                        )
                        max_sz_pen = _clamp(
                            float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_MAX_SIZE_PENALTY", 0.35) or 0.35),
                            0.05,
                            0.80,
                        )
                        dir_rr_penalty = _clamp(shortfall * 0.20, 0.0, max_rr_pen)
                        dir_size_penalty = _clamp(shortfall * 0.40, 0.0, max_sz_pen)
                        rr_adj -= dir_rr_penalty
                        size_mult *= (1.0 - dir_size_penalty)
                        logger.debug(
                            "[MT5AdaptiveExec] directional penalty dir=%s samples=%s wr=%.3f target=%.3f avg_pnl=%.4f rr_pen=%.3f size_pen=%.3f",
                            direction,
                            dir_samples,
                            dir_wr,
                            dir_target_wr,
                            dir_avg_pnl,
                            dir_rr_penalty,
                            dir_size_penalty,
                        )
                # Direction quality hard guard (source scoped):
                # block repeated weak-side trades unless this setup is very high quality.
                block_sources_raw = str(
                    getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_SOURCES", "scalp_xauusd") or "scalp_xauusd"
                )
                block_sources = {s.strip().lower() for s in block_sources_raw.split(",") if s.strip()}
                block_scope_all = ("*" in block_sources) or ("all" in block_sources)
                block_enabled = bool(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_ENABLED", True))
                block_applies = bool(block_enabled and (block_scope_all or (src_key in block_sources)))
                block_min_samples = max(dir_min_samples, int(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MIN_SAMPLES", 4) or 4))
                block_max_wr = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MAX_WIN_RATE", 0.35) or 0.35),
                    0.05,
                    0.95,
                )
                block_max_avg_pnl = float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_MAX_AVG_PNL", -5.0) or -5.0)
                rescue_conf = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_RESCUE_MIN_CONFIDENCE", 84.0) or 84.0) / 100.0,
                    0.50,
                    0.99,
                )
                rescue_prob = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_RESCUE_MIN_NEURAL_PROB", 0.70) or 0.70),
                    0.0,
                    1.0,
                )
                direction_block_rescue = bool(
                    (conf >= rescue_conf)
                    and (np_now is not None)
                    and (float(np_now) >= rescue_prob)
                )

                # Softer block policy: enforce hard block only inside selected UTC windows.
                windows = self._parse_utc_hour_windows(
                    str(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_HOURS_UTC", "12-20") or "12-20")
                )
                if windows:
                    direction_in_block_window = self._hour_in_windows(_utc_now().hour, windows)
                soft_outside_window = bool(
                    getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_OUTSIDE_WINDOW_SOFT_ONLY", True)
                )

                # Calm-news rescue: when event shock is low, allow carefully if BG behavior edge supports it.
                calm_rescue_enabled = bool(
                    getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_NEWS_RESCUE_ENABLED", True)
                )
                calm_max_shock = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MAX_SHOCK_SCORE", 7.0) or 7.0),
                    0.0,
                    30.0,
                )
                calm_min_conf = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MIN_CONFIDENCE", 78.0) or 78.0) / 100.0,
                    0.50,
                    0.99,
                )
                calm_min_bg_edge = _clamp(
                    float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_MIN_BEHAVIOR_EDGE", 0.12) or 0.12),
                    -1.0,
                    1.0,
                )
                is_calm = self._is_xau_news_calm(raw_now)
                # optional stricter calm score gate if provided
                shock_score_now = _safe_float_opt(raw_now.get("xau_event_shock_score"))
                if shock_score_now is not None and float(shock_score_now) > calm_max_shock:
                    is_calm = False
                direction_calm_rescue = bool(
                    calm_rescue_enabled
                    and is_calm
                    and (conf >= calm_min_conf)
                    and (behavior_edge >= calm_min_bg_edge)
                )
                if (not direction_calm_rescue) and calm_rescue_enabled and is_calm and (not bool(behavior_stats.get("applied"))):
                    calm_bootstrap_conf = _clamp(
                        float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_BOOTSTRAP_MIN_CONFIDENCE", 80.0) or 80.0) / 100.0,
                        0.50,
                        0.99,
                    )
                    calm_bootstrap_prob = _clamp(
                        float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_BLOCK_CALM_BOOTSTRAP_MIN_NEURAL_PROB", 0.64) or 0.64),
                        0.0,
                        1.0,
                    )
                    direction_calm_rescue = bool(
                        (conf >= calm_bootstrap_conf)
                        and (np_now is not None)
                        and (float(np_now) >= calm_bootstrap_prob)
                    )

                if (
                    block_applies
                    and dir_samples >= block_min_samples
                    and dir_wr <= block_max_wr
                    and dir_avg_pnl <= block_max_avg_pnl
                    and (not direction_block_rescue)
                    and (not direction_calm_rescue)
                ):
                    if (not direction_in_block_window) and soft_outside_window:
                        direction_softened = True
                        # outside block window => keep hunting but with reduced risk.
                        soft_rr_pen = _clamp(
                            float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_SOFT_RR_PENALTY", 0.10) or 0.10),
                            0.02,
                            0.30,
                        )
                        soft_size_pen = _clamp(
                            float(getattr(config, "MT5_ADAPTIVE_EXECUTION_DIRECTION_SOFT_SIZE_PENALTY", 0.22) or 0.22),
                            0.05,
                            0.50,
                        )
                        rr_adj -= soft_rr_pen
                        size_mult *= (1.0 - soft_size_pen)
                        direction_block_reason = (
                            f"direction_guard_soft:{src_key or 'unknown'}:{direction}:outside_window"
                            f":wr={dir_wr:.2f} avg_pnl={dir_avg_pnl:.2f}"
                        )
                    else:
                        direction_blocked = True
                        direction_block_reason = (
                            f"direction_guard:{src_key or 'unknown'}:{direction}"
                            f":wr={dir_wr:.2f}<=max{block_max_wr:.2f}"
                            f":avg_pnl={dir_avg_pnl:.2f}<=max{block_max_avg_pnl:.2f}"
                        )
                        logger.info(
                            "[MT5AdaptiveExec] direction hard guard %s %s blocked source=%s wr=%.3f avg_pnl=%.4f samples=%s",
                            sig_symbol,
                            direction,
                            src_key or "-",
                            dir_wr,
                            dir_avg_pnl,
                            dir_samples,
                        )

            # ── Neural probability influence (bounded, never blocks a trade) ──────
            neural_bonus = 0.0
            if neural_prob is not None:
                np_val = _clamp(float(neural_prob), 0.0, 1.0)
                # High neural confidence → extend TP; low → shrink TP (bounded ±15%)
                neural_bonus = _clamp((np_val - 0.55) * 0.50, -0.10, 0.15)
                rr_adj += neural_bonus
                # Size adjustment from neural signal (bounded ±10%)
                size_mult *= 1.0 + _clamp((np_val - 0.55) * 0.35, -0.08, 0.10)
                # When model is uncertain (<40%), widen stop slightly to reduce premature stop-outs
                if np_val < 0.40:
                    stop_scale *= 1.05
                logger.debug(
                    "[MT5AdaptiveExec] neural_prob=%.3f neural_bonus=%.3f rr_adj=%.3f size_mult=%.3f stop_scale=%.3f",
                    np_val, neural_bonus, rr_adj, size_mult, stop_scale,
                )

            # Keep dollar risk roughly stable when widening stops; amplify only modestly when conditions improve.
            size_mult *= (1.0 / max(0.70, stop_scale)) ** 0.55
            size_mult *= 1.0 + _clamp((conf - 0.72) * 0.22, -0.05, 0.06)

            stop_scale = _clamp(
                stop_scale,
                float(getattr(config, "MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MIN", 0.85)),
                float(getattr(config, "MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MAX", 1.35)),
            )
            size_mult = _clamp(
                size_mult,
                float(getattr(config, "MT5_ADAPTIVE_EXECUTION_SIZE_MIN", 0.70)),
                float(getattr(config, "MT5_ADAPTIVE_EXECUTION_SIZE_MAX", 1.10)),
            )
            rr_target = _clamp(rr0 * (1.0 + rr_adj), rr_floor, rr_cap)
            rr_target = round(float(rr_target), 2)

            is_long = direction == "long"
            new_risk = max(float(point or 0.0) * 2.0, float(base_risk) * float(stop_scale))
            if is_long:
                sl = price - new_risk
                tp1 = price + new_risk * 1.0
                tp2 = price + new_risk * rr_target
                tp3 = price + new_risk * max(rr_target + 1.0, 3.0)
            else:
                sl = price + new_risk
                tp1 = price - new_risk * 1.0
                tp2 = price - new_risk * rr_target
                tp3 = price - new_risk * max(rr_target + 1.0, 3.0)

            # Materiality threshold: avoid churn for microscopic changes.
            changed = (
                abs(sl - sl0) > max(float(point or 0.0) * 2.0, abs(sl0) * 0.00002)
                or abs(_safe_float(getattr(signal, "take_profit_2", 0.0), 0.0) - tp2) > max(float(point or 0.0) * 2.0, abs(tp2) * 0.00002)
                or abs(rr_target - rr0) >= 0.05
                or abs(size_mult - 1.0) >= 0.03
            )

            factors = {
                "family": family,
                "atr_pct": round(float(atr_pct), 5),
                "spread_pct": round(float(spread_pct), 6),
                "confidence": round(float(conf), 4),
                "trend_aligned": bool(trend_aligned),
                "session": session,
                "vol_ratio": round(float(vol_ratio), 4),
                "samples": int(samples),
                "win_rate": stats.get("win_rate"),
                "tp_rate": stats.get("tp_rate"),
                "sl_rate": stats.get("sl_rate"),
                "mae": stats.get("mae"),
                "dir_samples": int(dir_samples),
                "dir_win_rate": dir_stats.get("win_rate"),
                "dir_avg_pnl": dir_stats.get("avg_pnl"),
                "rr_adj": round(float(rr_adj), 4),
                "hist_bonus": round(float(hist_bonus), 4),
                "dir_rr_penalty": round(float(dir_rr_penalty), 4),
                "dir_size_penalty": round(float(dir_size_penalty), 4),
                "direction_blocked": bool(direction_blocked),
                "direction_softened": bool(direction_softened),
                "direction_block_reason": str(direction_block_reason or ""),
                "direction_block_rescue": bool(direction_block_rescue),
                "direction_calm_rescue": bool(direction_calm_rescue),
                "direction_in_block_window": bool(direction_in_block_window),
                "source": str(src_key or ""),
                "behavior_layer_applied": bool(behavior_stats.get("applied")),
                "behavior_samples": int(behavior_stats.get("samples", 0) or 0),
                "behavior_matched_tokens": int(behavior_stats.get("matched_tokens", 0) or 0),
                "behavior_edge": round(float(behavior_edge), 4),
                "behavior_rr_adj": round(float(behavior_rr_adj), 4),
                "behavior_size_adj": round(float(behavior_size_adj), 4),
                "behavior_source_mode": str(behavior_stats.get("source_mode", "")),
                "behavior_tokens_used": list(behavior_stats.get("tokens_used", []) or [])[:8],
                "behavior_volume_profile_enabled": bool(behavior_stats.get("volume_profile_enabled", False)),
                "behavior_soft_cap_applied": bool(behavior_soft_cap_applied),
                "neural_prob": round(float(neural_prob), 4) if neural_prob is not None else None,
                "neural_bonus": round(float(neural_bonus), 4),
            }
            plan_reason = "direction_blocked" if direction_blocked else ("adaptive_applied" if changed else "adaptive_neutral")
            return AdaptiveExecutionPlan(
                ok=True,
                applied=bool(changed or direction_blocked),
                reason=plan_reason,
                signal_symbol=sig_symbol,
                broker_symbol=str(broker_symbol or ""),
                account_key=str(account_key or ""),
                rr_target=float(rr_target),
                rr_base=float(rr0),
                stop_scale=round(float(stop_scale), 4),
                size_multiplier=round(float(size_mult), 4),
                entry=float(price),
                stop_loss=float(sl),
                take_profit_1=float(tp1),
                take_profit_2=float(tp2),
                take_profit_3=float(tp3),
                factors=factors,
            )
        except Exception as e:
            logger.debug("[MT5AdaptiveExec] planner error: %s", e, exc_info=True)
            return AdaptiveExecutionPlan(False, False, f"planner_error:{e}")


mt5_adaptive_trade_planner = MT5AdaptiveTradePlanner()
