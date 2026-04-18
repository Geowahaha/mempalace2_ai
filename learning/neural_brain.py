"""
learning/neural_brain.py
Signal outcome memory + lightweight backprop trainer for Dexter Pro.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time as time_module
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, time
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np

from config import config
from execution.mt5_executor import mt5_executor
from learning.symbol_normalizer import canonical_symbol

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


@dataclass
class TrainResult:
    ok: bool
    status: str
    message: str
    samples: int = 0
    train_accuracy: float = 0.0
    val_accuracy: float = 0.0
    win_rate: float = 0.0


class NeuralBrain:
    FEATURE_NAMES = [
        "confidence",
        "risk_reward",
        "rsi",
        "atr_pct",
        "edge",
        "long_score",
        "short_score",
        "is_long",
        "pat_ob",
        "pat_fvg",
        "pat_choch",
        "pat_bos",
        "pat_bb",
        "pat_div",
        "src_xauusd",
        "src_crypto",
        "src_stocks",
        "src_us_open",
        "src_manual",
        # --- NEW features (added for enhanced adaptive TP/SL learning) ---
        "hour_sin",       # time-of-day sine encoding
        "hour_cos",       # time-of-day cosine encoding
        "sl_ratio",       # |entry - SL| / entry  (tight SL => more SL hits)
        "tp_sl_ratio",    # |TP2 - entry| / |entry - SL|  (actual proposed RR)
        "mae_hist",       # historical mean adverse excursion from autopilot DB (0 if unknown)
        "resolve_age_h",  # hours until signal resolved (0 at prediction time)
    ]

    def __init__(self):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = data_dir / "signal_learning.db"
        self.model_path = data_dir / "neural_brain.npz"
        self._lock = threading.Lock()
        self._model_cache: Optional[dict] = None
        self._reason_study_cache: Optional[dict] = None
        self._reason_study_cache_key: tuple[int, int] | None = None
        self._reason_study_cache_ts: float = 0.0
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path), timeout=15)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    signal_symbol TEXT NOT NULL,
                    broker_symbol TEXT,
                    direction TEXT,
                    confidence REAL,
                    risk_reward REAL,
                    rsi REAL,
                    atr REAL,
                    timeframe TEXT,
                    entry REAL,
                    stop_loss REAL,
                    take_profit_1 REAL,
                    take_profit_2 REAL,
                    take_profit_3 REAL,
                    pattern TEXT,
                    session TEXT,
                    score_long REAL,
                    score_short REAL,
                    score_edge REAL,
                    mt5_status TEXT NOT NULL,
                    mt5_message TEXT,
                    ticket INTEGER,
                    position_id INTEGER,
                    resolved INTEGER NOT NULL DEFAULT 0,
                    outcome INTEGER,
                    pnl REAL,
                    closed_at TEXT,
                    extra_json TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_events_resolved ON signal_events(resolved, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_events_pos ON signal_events(position_id, ticket)"
            )
            # Backward-compatible migrations for older DBs.
            cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(signal_events)").fetchall()}
            if "timeframe" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN timeframe TEXT")
            if "take_profit_1" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN take_profit_1 REAL")
            if "take_profit_3" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN take_profit_3 REAL")
            # New feature columns (added for enhanced neural training)
            if "feat_hour_sin" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN feat_hour_sin REAL")
            if "feat_hour_cos" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN feat_hour_cos REAL")
            if "feat_sl_ratio" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN feat_sl_ratio REAL")
            if "feat_tp_sl_ratio" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN feat_tp_sl_ratio REAL")
            if "feat_mae_hist" not in cols:
                conn.execute("ALTER TABLE signal_events ADD COLUMN feat_mae_hist REAL")
            conn.commit()

    def _safe_float(self, value, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def _pattern_flags(self, pattern: str) -> dict[str, float]:
        p = (pattern or "").upper()
        return {
            "pat_ob": 1.0 if "OB" in p or "ORDER BLOCK" in p else 0.0,
            "pat_fvg": 1.0 if "FVG" in p else 0.0,
            "pat_choch": 1.0 if "CHOCH" in p else 0.0,
            "pat_bos": 1.0 if "BOS" in p else 0.0,
            "pat_bb": 1.0 if "BB" in p or "BOLLINGER" in p else 0.0,
            "pat_div": 1.0 if "DIVERGENCE" in p or "DIV" in p else 0.0,
        }

    def _source_flags(self, source: str) -> dict[str, float]:
        s = (source or "").lower()
        return {
            "src_xauusd": 1.0 if "xau" in s or "gold" in s else 0.0,
            "src_crypto": 1.0 if "crypto" in s else 0.0,
            "src_stocks": 1.0 if "stock" in s else 0.0,
            "src_us_open": 1.0 if "us_open" in s else 0.0,
            "src_manual": 1.0 if "manual" in s else 0.0,
        }

    def _signal_feature_dict(self, signal, source: str, now_utc: Optional[datetime] = None) -> dict[str, float]:
        entry = max(1e-12, self._safe_float(getattr(signal, "entry", 0.0), 0.0))
        atr = self._safe_float(getattr(signal, "atr", 0.0), 0.0)
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
        sl = self._safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
        tp2 = self._safe_float(getattr(signal, "take_profit_2", 0.0), 0.0)
        # Time-of-day encoding (cyclic)
        now = now_utc or _utc_now()
        hour_frac = (now.hour + now.minute / 60.0) / 24.0
        import math
        fd = {
            "confidence": np.clip(self._safe_float(getattr(signal, "confidence", 0.0), 0.0) / 100.0, 0.0, 1.5),
            "risk_reward": np.clip(self._safe_float(getattr(signal, "risk_reward", 0.0), 0.0) / 5.0, 0.0, 2.0),
            "rsi": np.clip(self._safe_float(getattr(signal, "rsi", 50.0), 50.0) / 100.0, 0.0, 1.0),
            "atr_pct": np.clip(atr / entry, 0.0, 0.5),
            "edge": np.clip(self._safe_float(raw_scores.get("edge", 0.0), 0.0) / 100.0, 0.0, 2.0),
            "long_score": np.clip(self._safe_float(raw_scores.get("long", 0.0), 0.0) / 100.0, 0.0, 3.0),
            "short_score": np.clip(self._safe_float(raw_scores.get("short", 0.0), 0.0) / 100.0, 0.0, 3.0),
            "is_long": 1.0 if str(getattr(signal, "direction", "")).lower() == "long" else 0.0,
            # Time of day
            "hour_sin": float(math.sin(2 * math.pi * hour_frac)),
            "hour_cos": float(math.cos(2 * math.pi * hour_frac)),
            # SL quality features
            "sl_ratio": float(np.clip(abs(entry - sl) / entry, 0.0, 0.20)) if sl > 0 else 0.0,
            "tp_sl_ratio": float(np.clip(abs(tp2 - entry) / max(abs(entry - sl), 1e-12), 0.0, 5.0)) if (sl > 0 and tp2 > 0) else 0.0,
            # MAE from autopilot (filled in from DB for training rows; 0 for live prediction)
            "mae_hist": 0.0,
            # Hours to resolution (0 at prediction time)
            "resolve_age_h": 0.0,
        }
        fd.update(self._pattern_flags(str(getattr(signal, "pattern", ""))))
        fd.update(self._source_flags(source))
        return fd

    def _to_vector(self, feature_dict: dict[str, float]) -> np.ndarray:
        return np.array([float(feature_dict.get(k, 0.0)) for k in self.FEATURE_NAMES], dtype=np.float64)

    def record_execution(self, signal, result, source: str) -> None:
        if not config.NEURAL_BRAIN_ENABLED:
            return
        if signal is None or result is None:
            return

        raw_signal_symbol = str(getattr(signal, "symbol", "") or "")
        raw_broker_symbol = str(getattr(result, "broker_symbol", "") or "")
        signal_symbol = canonical_symbol(raw_signal_symbol) or raw_signal_symbol.strip().upper()
        broker_symbol = canonical_symbol(raw_broker_symbol) or raw_broker_symbol.strip().upper()
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
        extra = {
            "raw_scores": raw_scores,
            "reasons": list(getattr(signal, "reasons", []) or []),
            "warnings": list(getattr(signal, "warnings", []) or []),
            "raw_signal_symbol": raw_signal_symbol,
            "raw_broker_symbol": raw_broker_symbol,
            "canonical_signal_symbol": signal_symbol,
            "canonical_broker_symbol": broker_symbol,
        }
        now_iso = _iso(_utc_now())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO signal_events (
                        created_at, source, signal_symbol, broker_symbol, direction,
                        confidence, risk_reward, rsi, atr, timeframe, entry, stop_loss, take_profit_1, take_profit_2, take_profit_3,
                        pattern, session, score_long, score_short, score_edge,
                        mt5_status, mt5_message, ticket, position_id, resolved, outcome, pnl, closed_at, extra_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, ?)
                    """,
                    (
                        now_iso,
                        str(source or ""),
                        signal_symbol,
                        broker_symbol,
                        str(getattr(signal, "direction", "") or ""),
                        self._safe_float(getattr(signal, "confidence", 0.0), 0.0),
                        self._safe_float(getattr(signal, "risk_reward", 0.0), 0.0),
                        self._safe_float(getattr(signal, "rsi", 0.0), 0.0),
                        self._safe_float(getattr(signal, "atr", 0.0), 0.0),
                        str(getattr(signal, "timeframe", "") or "1h"),
                        self._safe_float(getattr(signal, "entry", 0.0), 0.0),
                        self._safe_float(getattr(signal, "stop_loss", 0.0), 0.0),
                        self._safe_float(getattr(signal, "take_profit_1", 0.0), 0.0),
                        self._safe_float(getattr(signal, "take_profit_2", 0.0), 0.0),
                        self._safe_float(getattr(signal, "take_profit_3", 0.0), 0.0),
                        str(getattr(signal, "pattern", "") or ""),
                        str(getattr(signal, "session", "") or ""),
                        self._safe_float(raw_scores.get("long", 0.0), 0.0),
                        self._safe_float(raw_scores.get("short", 0.0), 0.0),
                        self._safe_float(raw_scores.get("edge", 0.0), 0.0),
                        str(getattr(result, "status", "") or ""),
                        str(getattr(result, "message", "") or "")[:300],
                        int(getattr(result, "ticket", 0) or 0),
                        int(getattr(result, "position_id", 0) or 0),
                        json.dumps(extra, ensure_ascii=True),
                    ),
                )
                conn.commit()

    def _is_recent_duplicate(self, conn: sqlite3.Connection, signal, source: str, minutes: int = 5) -> bool:
        raw_symbol = str(getattr(signal, "symbol", "") or "")
        symbol = canonical_symbol(raw_symbol) or raw_symbol.strip().upper()
        direction = str(getattr(signal, "direction", "") or "")
        if not symbol:
            return False
        since_iso = _iso(_utc_now() - timedelta(minutes=max(1, int(minutes))))
        row = conn.execute(
            """
            SELECT id, entry
            FROM signal_events
            WHERE created_at >= ?
              AND source = ?
              AND signal_symbol = ?
              AND direction = ?
              AND mt5_status = 'telegram_sent'
            ORDER BY id DESC
            LIMIT 1
            """,
            (since_iso, str(source or ""), symbol, direction),
        ).fetchone()
        if row is None:
            return False
        prev_entry = self._safe_float(row[1], 0.0)
        cur_entry = self._safe_float(getattr(signal, "entry", 0.0), 0.0)
        if prev_entry <= 0 or cur_entry <= 0:
            return True
        return abs(prev_entry - cur_entry) <= max(1e-8, cur_entry * 0.0002)

    def record_signal_sent(self, signal, source: str) -> None:
        """
        Record a signal that was sent to Telegram (even if not executed on MT5).
        This powers post-hoc TP/SL outcome analysis and neural training.
        """
        if not config.NEURAL_BRAIN_ENABLED:
            return
        if signal is None:
            return
        raw_signal_symbol = str(getattr(signal, "symbol", "") or "")
        signal_symbol = canonical_symbol(raw_signal_symbol) or raw_signal_symbol.strip().upper()
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
        extra = {
            "kind": "telegram_signal",
            "raw_scores": raw_scores,
            "reasons": list(getattr(signal, "reasons", []) or []),
            "warnings": list(getattr(signal, "warnings", []) or []),
            "raw_signal_symbol": raw_signal_symbol,
            "canonical_signal_symbol": signal_symbol,
        }
        now_iso = _iso(_utc_now())
        with self._lock:
            with self._connect() as conn:
                if self._is_recent_duplicate(conn, signal, source=source, minutes=5):
                    return
                conn.execute(
                    """
                    INSERT INTO signal_events (
                        created_at, source, signal_symbol, broker_symbol, direction,
                        confidence, risk_reward, rsi, atr, timeframe, entry, stop_loss, take_profit_1, take_profit_2, take_profit_3,
                        pattern, session, score_long, score_short, score_edge,
                        mt5_status, mt5_message, ticket, position_id, resolved, outcome, pnl, closed_at, extra_json
                    )
                    VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'telegram_sent', 'sent_to_telegram', 0, 0, 0, NULL, NULL, NULL, ?)
                    """,
                    (
                        now_iso,
                        str(source or ""),
                        signal_symbol,
                        str(getattr(signal, "direction", "") or ""),
                        self._safe_float(getattr(signal, "confidence", 0.0), 0.0),
                        self._safe_float(getattr(signal, "risk_reward", 0.0), 0.0),
                        self._safe_float(getattr(signal, "rsi", 0.0), 0.0),
                        self._safe_float(getattr(signal, "atr", 0.0), 0.0),
                        str(getattr(signal, "timeframe", "") or "1h"),
                        self._safe_float(getattr(signal, "entry", 0.0), 0.0),
                        self._safe_float(getattr(signal, "stop_loss", 0.0), 0.0),
                        self._safe_float(getattr(signal, "take_profit_1", 0.0), 0.0),
                        self._safe_float(getattr(signal, "take_profit_2", 0.0), 0.0),
                        self._safe_float(getattr(signal, "take_profit_3", 0.0), 0.0),
                        str(getattr(signal, "pattern", "") or ""),
                        str(getattr(signal, "session", "") or ""),
                        self._safe_float(raw_scores.get("long", 0.0), 0.0),
                        self._safe_float(raw_scores.get("short", 0.0), 0.0),
                        self._safe_float(raw_scores.get("edge", 0.0), 0.0),
                        json.dumps(extra, ensure_ascii=True),
                    ),
                )
                conn.commit()

    @staticmethod
    def _safe_json_load(raw: str) -> dict:
        if not raw:
            return {}
        try:
            val = json.loads(raw)
            return val if isinstance(val, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _normalize_reason_tag(value: str) -> str:
        token = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
        token = re.sub(r"_+", "_", token).strip("_")
        return token[:80]

    @classmethod
    def _append_reason_tag(cls, out: list[str], seen: set[str], prefix: str, value: str) -> None:
        token = cls._normalize_reason_tag(value)
        if not token:
            return
        tag = f"{prefix}:{token}"
        if tag in seen:
            return
        seen.add(tag)
        out.append(tag)

    def _signal_reason_tags(self, signal, source: str) -> list[str]:
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
        tags: list[str] = []
        seen: set[str] = set()
        self._append_reason_tag(tags, seen, "source", source)
        self._append_reason_tag(tags, seen, "symbol", canonical_symbol(str(getattr(signal, "symbol", "") or "")) or str(getattr(signal, "symbol", "") or ""))
        self._append_reason_tag(tags, seen, "pattern", str(getattr(signal, "pattern", "") or ""))
        self._append_reason_tag(tags, seen, "session", str(getattr(signal, "session", "") or ""))
        self._append_reason_tag(tags, seen, "timeframe", str(getattr(signal, "timeframe", "") or ""))
        entry_type = (
            getattr(signal, "entry_type", "")
            or raw_scores.get("entry_type")
            or raw_scores.get("scalp_m1_entry_order_type")
            or raw_scores.get("scalp_m1_entry_type")
        )
        self._append_reason_tag(tags, seen, "entry", str(entry_type or ""))
        family = (
            raw_scores.get("strategy_family")
            or raw_scores.get("family")
            or raw_scores.get("scalp_family")
            or raw_scores.get("scalping_source")
        )
        self._append_reason_tag(tags, seen, "family", str(family or ""))
        for item in list(getattr(signal, "reasons", []) or []):
            self._append_reason_tag(tags, seen, "reason", str(item or ""))
        for item in list(getattr(signal, "warnings", []) or []):
            self._append_reason_tag(tags, seen, "warning", str(item or ""))
        for item in list(raw_scores.get("gate_reasons") or []):
            self._append_reason_tag(tags, seen, "gate", str(item or ""))
        return tags

    def _row_reason_tags(self, row: sqlite3.Row) -> list[str]:
        row_obj = dict(row)
        extra = self._safe_json_load(str(row_obj.get("extra_json", "") or ""))
        raw_scores = dict(extra.get("raw_scores", {}) or {})
        tags: list[str] = []
        seen: set[str] = set()
        self._append_reason_tag(tags, seen, "source", str(row_obj.get("source", "") or ""))
        self._append_reason_tag(
            tags,
            seen,
            "symbol",
            canonical_symbol(str(row_obj.get("signal_symbol", "") or row_obj.get("broker_symbol", "") or ""))
            or str(row_obj.get("signal_symbol", "") or row_obj.get("broker_symbol", "") or ""),
        )
        self._append_reason_tag(tags, seen, "pattern", str(row_obj.get("pattern", "") or ""))
        self._append_reason_tag(tags, seen, "session", str(row_obj.get("session", "") or ""))
        self._append_reason_tag(tags, seen, "timeframe", str(row_obj.get("timeframe", "") or ""))
        entry_type = (
            raw_scores.get("entry_type")
            or extra.get("entry_type")
            or raw_scores.get("scalp_m1_entry_order_type")
            or raw_scores.get("scalp_m1_entry_type")
        )
        self._append_reason_tag(tags, seen, "entry", str(entry_type or ""))
        family = (
            raw_scores.get("strategy_family")
            or raw_scores.get("family")
            or raw_scores.get("scalp_family")
            or raw_scores.get("scalping_source")
        )
        self._append_reason_tag(tags, seen, "family", str(family or ""))
        for item in list(extra.get("reasons", []) or []):
            self._append_reason_tag(tags, seen, "reason", str(item or ""))
        for item in list(extra.get("warnings", []) or []):
            self._append_reason_tag(tags, seen, "warning", str(item or ""))
        for item in list(raw_scores.get("gate_reasons") or []):
            self._append_reason_tag(tags, seen, "gate", str(item or ""))
        return tags

    def _extract_exit_state(self, row: sqlite3.Row) -> str:
        row_obj = dict(row)
        extra = self._safe_json_load(str(row_obj.get("extra_json", "") or ""))
        market_eval = dict(extra.get("market_eval", {}) or {})
        close_resolution = dict(extra.get("close_resolution", {}) or {})
        state = str(market_eval.get("state") or close_resolution.get("state") or "").strip().lower()
        if state:
            return state
        msg = str(row_obj.get("mt5_message", "") or "").strip().lower()
        if msg.startswith("mt5_close:"):
            return self._normalize_reason_tag(msg.split(":", 1)[1])
        pnl = self._safe_float(row_obj.get("pnl"), 0.0)
        if "ctrader_reconciled_close" in msg:
            return "win" if pnl > 0 else ("loss" if pnl < 0 else "flat")
        return "win" if pnl > 0 else ("loss" if pnl < 0 else "flat")

    @staticmethod
    def _reason_bucket_score(resolved: int, win_rate: float, avg_r: float) -> float:
        if resolved <= 0:
            return 0.0
        sample_factor = min(1.0, float(np.log1p(resolved) / np.log1p(24.0)))
        win_edge = (float(win_rate) - 0.5) * 2.0
        pnl_edge = float(np.clip(avg_r, -1.5, 1.5) / 1.5)
        return float((0.65 * win_edge + 0.35 * pnl_edge) * sample_factor)

    def _market_fetch(self, symbol: str, timeframe: str, bars: int = 1200):
        tf = (timeframe or "1h").strip().lower()
        tf = tf if tf in {"1m", "5m", "15m", "30m", "1h", "4h", "1d"} else "1h"
        raw_sym = str(symbol or "").strip().upper()
        sym = canonical_symbol(raw_sym) or raw_sym
        try:
            if sym == "XAUUSD":
                from market.data_fetcher import xauusd_provider
                return xauusd_provider.fetch(tf if tf in {"1m", "5m", "15m", "30m", "1h", "4h", "1d"} else "1h", bars=bars)
            if "/" in sym:
                from market.data_fetcher import crypto_provider
                return crypto_provider.fetch_ohlcv(sym, tf if tf in {"1m", "5m", "15m", "30m", "1h", "4h", "1d"} else "1h", bars=bars)
            try:
                if sym in {str(x).upper() for x in config.get_fx_major_symbols()}:
                    from market.data_fetcher import fx_provider
                    return fx_provider.fetch_ohlcv(sym, tf if tf in {"1m", "5m", "15m", "30m", "1h", "4h", "1d"} else "1h", bars=bars)
            except Exception:
                pass
            from scanners.stock_scanner import fetch_stock_ohlcv
            tf_stock = tf if tf in {"1h", "4h", "1d", "1wk"} else "1h"
            return fetch_stock_ohlcv(raw_sym or sym, tf_stock, bars=bars)
        except Exception:
            return None

    def _evaluate_signal_path(
        self,
        direction: str,
        entry: float,
        stop_loss: float,
        tp1: float,
        tp2: float,
        tp3: float,
        df,
    ) -> dict:
        if df is None or len(df) == 0:
            return {"resolved": False, "state": "no_data"}

        risk = abs(float(entry) - float(stop_loss))
        if risk <= 1e-12:
            return {"resolved": False, "state": "invalid_levels"}

        dir_long = str(direction or "").lower() == "long"
        resolved = None
        resolved_r = None
        resolved_ts = None
        resolved_price = None

        for ts, row in df.iterrows():
            high = self._safe_float(row.get("high"), 0.0)
            low = self._safe_float(row.get("low"), 0.0)
            if dir_long:
                hit_sl = low <= stop_loss
                hit_tp1 = high >= tp1
                hit_tp2 = high >= tp2
                hit_tp3 = high >= tp3
                if hit_sl and (hit_tp1 or hit_tp2 or hit_tp3):
                    resolved, resolved_r, resolved_price = "sl_ambiguous", -1.0, stop_loss
                elif hit_sl:
                    resolved, resolved_r, resolved_price = "sl", -1.0, stop_loss
                elif hit_tp3:
                    resolved, resolved_r, resolved_price = "tp3", 3.0, tp3
                elif hit_tp2:
                    resolved, resolved_r, resolved_price = "tp2", 2.0, tp2
                elif hit_tp1:
                    resolved, resolved_r, resolved_price = "tp1", 1.0, tp1
            else:
                hit_sl = high >= stop_loss
                hit_tp1 = low <= tp1
                hit_tp2 = low <= tp2
                hit_tp3 = low <= tp3
                if hit_sl and (hit_tp1 or hit_tp2 or hit_tp3):
                    resolved, resolved_r, resolved_price = "sl_ambiguous", -1.0, stop_loss
                elif hit_sl:
                    resolved, resolved_r, resolved_price = "sl", -1.0, stop_loss
                elif hit_tp3:
                    resolved, resolved_r, resolved_price = "tp3", 3.0, tp3
                elif hit_tp2:
                    resolved, resolved_r, resolved_price = "tp2", 2.0, tp2
                elif hit_tp1:
                    resolved, resolved_r, resolved_price = "tp1", 1.0, tp1

            if resolved is not None:
                resolved_ts = ts
                break

        last_close = self._safe_float(df["close"].iloc[-1], entry)
        current_r = ((last_close - entry) / risk) if dir_long else ((entry - last_close) / risk)

        if resolved is None:
            return {
                "resolved": False,
                "state": "pending",
                "current_price": float(last_close),
                "current_r": float(current_r),
            }
        return {
            "resolved": True,
            "state": resolved,
            "resolved_r": float(resolved_r),
            "resolved_price": float(resolved_price),
            "resolved_ts": resolved_ts,
            "current_price": float(last_close),
            "current_r": float(current_r),
        }

    def sync_signal_outcomes_from_market(self, days: int = 30, max_records: int = 200) -> dict:
        """
        Evaluate Telegram-sent signals against market path to determine TP/SL outcomes.
        """
        if not config.NEURAL_BRAIN_ENABLED:
            return {"ok": False, "status": "disabled", "message": "neural brain disabled", "updated": 0}
        if not config.SIGNAL_FEEDBACK_ENABLED:
            return {"ok": False, "status": "disabled", "message": "signal feedback disabled", "updated": 0}

        since_iso = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        max_n = max(10, int(max_records))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, created_at, source, signal_symbol, direction, timeframe,
                           entry, stop_loss, take_profit_1, take_profit_2, take_profit_3, extra_json
                    FROM signal_events
                    WHERE resolved = 0
                      AND mt5_status = 'telegram_sent'
                      AND created_at >= ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (since_iso, max_n),
                ).fetchall()

                cache = {}
                updated = 0
                reviewed = 0
                resolved_count = 0
                pseudo_labeled = 0
                pseudo_enabled = bool(config.NEURAL_BRAIN_PSEUDO_LABEL_ENABLED)
                pseudo_min_hours = max(0.5, float(config.NEURAL_BRAIN_PSEUDO_LABEL_MIN_HOURS))
                pseudo_min_abs_r = max(0.05, float(config.NEURAL_BRAIN_PSEUDO_LABEL_MIN_ABS_R))
                now_utc = _utc_now()
                for row in rows:
                    reviewed += 1
                    row_id = int(row[0])
                    created_at = _parse_iso(str(row[1]) or "")
                    source = str(row[2] or "")
                    symbol = str(row[3] or "")
                    direction = str(row[4] or "long")
                    timeframe = str(row[5] or "1h")
                    entry = self._safe_float(row[6], 0.0)
                    stop_loss = self._safe_float(row[7], 0.0)
                    tp1 = self._safe_float(row[8], 0.0)
                    tp2 = self._safe_float(row[9], 0.0)
                    tp3 = self._safe_float(row[10], 0.0)
                    extra = self._safe_json_load(str(row[11] or ""))

                    if created_at is None or entry <= 0 or stop_loss <= 0 or tp2 <= 0:
                        continue
                    if tp1 <= 0:
                        tp1 = entry + (entry - stop_loss) if direction.lower() == "long" else entry - (stop_loss - entry)
                    if tp3 <= 0:
                        tp3 = entry + 3 * abs(entry - stop_loss) if direction.lower() == "long" else entry - 3 * abs(stop_loss - entry)

                    # Use a finer path timeframe for outcome evaluation so recent TP/SL hits
                    # can be detected without waiting for the next higher-timeframe bar close.
                    path_tf = timeframe if timeframe in {"1m", "5m", "15m", "30m"} else "5m"
                    key = (symbol, path_tf)
                    if key not in cache:
                        cache[key] = self._market_fetch(symbol, path_tf, bars=3000)
                    df = cache.get(key)
                    if df is None or len(df) == 0:
                        continue
                    try:
                        idx = df.index
                        if getattr(idx, "tz", None) is None:
                            df.index = idx.tz_localize(timezone.utc)
                    except Exception:
                        pass
                    df_after = df[df.index >= created_at]
                    if df_after is None or len(df_after) == 0:
                        continue

                    eval_result = self._evaluate_signal_path(
                        direction=direction,
                        entry=entry,
                        stop_loss=stop_loss,
                        tp1=tp1,
                        tp2=tp2,
                        tp3=tp3,
                        df=df_after,
                    )
                    extra["market_eval"] = {
                        "state": eval_result.get("state"),
                        "current_price": eval_result.get("current_price"),
                        "current_r": eval_result.get("current_r"),
                        "path_tf": path_tf,
                        "source": source,
                    }

                    if eval_result.get("resolved"):
                        state = str(eval_result.get("state"))
                        resolved_r = self._safe_float(eval_result.get("resolved_r"), 0.0)
                        resolved_ts = eval_result.get("resolved_ts")
                        resolved_at = _iso(resolved_ts if isinstance(resolved_ts, datetime) else _utc_now())
                        extra["market_eval"]["resolved_price"] = eval_result.get("resolved_price")
                        extra["market_eval"]["resolved_r"] = resolved_r
                        extra["market_eval"]["resolved_at"] = resolved_at
                        outcome = 1 if resolved_r > 0 else 0
                        conn.execute(
                            """
                            UPDATE signal_events
                            SET resolved = 1,
                                outcome = ?,
                                pnl = ?,
                                closed_at = ?,
                                mt5_message = ?,
                                extra_json = ?
                            WHERE id = ?
                            """,
                            (
                                outcome,
                                resolved_r,
                                resolved_at,
                                f"market_eval:{state}",
                                json.dumps(extra, ensure_ascii=True),
                                row_id,
                            ),
                        )
                        updated += 1
                        resolved_count += 1
                    else:
                        current_r = self._safe_float(eval_result.get("current_r"), 0.0)
                        age_hours = max(0.0, (now_utc - created_at).total_seconds() / 3600.0)
                        if (
                            pseudo_enabled
                            and age_hours >= pseudo_min_hours
                            and abs(current_r) >= pseudo_min_abs_r
                        ):
                            outcome = 1 if current_r > 0 else 0
                            resolved_at = _iso(now_utc)
                            state = "pseudo_win" if outcome == 1 else "pseudo_loss"
                            extra["market_eval"]["state"] = state
                            extra["market_eval"]["pseudo_label"] = True
                            extra["market_eval"]["pseudo_age_h"] = round(age_hours, 3)
                            extra["market_eval"]["pseudo_abs_r_min"] = pseudo_min_abs_r
                            conn.execute(
                                """
                                UPDATE signal_events
                                SET resolved = 1,
                                    outcome = ?,
                                    pnl = ?,
                                    closed_at = ?,
                                    mt5_message = ?,
                                    extra_json = ?
                                WHERE id = ?
                                """,
                                (
                                    outcome,
                                    current_r,
                                    resolved_at,
                                    "market_eval:pseudo_label",
                                    json.dumps(extra, ensure_ascii=True),
                                    row_id,
                                ),
                            )
                            updated += 1
                            resolved_count += 1
                            pseudo_labeled += 1
                        else:
                            conn.execute(
                                "UPDATE signal_events SET extra_json = ? WHERE id = ?",
                                (json.dumps(extra, ensure_ascii=True), row_id),
                            )
                            updated += 1
                conn.commit()

        return {
            "ok": True,
            "status": "ok",
            "message": "",
            "reviewed": reviewed,
            "updated": updated,
            "resolved": resolved_count,
            "pseudo_labeled": pseudo_labeled,
        }

    def signal_feedback_report(self, days: int = 30, source_contains: str = "") -> dict:
        days_i = max(1, int(days))
        since_iso = _iso(_utc_now() - timedelta(days=days_i))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT signal_symbol, source, resolved, outcome, pnl, mt5_message, extra_json
                    FROM signal_events
                    WHERE mt5_status = 'telegram_sent'
                      AND created_at >= ?
                    ORDER BY id DESC
                    """,
                    (since_iso,),
                ).fetchall()

        src_filter = str(source_contains or "").strip().lower()
        if src_filter:
            rows = [r for r in rows if src_filter in str(r[1] or "").lower()]

        sent = len(rows)
        if sent == 0:
            return {
                "ok": True,
                "status": "no_data",
                "days": days_i,
                "source_filter": src_filter,
                "sent": 0,
                "resolved": 0,
                "pending": 0,
                "tp1": 0,
                "tp2": 0,
                "tp3": 0,
                "sl": 0,
                "win_rate": 0.0,
                "avg_r_resolved": 0.0,
                "avg_r_pending": 0.0,
                "top_symbols": [],
            }

        resolved = 0
        pending = 0
        tp1 = tp2 = tp3 = sl = 0
        r_resolved: list[float] = []
        r_pending: list[float] = []
        by_symbol: dict[str, dict] = {}

        for symbol, source, is_resolved, outcome, pnl, mt5_message, extra_json in rows:
            sym = str(symbol or "UNKNOWN")
            rec = by_symbol.setdefault(sym, {"symbol": sym, "sent": 0, "resolved": 0, "wins": 0, "net_r": 0.0})
            rec["sent"] += 1
            extra = self._safe_json_load(str(extra_json or ""))
            eval_state = str((extra.get("market_eval") or {}).get("state", "")).lower()

            if int(is_resolved or 0) == 1:
                resolved += 1
                rec["resolved"] += 1
                rr = self._safe_float(pnl, 0.0)
                r_resolved.append(rr)
                rec["net_r"] += rr
                if rr > 0:
                    rec["wins"] += 1
                if "tp3" in eval_state:
                    tp3 += 1
                elif "tp2" in eval_state:
                    tp2 += 1
                elif "tp1" in eval_state:
                    tp1 += 1
                elif "sl" in eval_state:
                    sl += 1
            else:
                pending += 1
                current_r = self._safe_float((extra.get("market_eval") or {}).get("current_r"), 0.0)
                r_pending.append(current_r)

        # Win rate counts all positive resolved outcomes (including pseudo/market-eval wins);
        # TP1/TP2/TP3 and SL remain explicit state counters for transparency.
        wins = sum(1 for rr in r_resolved if float(rr or 0.0) > 0)
        win_rate = (100.0 * wins / resolved) if resolved > 0 else 0.0
        for rec in by_symbol.values():
            rec["win_rate"] = round((100.0 * rec["wins"] / rec["resolved"]) if rec["resolved"] else 0.0, 1)
            rec["net_r"] = round(rec["net_r"], 3)
        top_symbols = sorted(
            by_symbol.values(),
            key=lambda x: (x["net_r"], x["win_rate"], x["resolved"], x["sent"]),
            reverse=True,
        )[:10]

        return {
            "ok": True,
            "status": "ok",
            "days": days_i,
            "source_filter": src_filter,
            "sent": sent,
            "resolved": resolved,
            "pending": pending,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl": sl,
            "wins": wins,
            "win_rate": round(win_rate, 1),
            "avg_r_resolved": round(float(np.mean(r_resolved)) if r_resolved else 0.0, 4),
            "avg_r_pending": round(float(np.mean(r_pending)) if r_pending else 0.0, 4),
            "top_symbols": top_symbols,
        }

    def build_reason_study_report(
        self,
        *,
        days: Optional[int] = None,
        min_resolved: Optional[int] = None,
    ) -> dict:
        if not config.NEURAL_BRAIN_ENABLED:
            return {"ok": False, "status": "disabled", "message": "neural brain disabled"}
        if not bool(getattr(config, "NEURAL_BRAIN_REASON_STUDY_ENABLED", True)):
            return {"ok": False, "status": "disabled", "message": "reason study disabled"}

        days_i = max(1, int(days if days is not None else getattr(config, "NEURAL_BRAIN_REASON_STUDY_LOOKBACK_DAYS", 120)))
        min_n = max(1, int(min_resolved if min_resolved is not None else getattr(config, "NEURAL_BRAIN_REASON_STUDY_MIN_RESOLVED", 8)))
        cache_key = (days_i, min_n)
        now_ts = time_module.time()
        cache_ttl = max(5, int(getattr(config, "NEURAL_BRAIN_REASON_STUDY_CACHE_SEC", 120) or 120))
        if (
            self._reason_study_cache is not None
            and self._reason_study_cache_key == cache_key
            and (now_ts - float(self._reason_study_cache_ts or 0.0)) <= cache_ttl
        ):
            return dict(self._reason_study_cache)

        since_iso = _iso(_utc_now() - timedelta(days=days_i))
        with self._lock:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT created_at, source, signal_symbol, broker_symbol, pattern, session, timeframe,
                           mt5_message, outcome, pnl, extra_json
                    FROM signal_events
                    WHERE resolved = 1
                      AND outcome IN (0, 1)
                      AND created_at >= ?
                    ORDER BY id DESC
                    """,
                    (since_iso,),
                ).fetchall()

        tag_stats: dict[str, dict] = {}
        exit_state_counts: dict[str, int] = {}
        for row in rows:
            exit_state = self._extract_exit_state(row)
            exit_state_counts[exit_state] = int(exit_state_counts.get(exit_state, 0) or 0) + 1
            pnl = self._safe_float(row["pnl"], 0.0)
            outcome = int(row["outcome"] or 0)
            for tag in self._row_reason_tags(row):
                rec = tag_stats.setdefault(
                    tag,
                    {
                        "tag": tag,
                        "resolved": 0,
                        "wins": 0,
                        "losses": 0,
                        "net_r": 0.0,
                        "tp_like": 0,
                        "sl_like": 0,
                        "other_exit": 0,
                    },
                )
                rec["resolved"] += 1
                rec["wins"] += 1 if outcome == 1 else 0
                rec["losses"] += 1 if outcome == 0 else 0
                rec["net_r"] += pnl
                if exit_state.startswith("tp") or exit_state == "win":
                    rec["tp_like"] += 1
                elif exit_state in {"sl", "loss"}:
                    rec["sl_like"] += 1
                else:
                    rec["other_exit"] += 1

        tag_rows: list[dict] = []
        tag_index: dict[str, dict] = {}
        for tag, rec in tag_stats.items():
            resolved = int(rec["resolved"] or 0)
            wins = int(rec["wins"] or 0)
            win_rate = (wins / resolved) if resolved else 0.0
            avg_r = (float(rec["net_r"] or 0.0) / resolved) if resolved else 0.0
            row = {
                "tag": tag,
                "resolved": resolved,
                "wins": wins,
                "losses": int(rec["losses"] or 0),
                "win_rate": round(win_rate, 4),
                "avg_r": round(avg_r, 4),
                "net_r": round(float(rec["net_r"] or 0.0), 4),
                "tp_like": int(rec["tp_like"] or 0),
                "sl_like": int(rec["sl_like"] or 0),
                "other_exit": int(rec["other_exit"] or 0),
                "eligible": bool(resolved >= min_n),
                "score": round(self._reason_bucket_score(resolved, win_rate, avg_r), 4),
            }
            tag_rows.append(row)
            tag_index[tag] = row

        tag_rows.sort(key=lambda item: (abs(float(item.get("score", 0.0) or 0.0)), int(item.get("resolved", 0) or 0)), reverse=True)
        eligible_rows = [dict(row) for row in tag_rows if bool(row.get("eligible"))]
        report = {
            "ok": True,
            "status": "ok",
            "days": days_i,
            "min_resolved": min_n,
            "resolved_rows": len(rows),
            "eligible_tags": len(eligible_rows),
            "exit_state_counts": dict(sorted(exit_state_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
            "top_positive_tags": [
                row
                for row in sorted(
                    eligible_rows,
                    key=lambda item: (float(item.get("score", 0.0) or 0.0), int(item.get("resolved", 0) or 0)),
                    reverse=True,
                )
                if float(row.get("score", 0.0) or 0.0) > 0
            ][:12],
            "top_negative_tags": [
                row
                for row in sorted(
                    eligible_rows,
                    key=lambda item: (float(item.get("score", 0.0) or 0.0), -int(item.get("resolved", 0) or 0)),
                )
                if float(row.get("score", 0.0) or 0.0) < 0
            ][:12],
            "tag_rows": tag_rows[:80],
            "tag_index": tag_index,
        }
        self._reason_study_cache = dict(report)
        self._reason_study_cache_key = cache_key
        self._reason_study_cache_ts = now_ts
        return report

    def reason_confidence_adjustment(self, signal, source: str) -> dict:
        if not config.NEURAL_BRAIN_ENABLED:
            return {"applied": False, "reason": "neural_disabled"}
        if not bool(getattr(config, "NEURAL_BRAIN_REASON_STUDY_ENABLED", True)):
            return {"applied": False, "reason": "reason_study_disabled"}

        report = self.build_reason_study_report()
        if not bool(report.get("ok")):
            return {"applied": False, "reason": str(report.get("status") or "reason_study_unavailable")}

        tag_index = dict(report.get("tag_index") or {})
        matched = []
        for tag in self._signal_reason_tags(signal, source):
            row = dict(tag_index.get(tag) or {})
            if row and bool(row.get("eligible")):
                matched.append(row)
        if not matched:
            return {"applied": False, "reason": "no_reason_history"}

        weighted_sum = 0.0
        weight_sum = 0.0
        for row in matched:
            resolved = max(1.0, float(row.get("resolved", 0) or 0))
            weight = float(np.sqrt(resolved))
            weighted_sum += float(row.get("score", 0.0) or 0.0) * weight
            weight_sum += weight
        if weight_sum <= 0:
            return {"applied": False, "reason": "no_reason_weight"}

        avg_score = weighted_sum / weight_sum
        mult = max(0.0, float(getattr(config, "NEURAL_BRAIN_REASON_STUDY_WEIGHT", 0.20) or 0.20))
        max_delta = abs(float(getattr(config, "NEURAL_BRAIN_REASON_STUDY_MAX_DELTA", 4.0) or 4.0))
        delta = float(np.clip(avg_score * 10.0 * mult, -max_delta, max_delta))
        if abs(delta) < 0.05:
            return {
                "applied": False,
                "reason": "reason_delta_too_small",
                "matched_tags": [row.get("tag") for row in matched[:5]],
            }
        matched_sorted = sorted(
            matched,
            key=lambda item: (abs(float(item.get("score", 0.0) or 0.0)), int(item.get("resolved", 0) or 0)),
            reverse=True,
        )
        return {
            "applied": True,
            "reason": "applied",
            "delta": round(delta, 3),
            "avg_score": round(float(avg_score), 4),
            "matched_count": len(matched),
            "matched_tags": [dict(row) for row in matched_sorted[:5]],
            "study_days": int(report.get("days", 0) or 0),
            "study_resolved_rows": int(report.get("resolved_rows", 0) or 0),
        }

    def us_open_trader_dashboard(self, risk_pct: float = 1.0, start_balance: float = 1000.0) -> dict:
        """
        Build a US-open trader dashboard for today's NY session using signal_events.

        Scope:
        - US stocks only
        - rows sent to Telegram
        - NY cash-open review window (09:30 -> configurable review end, default 120m)
        """
        if not config.NEURAL_BRAIN_ENABLED:
            return {"ok": False, "status": "disabled", "message": "neural brain disabled"}

        try:
            ny_tz = ZoneInfo("America/New_York")
            bkk_tz = ZoneInfo("Asia/Bangkok")
        except Exception:
            ny_tz = timezone.utc
            bkk_tz = timezone.utc

        now_utc = _utc_now()
        ny_now = now_utc.astimezone(ny_tz)
        ny_day = ny_now.date()
        open_start_ny = datetime(ny_day.year, ny_day.month, ny_day.day, 9, 30, tzinfo=ny_tz)
        hard_stop_min = max(30, int(getattr(config, "US_OPEN_SMART_POST_OPEN_MAX_MIN", 90) or 90))
        review_max_min = max(hard_stop_min, 120)
        core_end_ny = open_start_ny + timedelta(minutes=hard_stop_min)
        review_end_ny = open_start_ny + timedelta(minutes=review_max_min)

        query_since = _iso(open_start_ny.astimezone(timezone.utc) - timedelta(hours=1))
        query_until = _iso(review_end_ny.astimezone(timezone.utc) + timedelta(hours=1))

        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT created_at, signal_symbol, source, pattern, confidence,
                           score_edge, score_long, score_short,
                           resolved, outcome, pnl, extra_json
                    FROM signal_events
                    WHERE mt5_status = 'telegram_sent'
                      AND created_at >= ? AND created_at <= ?
                    ORDER BY created_at ASC
                    """,
                    (query_since, query_until),
                ).fetchall()

        day_start_ny = datetime.combine(ny_day, time.min, tzinfo=ny_tz)
        day_end_ny = day_start_ny + timedelta(days=1)
        day_query_since = _iso(day_start_ny.astimezone(timezone.utc))
        day_query_until = _iso(day_end_ny.astimezone(timezone.utc))

        with self._lock:
            with self._connect() as conn:
                day_rows = conn.execute(
                    """
                    SELECT created_at, signal_symbol, source, pattern, confidence,
                           score_edge, score_long, score_short,
                           resolved, outcome, pnl, extra_json
                    FROM signal_events
                    WHERE mt5_status = 'telegram_sent'
                      AND created_at >= ? AND created_at < ?
                    ORDER BY created_at ASC
                    """,
                    (day_query_since, day_query_until),
                ).fetchall()

        def _is_us_stock_symbol(sym: str) -> bool:
            s = str(sym or "").upper()
            if not s:
                return False
            if "/" in s or s.startswith("^"):
                return False
            # non-US suffixes used in this project
            for suf in (".L", ".DE", ".PA", ".T", ".HK", ".BK", ".SI", ".NS", ".BO", ".AX"):
                if s.endswith(suf):
                    return False
            return True

        bucket_labels = {
            "gold": "Gold",
            "thai_stocks": "Thailand Stocks",
            "us_stocks": "US Stocks",
            "global_stocks": "Global Stocks",
            "crypto": "Crypto",
            "other": "Other",
        }

        def _normalize_market_filter(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
            raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
            if not raw or raw in {"all", "any"}:
                return None, None
            aliases = {
                "gold": "gold",
                "xau": "gold",
                "xauusd": "gold",
                "thai": "thai_stocks",
                "th": "thai_stocks",
                "thai_stocks": "thai_stocks",
                "thai_stock": "thai_stocks",
                "set": "thai_stocks",
                "set50": "thai_stocks",
                "us": "us_stocks",
                "usa": "us_stocks",
                "america": "us_stocks",
                "american": "us_stocks",
                "us_stocks": "us_stocks",
                "us_stock": "us_stocks",
                "global": "global_stocks",
                "world": "global_stocks",
                "global_stocks": "global_stocks",
                "crypto": "crypto",
                "coin": "crypto",
                "coins": "crypto",
                "other": "other",
            }
            key = aliases.get(raw)
            if not key:
                return None, None
            return key, bucket_labels.get(key, key)

        def _base_setup(pattern: str) -> str:
            p = str(pattern or "").upper().strip()
            for pref in ("BULLISH_", "BEARISH_"):
                if p.startswith(pref):
                    return p[len(pref):]
            return p or "UNKNOWN"

        def _conf_band(conf: float) -> str:
            c = float(conf or 0.0)
            if c >= 80:
                return "80+"
            if c >= 75:
                return "75-79"
            if c >= 70:
                return "70-74"
            return "<70"

        events: list[dict] = []
        for row in rows:
            created_at, symbol, source, pattern, confidence, score_edge, score_long, score_short, resolved, outcome, pnl, extra_json = row
            src = str(source or "")
            if not any(k in src for k in ("stocks", "us_open")):
                continue
            sym = str(symbol or "")
            if not _is_us_stock_symbol(sym):
                continue
            dt_utc = _parse_iso(str(created_at or ""))
            if dt_utc is None:
                continue
            dt_ny = dt_utc.astimezone(ny_tz)
            if not (open_start_ny <= dt_ny <= review_end_ny):
                continue
            extra = self._safe_json_load(str(extra_json or ""))
            market_eval = dict(extra.get("market_eval") or {})
            raw_scores = dict(extra.get("raw_scores") or {})
            current_r = market_eval.get("current_r")
            try:
                current_r = float(current_r) if current_r is not None else None
            except Exception:
                current_r = None

            quality_score_exact = raw_scores.get("quality_score")
            try:
                quality_score_exact = int(quality_score_exact) if quality_score_exact is not None else None
            except Exception:
                quality_score_exact = None
            quality_tag = str(raw_scores.get("quality_tag") or "").upper() or None
            vol_ratio = raw_scores.get("vol_ratio")
            try:
                vol_ratio = float(vol_ratio) if vol_ratio is not None else None
            except Exception:
                vol_ratio = None

            minute_from_open = max(0.0, (dt_ny - open_start_ny).total_seconds() / 60.0)
            events.append({
                "created_at_utc": dt_utc,
                "created_at_ny": dt_ny,
                "created_at_bkk": dt_utc.astimezone(bkk_tz),
                "symbol": sym,
                "source": src,
                "pattern": str(pattern or ""),
                "setup": _base_setup(pattern),
                "confidence": self._safe_float(confidence, 0.0),
                "score_edge": self._safe_float(score_edge, 0.0),
                "score_long": self._safe_float(score_long, 0.0),
                "score_short": self._safe_float(score_short, 0.0),
                "resolved": int(resolved or 0),
                "outcome": None if outcome is None else int(outcome),
                "pnl_r": None if pnl is None else self._safe_float(pnl, 0.0),
                "eval_state": str(market_eval.get("state") or "").lower(),
                "current_r": current_r,
                "minute_from_open": minute_from_open,
                "segment": "core" if minute_from_open <= hard_stop_min else "late",
                "quality_score_exact": quality_score_exact,
                "quality_tag": quality_tag,
                "vol_ratio": vol_ratio,
                "conf_band": _conf_band(self._safe_float(confidence, 0.0)),
            })

        if not events:
            return {
                "ok": True,
                "status": "no_data",
                "ny_date": str(ny_day),
                "window": {
                    "open_start_ny": open_start_ny.strftime("%Y-%m-%d %H:%M %Z"),
                    "core_end_ny": core_end_ny.strftime("%Y-%m-%d %H:%M %Z"),
                    "review_end_ny": review_end_ny.strftime("%Y-%m-%d %H:%M %Z"),
                    "hard_stop_min": hard_stop_min,
                    "review_max_min": review_max_min,
                },
                "message": "No US stock signal_events found in today's US-open window.",
            }

        def _summarize_bucket(bucket_rows: list[dict]) -> dict:
            sent = len(bucket_rows)
            resolved_rows = [r for r in bucket_rows if r["resolved"] == 1 and r["pnl_r"] is not None]
            pending_rows = [r for r in bucket_rows if r["resolved"] != 1]
            wins = sum(1 for r in resolved_rows if float(r["pnl_r"] or 0.0) > 0)
            losses = sum(1 for r in resolved_rows if float(r["pnl_r"] or 0.0) < 0)
            flat = sum(1 for r in resolved_rows if abs(float(r["pnl_r"] or 0.0)) < 1e-12)
            net_r = float(sum(float(r["pnl_r"] or 0.0) for r in resolved_rows))
            pending_mark_r = float(sum(float(r["current_r"] or 0.0) for r in pending_rows if r["current_r"] is not None))
            avg_conf = float(np.mean([float(r["confidence"] or 0.0) for r in bucket_rows])) if bucket_rows else 0.0
            med_edge = float(np.median([float(r["score_edge"] or 0.0) for r in bucket_rows])) if bucket_rows else 0.0
            win_rate = (100.0 * wins / len(resolved_rows)) if resolved_rows else 0.0
            return {
                "sent": sent,
                "resolved": len(resolved_rows),
                "pending": len(pending_rows),
                "wins": wins,
                "losses": losses,
                "flat": flat,
                "win_rate": round(win_rate, 1),
                "net_r": round(net_r, 4),
                "pending_mark_r": round(pending_mark_r, 4),
                "avg_conf": round(avg_conf, 2),
                "median_edge": round(med_edge, 2),
            }

        overall = _summarize_bucket(events)
        core_rows = [r for r in events if r["segment"] == "core"]
        late_rows = [r for r in events if r["segment"] == "late"]
        core = _summarize_bucket(core_rows)
        late = _summarize_bucket(late_rows)

        # Symbol aggregation (use realized netR + marked pendingR for current session view)
        by_symbol: dict[str, dict] = {}
        for r in events:
            rec = by_symbol.setdefault(r["symbol"], {
                "symbol": r["symbol"], "sent": 0, "resolved": 0, "wins": 0, "losses": 0,
                "net_r": 0.0, "pending_mark_r": 0.0, "source_set": set(),
            })
            rec["sent"] += 1
            rec["source_set"].add(r["source"])
            if r["resolved"] == 1 and r["pnl_r"] is not None:
                rec["resolved"] += 1
                rr = float(r["pnl_r"] or 0.0)
                rec["net_r"] += rr
                if rr > 0:
                    rec["wins"] += 1
                elif rr < 0:
                    rec["losses"] += 1
            elif r["current_r"] is not None:
                rec["pending_mark_r"] += float(r["current_r"] or 0.0)
        for rec in by_symbol.values():
            rec["win_rate"] = round((100.0 * rec["wins"] / rec["resolved"]) if rec["resolved"] else 0.0, 1)
            rec["net_r"] = round(rec["net_r"], 4)
            rec["pending_mark_r"] = round(rec["pending_mark_r"], 4)
            rec["session_r"] = round(rec["net_r"] + rec["pending_mark_r"], 4)
            rec["sources"] = sorted(rec.pop("source_set"))
        symbols_sorted = sorted(by_symbol.values(), key=lambda x: (x["session_r"], x["net_r"], x["win_rate"], x["sent"]), reverse=True)
        best_symbols = symbols_sorted[:5]
        worst_symbols = sorted(by_symbol.values(), key=lambda x: (x["session_r"], x["net_r"], x["sent"]))[:5]
        most_active_symbols = sorted(
            by_symbol.values(),
            key=lambda x: (x["sent"], x["resolved"], x["session_r"], x["win_rate"]),
            reverse=True,
        )[:10]

        # Setup breakdown (resolved-focused win rate, include sent count context)
        def _build_setup_rows(rows_for_setup: list[dict]) -> list[dict]:
            by_setup_local: dict[str, dict] = {}
            for rrw in rows_for_setup:
                setup = str(rrw["setup"] or "UNKNOWN")
                rec = by_setup_local.setdefault(setup, {"setup": setup, "sent": 0, "resolved": 0, "wins": 0, "losses": 0, "net_r": 0.0})
                rec["sent"] += 1
                if rrw["resolved"] == 1 and rrw["pnl_r"] is not None:
                    rec["resolved"] += 1
                    rr = float(rrw["pnl_r"] or 0.0)
                    rec["net_r"] += rr
                    if rr > 0:
                        rec["wins"] += 1
                    elif rr < 0:
                        rec["losses"] += 1
            rows_out = []
            for rec in by_setup_local.values():
                rec["win_rate"] = round((100.0 * rec["wins"] / rec["resolved"]) if rec["resolved"] else 0.0, 1)
                rec["net_r"] = round(rec["net_r"], 4)
                rows_out.append(rec)
            rows_out.sort(key=lambda x: (x["resolved"], x["win_rate"], x["net_r"], x["sent"]), reverse=True)
            return rows_out

        setup_rows = _build_setup_rows(events)
        setup_rows_core = _build_setup_rows(core_rows)
        setup_rows_late = _build_setup_rows(late_rows)

        # Quality distribution (exact if available, else proxy/source bands)
        exact_q_counter: dict[str, int] = {"Q0": 0, "Q1": 0, "Q2": 0, "Q3": 0}
        exact_q_count = 0
        quality_tag_counter: dict[str, int] = {}
        source_tier_counter: dict[str, int] = {}
        conf_band_counter: dict[str, int] = {"<70": 0, "70-74": 0, "75-79": 0, "80+": 0}
        for r in events:
            conf_band_counter[r["conf_band"]] = int(conf_band_counter.get(r["conf_band"], 0) or 0) + 1
            source_tier_counter[r["source"]] = int(source_tier_counter.get(r["source"], 0) or 0) + 1
            if r["quality_tag"]:
                quality_tag_counter[r["quality_tag"]] = int(quality_tag_counter.get(r["quality_tag"], 0) or 0) + 1
            if r["quality_score_exact"] is not None and 0 <= int(r["quality_score_exact"]) <= 3:
                exact_q_counter[f"Q{int(r['quality_score_exact'])}"] += 1
                exact_q_count += 1
        quality_distribution = {
            "mode": "exact" if exact_q_count == len(events) else ("mixed" if exact_q_count > 0 else "proxy_only"),
            "exact_q_count": exact_q_count,
            "rows_total": len(events),
            "q_scores": exact_q_counter,
            "quality_tags": dict(sorted(quality_tag_counter.items())),
            "source_tiers": dict(sorted(source_tier_counter.items(), key=lambda kv: (-kv[1], kv[0]))),
            "confidence_bands": conf_band_counter,
        }

        # Late-open degradation heuristic (compare core vs late)
        if late["sent"] == 0:
            late_verdict = "no_late_signals"
            degraded = None
        else:
            core_mark_per_signal = ((core["net_r"] + core["pending_mark_r"]) / core["sent"]) if core["sent"] else 0.0
            late_mark_per_signal = ((late["net_r"] + late["pending_mark_r"]) / late["sent"]) if late["sent"] else 0.0
            late_conf_drop = float(core["avg_conf"]) - float(late["avg_conf"])
            degraded = (
                (late_mark_per_signal + 0.05) < core_mark_per_signal
                or (late["resolved"] > 0 and core["resolved"] > 0 and float(late["win_rate"]) + 10.0 < float(core["win_rate"]))
                or late_conf_drop >= 2.0
            )
            if (not degraded) and int(late.get("resolved", 0) or 0) < 2:
                late_verdict = "inconclusive"
            else:
                late_verdict = "degraded" if degraded else "not_degraded"

        # Simulation (fixed-R proxy)
        start_bal = max(1.0, float(start_balance))
        risk_pct_f = max(0.1, float(risk_pct))
        risk_amt = start_bal * (risk_pct_f / 100.0)
        sim = {
            "start_balance": round(start_bal, 2),
            "risk_pct": round(risk_pct_f, 4),
            "risk_amount_per_trade": round(risk_amt, 2),
            "realized_balance": round(start_bal + (overall["net_r"] * risk_amt), 2),
            "marked_balance": round(start_bal + ((overall["net_r"] + overall["pending_mark_r"]) * risk_amt), 2),
            "realized_pnl": round(overall["net_r"] * risk_amt, 2),
            "marked_pnl": round((overall["net_r"] + overall["pending_mark_r"]) * risk_amt, 2),
        }

        all_session_symbols = [
            r["symbol"]
            for r in sorted(by_symbol.values(), key=lambda x: (x["sent"], x["session_r"], x["resolved"]), reverse=True)
        ]

        # US-stock signals in the same NY day but outside the US-open review window.
        # Visibility only (not counted in session totals) to avoid hiding names like AAPL that fired later.
        outside_rows = []
        for row in day_rows:
            created_at, symbol, source, pattern, confidence, score_edge, score_long, score_short, resolved, outcome, pnl, extra_json = row
            src = str(source or "")
            if not any(k in src for k in ("stocks", "us_open")):
                continue
            sym = str(symbol or "")
            if not _is_us_stock_symbol(sym):
                continue
            dt_utc = _parse_iso(str(created_at or ""))
            if dt_utc is None:
                continue
            dt_ny = dt_utc.astimezone(ny_tz)
            if open_start_ny <= dt_ny <= review_end_ny:
                continue
            extra = self._safe_json_load(str(extra_json or ""))
            market_eval = dict(extra.get("market_eval") or {})
            current_r = market_eval.get("current_r")
            try:
                current_r = float(current_r) if current_r is not None else None
            except Exception:
                current_r = None
            outside_rows.append({
                "symbol": sym,
                "source": src,
                "resolved": int(resolved or 0),
                "pnl_r": None if pnl is None else self._safe_float(pnl, 0.0),
                "current_r": current_r,
                "confidence": self._safe_float(confidence, 0.0),
                "score_edge": self._safe_float(score_edge, 0.0),
            })

        outside_by_symbol: dict[str, dict] = {}
        for r in outside_rows:
            rec = outside_by_symbol.setdefault(
                r["symbol"],
                {"symbol": r["symbol"], "sent": 0, "resolved": 0, "wins": 0, "losses": 0, "net_r": 0.0, "pending_mark_r": 0.0},
            )
            rec["sent"] += 1
            if r["resolved"] == 1 and r["pnl_r"] is not None:
                rec["resolved"] += 1
                rr = float(r["pnl_r"] or 0.0)
                rec["net_r"] += rr
                if rr > 0:
                    rec["wins"] += 1
                elif rr < 0:
                    rec["losses"] += 1
            elif r["current_r"] is not None:
                rec["pending_mark_r"] += float(r["current_r"] or 0.0)
        for rec in outside_by_symbol.values():
            rec["net_r"] = round(rec["net_r"], 4)
            rec["pending_mark_r"] = round(rec["pending_mark_r"], 4)
            rec["session_r"] = round(rec["net_r"] + rec["pending_mark_r"], 4)
            rec["win_rate"] = round((100.0 * rec["wins"] / rec["resolved"]) if rec["resolved"] else 0.0, 1)
        outside_top_active = sorted(
            outside_by_symbol.values(), key=lambda x: (x["sent"], x["resolved"], x["session_r"]), reverse=True
        )[:10]
        if outside_rows:
            outside_summary = _summarize_bucket(outside_rows)
        else:
            outside_summary = {
                "sent": 0, "resolved": 0, "pending": 0, "wins": 0, "losses": 0, "flat": 0,
                "win_rate": 0.0, "net_r": 0.0, "pending_mark_r": 0.0, "avg_conf": 0.0, "median_edge": 0.0,
            }

        return {
            "ok": True,
            "status": "ok",
            "ny_date": str(ny_day),
            "window": {
                "open_start_ny": open_start_ny.strftime("%Y-%m-%d %H:%M %Z"),
                "core_end_ny": core_end_ny.strftime("%Y-%m-%d %H:%M %Z"),
                "review_end_ny": review_end_ny.strftime("%Y-%m-%d %H:%M %Z"),
                "open_start_bkk": open_start_ny.astimezone(bkk_tz).strftime("%Y-%m-%d %H:%M"),
                "core_end_bkk": core_end_ny.astimezone(bkk_tz).strftime("%Y-%m-%d %H:%M"),
                "review_end_bkk": review_end_ny.astimezone(bkk_tz).strftime("%Y-%m-%d %H:%M"),
                "hard_stop_min": hard_stop_min,
                "review_max_min": review_max_min,
            },
            "summary": overall,
            "segments": {
                "core": core,
                "late": late,
                "degraded": degraded,
                "verdict": late_verdict,
            },
            "simulation": sim,
            "best_symbols": best_symbols,
            "worst_symbols": worst_symbols,
            "most_active_symbols": most_active_symbols,
            "symbol_stats_all": sorted(
                by_symbol.values(),
                key=lambda x: (x["sent"], x["session_r"], x["resolved"], x["win_rate"]),
                reverse=True,
            ),
            "all_session_symbols": all_session_symbols,
            "outside_window_summary": outside_summary,
            "outside_window_top_active": outside_top_active,
            "win_rate_by_setup": setup_rows[:10],
            "setup_stats_all": setup_rows,
            "setup_stats_by_segment": {
                "core": setup_rows_core,
                "late": setup_rows_late,
            },
            "quality_distribution": quality_distribution,
            "events_count": len(events),
            "sources_seen": sorted({str(r["source"]) for r in events}),
        }

    def daily_signal_trader_dashboard(
        self,
        days: int = 1,
        risk_pct: float = 1.0,
        start_balance: float = 1000.0,
        timezone_name: str = "Asia/Bangkok",
        market_filter: Optional[str] = None,
        symbol_filter: Optional[str] = None,
        window_mode: Optional[str] = None,
    ) -> dict:
        """Build daily all-signals trader dashboard using signal_events (telegram_sent rows)."""
        if not config.NEURAL_BRAIN_ENABLED:
            return {"ok": False, "status": "disabled", "message": "neural brain disabled"}

        days_i = max(1, min(30, int(days or 1)))
        try:
            user_tz = ZoneInfo(str(timezone_name or "Asia/Bangkok"))
        except Exception:
            user_tz = timezone.utc

        now_utc = _utc_now()
        now_local = now_utc.astimezone(user_tz)
        day_start_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=user_tz)
        next_day_local = day_start_local + timedelta(days=1)

        mode_raw = str(window_mode or "").strip().lower().replace("-", "_").replace(" ", "_")
        if mode_raw in {"today", "d1", "1d"}:
            mode = "today"
        elif mode_raw in {"yesterday", "yday"}:
            mode = "yesterday"
        elif mode_raw in {"this_week", "week", "wtd"}:
            mode = "this_week"
        elif mode_raw in {"this_month", "month", "mtd"}:
            mode = "this_month"
        elif mode_raw in {"rolling_days", "rolling", "days"}:
            mode = "rolling_days"
        else:
            mode = "rolling_days" if days_i > 1 else "today"

        if mode == "today":
            start_local = day_start_local
            end_local = next_day_local
        elif mode == "yesterday":
            start_local = day_start_local - timedelta(days=1)
            end_local = day_start_local
        elif mode == "this_week":
            start_local = day_start_local - timedelta(days=day_start_local.weekday())
            end_local = next_day_local
        elif mode == "this_month":
            start_local = day_start_local.replace(day=1)
            end_local = next_day_local
        else:
            end_local = next_day_local
            start_local = end_local - timedelta(days=days_i)

        start_utc = start_local.astimezone(timezone.utc)
        end_utc = end_local.astimezone(timezone.utc)

        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT created_at, signal_symbol, source, pattern, confidence,
                           score_edge, score_long, score_short,
                           resolved, outcome, pnl, extra_json
                    FROM signal_events
                    WHERE mt5_status = 'telegram_sent'
                      AND created_at >= ? AND created_at < ?
                    ORDER BY created_at ASC
                    """,
                    (_iso(start_utc), _iso(end_utc)),
                ).fetchall()

        non_us_suffixes = (".L", ".DE", ".PA", ".T", ".HK", ".BK", ".SI", ".NS", ".BO", ".AX")

        def _looks_us_stock(sym: str) -> bool:
            s = str(sym or "").upper().strip()
            if not s or "/" in s or s.startswith("^"):
                return False
            if s.startswith("XAU") or s.startswith("XAG"):
                return False
            if any(s.endswith(suf) for suf in non_us_suffixes):
                return False
            # filter some obvious non-stock FX/metals/index aliases if source is ambiguous
            if s in {"BTCUSD", "ETHUSD", "US500", "US30", "USTEC", "XTIUSD", "XBRUSD", "USDJPY", "EURUSD", "GBPUSD", "NZDUSD"}:
                return False
            return True

        def _classify_bucket(sym: str, src: str) -> str:
            s = str(sym or "").upper().strip()
            source = str(src or "").lower()
            if s.startswith("XAU") or source.startswith("xauusd"):
                return "gold"
            if "crypto" in source or "/" in s or s in {"BTCUSD", "ETHUSD", "PAXGUSD", "BCHUSD", "XRPUSD", "SOLUSD", "DOGEUSD", "ADAUSD", "AVAXUSD"}:
                return "crypto"
            if s.endswith(".BK") or "thailand" in source or "thai_vi" in source:
                return "thai_stocks"
            if "stocks" in source or "us_open" in source:
                if _looks_us_stock(s):
                    return "us_stocks"
                if any(s.endswith(suf) for suf in non_us_suffixes):
                    return "thai_stocks" if s.endswith(".BK") else "global_stocks"
            if _looks_us_stock(s):
                return "us_stocks"
            if any(s.endswith(suf) for suf in non_us_suffixes):
                return "thai_stocks" if s.endswith(".BK") else "global_stocks"
            return "other"

        bucket_labels = {
            "gold": "Gold",
            "thai_stocks": "Thailand Stocks",
            "us_stocks": "US Stocks",
            "global_stocks": "Global Stocks",
            "crypto": "Crypto",
            "other": "Other",
        }

        def _normalize_market_filter(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
            raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
            if not raw or raw in {"all", "any"}:
                return None, None
            aliases = {
                "gold": "gold", "xau": "gold", "xauusd": "gold",
                "thai": "thai_stocks", "th": "thai_stocks", "thai_stocks": "thai_stocks", "thai_stock": "thai_stocks", "set": "thai_stocks", "set50": "thai_stocks",
                "us": "us_stocks", "usa": "us_stocks", "america": "us_stocks", "american": "us_stocks", "us_stocks": "us_stocks", "us_stock": "us_stocks",
                "global": "global_stocks", "world": "global_stocks", "global_stocks": "global_stocks",
                "crypto": "crypto", "coin": "crypto", "coins": "crypto",
                "other": "other",
            }
            key = aliases.get(raw)
            return (key, bucket_labels.get(key, key)) if key else (None, None)

        def _dashboard_symbol(raw: str) -> str:
            token = str(raw or "").strip().upper().replace(" ", "")
            if not token:
                return ""
            alias = {
                "GOLD": "XAUUSD",
                "XAU": "XAUUSD",
                "XAUUSD": "XAUUSD",
                "ETH": "ETHUSD",
                "ETHUSD": "ETHUSD",
                "ETHUSDT": "ETHUSD",
                "ETH/USDT": "ETHUSD",
                "BTC": "BTCUSD",
                "BTCUSD": "BTCUSD",
                "BTCUSDT": "BTCUSD",
                "BTC/USDT": "BTCUSD",
            }
            if token in alias:
                return alias[token]
            compact = token.replace("/", "")
            if compact in alias:
                return alias[compact]
            if token.endswith("/USDT") and len(token) > 5:
                return f"{token[:-5]}USD"
            if token.endswith("USDT") and len(token) > 4:
                return f"{token[:-4]}USD"
            return token

        def _symbol_key(raw: str) -> str:
            return "".join(ch for ch in str(raw or "").upper() if ch.isalnum())

        def _base_setup(pattern: str) -> str:
            p = str(pattern or "").upper().strip()
            for pref in ("BULLISH_", "BEARISH_"):
                if p.startswith(pref):
                    return p[len(pref):]
            return p or "UNKNOWN"

        def _conf_band(conf: float) -> str:
            c = float(conf or 0.0)
            if c >= 80:
                return "80+"
            if c >= 75:
                return "75-79"
            if c >= 70:
                return "70-74"
            return "<70"

        events: list[dict] = []
        for row in rows:
            created_at, symbol, source, pattern, confidence, score_edge, score_long, score_short, resolved, outcome, pnl, extra_json = row
            dt_utc = _parse_iso(str(created_at or ""))
            if dt_utc is None:
                continue
            dt_local = dt_utc.astimezone(user_tz)
            extra = self._safe_json_load(str(extra_json or ""))
            market_eval = dict(extra.get("market_eval") or {})
            raw_scores = dict(extra.get("raw_scores") or {})
            try:
                current_r = float(market_eval.get("current_r")) if market_eval.get("current_r") is not None else None
            except Exception:
                current_r = None
            q_exact = raw_scores.get("quality_score")
            try:
                q_exact = int(q_exact) if q_exact is not None else None
            except Exception:
                q_exact = None
            vol_ratio = raw_scores.get("vol_ratio")
            try:
                vol_ratio = float(vol_ratio) if vol_ratio is not None else None
            except Exception:
                vol_ratio = None
            bucket = _classify_bucket(str(symbol or ""), str(source or ""))
            events.append({
                "created_at_utc": dt_utc,
                "created_at_local": dt_local,
                "symbol": str(symbol or ""),
                "source": str(source or ""),
                "bucket": bucket,
                "setup": _base_setup(pattern),
                "pattern": str(pattern or ""),
                "confidence": self._safe_float(confidence, 0.0),
                "score_edge": self._safe_float(score_edge, 0.0),
                "score_long": self._safe_float(score_long, 0.0),
                "score_short": self._safe_float(score_short, 0.0),
                "resolved": int(resolved or 0),
                "outcome": None if outcome is None else int(outcome),
                "pnl_r": None if pnl is None else self._safe_float(pnl, 0.0),
                "eval_state": str(market_eval.get("state") or "").lower(),
                "current_r": current_r,
                "quality_score_exact": q_exact,
                "quality_tag": str(raw_scores.get("quality_tag") or "").upper() or None,
                "vol_ratio": vol_ratio,
                "conf_band": _conf_band(self._safe_float(confidence, 0.0)),
            })

        selected_bucket, selected_bucket_label = _normalize_market_filter(market_filter)
        if selected_bucket:
            events = [r for r in events if str(r.get("bucket")) == selected_bucket]
        selected_symbol = _dashboard_symbol(str(symbol_filter or ""))
        if selected_symbol:
            selected_key = _symbol_key(selected_symbol)
            events = [
                r for r in events
                if _symbol_key(_dashboard_symbol(str(r.get("symbol") or ""))) == selected_key
            ]

        if not events:
            filters = []
            if selected_bucket_label:
                filters.append(f"market={selected_bucket_label}")
            if selected_symbol:
                filters.append(f"symbol={selected_symbol}")
            suffix = f" ({', '.join(filters)})" if filters else ""
            return {
                "ok": True,
                "status": "no_data",
                "days": days_i,
                "timezone": str(getattr(user_tz, 'key', 'UTC')),
                "window_mode": mode,
                "window": {
                    "start_local": start_local.strftime("%Y-%m-%d %H:%M"),
                    "end_local": end_local.strftime("%Y-%m-%d %H:%M"),
                },
                "message": f"No signal_events found in selected day window{suffix}.",
                "market_filter": selected_bucket,
                "market_filter_label": selected_bucket_label,
                "symbol_filter": selected_symbol or None,
                "symbol_filter_label": selected_symbol or None,
            }

        def _summarize_bucket(bucket_rows: list[dict]) -> dict:
            sent = len(bucket_rows)
            resolved_rows = [r for r in bucket_rows if r["resolved"] == 1 and r["pnl_r"] is not None]
            pending_rows = [r for r in bucket_rows if r["resolved"] != 1]
            wins = sum(1 for r in resolved_rows if float(r["pnl_r"] or 0.0) > 0)
            losses = sum(1 for r in resolved_rows if float(r["pnl_r"] or 0.0) < 0)
            flat = sum(1 for r in resolved_rows if abs(float(r["pnl_r"] or 0.0)) < 1e-12)
            net_r = float(sum(float(r["pnl_r"] or 0.0) for r in resolved_rows))
            pending_mark_r = float(sum(float(r["current_r"] or 0.0) for r in pending_rows if r["current_r"] is not None))
            avg_conf = float(np.mean([float(r["confidence"] or 0.0) for r in bucket_rows])) if bucket_rows else 0.0
            win_rate = (100.0 * wins / len(resolved_rows)) if resolved_rows else 0.0
            return {
                "sent": sent,
                "resolved": len(resolved_rows),
                "pending": len(pending_rows),
                "wins": wins,
                "losses": losses,
                "flat": flat,
                "win_rate": round(win_rate, 1),
                "net_r": round(net_r, 4),
                "pending_mark_r": round(pending_mark_r, 4),
                "avg_conf": round(avg_conf, 2),
            }

        overall = _summarize_bucket(events)
        bucket_order = ["gold", "thai_stocks", "us_stocks", "global_stocks", "crypto", "other"]
        buckets = {}
        for b in bucket_order:
            rows_b = [r for r in events if r["bucket"] == b]
            buckets[b] = _summarize_bucket(rows_b)
            buckets[b]["label"] = bucket_labels.get(b, b)

        start_bal = max(1.0, float(start_balance))
        risk_pct_f = max(0.1, float(risk_pct))
        risk_amt = start_bal * (risk_pct_f / 100.0)
        sim_overall = {
            "start_balance": round(start_bal, 2),
            "risk_pct": round(risk_pct_f, 4),
            "risk_amount_per_trade": round(risk_amt, 2),
            "realized_balance": round(start_bal + (overall["net_r"] * risk_amt), 2),
            "marked_balance": round(start_bal + ((overall["net_r"] + overall["pending_mark_r"]) * risk_amt), 2),
            "realized_pnl": round(overall["net_r"] * risk_amt, 2),
            "marked_pnl": round((overall["net_r"] + overall["pending_mark_r"]) * risk_amt, 2),
        }
        for b in bucket_order:
            s = buckets[b]
            s["sim_realized_balance"] = round(start_bal + (float(s.get("net_r", 0.0)) * risk_amt), 2)
            s["sim_marked_balance"] = round(start_bal + ((float(s.get("net_r", 0.0)) + float(s.get("pending_mark_r", 0.0))) * risk_amt), 2)

        by_symbol: dict[str, dict] = {}
        for r in events:
            sym = str(r["symbol"] or "UNKNOWN")
            rec = by_symbol.setdefault(sym, {"symbol": sym, "bucket": r["bucket"], "sent": 0, "resolved": 0, "wins": 0, "losses": 0, "net_r": 0.0, "pending_mark_r": 0.0})
            rec["sent"] += 1
            if r["resolved"] == 1 and r["pnl_r"] is not None:
                rec["resolved"] += 1
                rr = float(r["pnl_r"] or 0.0)
                rec["net_r"] += rr
                if rr > 0:
                    rec["wins"] += 1
                elif rr < 0:
                    rec["losses"] += 1
            elif r["current_r"] is not None:
                rec["pending_mark_r"] += float(r["current_r"] or 0.0)
        for rec in by_symbol.values():
            rec["win_rate"] = round((100.0 * rec["wins"] / rec["resolved"]) if rec["resolved"] else 0.0, 1)
            rec["net_r"] = round(rec["net_r"], 4)
            rec["pending_mark_r"] = round(rec["pending_mark_r"], 4)
            rec["session_r"] = round(rec["net_r"] + rec["pending_mark_r"], 4)
        best_symbols = sorted(by_symbol.values(), key=lambda x: (x["session_r"], x["net_r"], x["win_rate"], x["sent"]), reverse=True)[:5]
        worst_symbols = sorted(by_symbol.values(), key=lambda x: (x["session_r"], x["net_r"], x["sent"]))[:5]

        by_setup: dict[str, dict] = {}
        for r in events:
            setup = str(r["setup"] or "UNKNOWN")
            rec = by_setup.setdefault(setup, {"setup": setup, "sent": 0, "resolved": 0, "wins": 0, "losses": 0, "net_r": 0.0})
            rec["sent"] += 1
            if r["resolved"] == 1 and r["pnl_r"] is not None:
                rec["resolved"] += 1
                rr = float(r["pnl_r"] or 0.0)
                rec["net_r"] += rr
                if rr > 0:
                    rec["wins"] += 1
                elif rr < 0:
                    rec["losses"] += 1
        setup_rows = []
        for rec in by_setup.values():
            rec["win_rate"] = round((100.0 * rec["wins"] / rec["resolved"]) if rec["resolved"] else 0.0, 1)
            rec["net_r"] = round(rec["net_r"], 4)
            setup_rows.append(rec)
        setup_rows.sort(key=lambda x: (x["resolved"], x["win_rate"], x["net_r"], x["sent"]), reverse=True)

        source_counts: dict[str, int] = {}
        conf_bands = {"<70": 0, "70-74": 0, "75-79": 0, "80+": 0}
        for r in events:
            source_counts[r["source"]] = int(source_counts.get(r["source"], 0) or 0) + 1
            conf_bands[r["conf_band"]] = int(conf_bands.get(r["conf_band"], 0) or 0) + 1

        return {
            "ok": True,
            "status": "ok",
            "days": days_i,
            "timezone": str(getattr(user_tz, 'key', 'UTC')),
            "window_mode": mode,
            "local_date": str(now_local.date()),
            "window": {
                "start_local": start_local.strftime("%Y-%m-%d %H:%M"),
                "end_local": end_local.strftime("%Y-%m-%d %H:%M"),
            },
            "summary": overall,
            "simulation": sim_overall,
            "buckets": buckets,
            "bucket_order": bucket_order,
            "best_symbols": best_symbols,
            "worst_symbols": worst_symbols,
            "win_rate_by_setup": setup_rows[:10],
            "confidence_bands": conf_bands,
            "source_counts": dict(sorted(source_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
            "events_count": len(events),
            "market_filter": selected_bucket,
            "market_filter_label": selected_bucket_label,
            "symbol_filter": selected_symbol or None,
            "symbol_filter_label": selected_symbol or None,
        }

    def _collect_mt5_closed_positions(self, days: int) -> tuple[bool, str, dict[int, dict], dict]:
        ok, state = mt5_executor._ensure_connection()
        if not ok:
            return False, state, {}, {}
        mt5 = mt5_executor._mt5

        now = _utc_now()
        start = now - timedelta(days=max(1, int(days)))
        try:
            deals, query_mode = mt5_executor._history_deals_get_robust(start, now)
        except Exception as e:
            return False, f"history_deals_get failed: {e}", {}, {}

        entry_out = {
            int(getattr(mt5, "DEAL_ENTRY_OUT", 1)),
            int(getattr(mt5, "DEAL_ENTRY_OUT_BY", 3)),
            int(getattr(mt5, "DEAL_ENTRY_INOUT", 2)),
        }
        magic_expected = int(config.MT5_MAGIC)

        grouped: dict[int, dict] = {}
        for d in deals:
            try:
                magic = int(getattr(d, "magic", 0) or 0)
                if magic != magic_expected:
                    continue
                entry = int(getattr(d, "entry", -1) or -1)
                if entry not in entry_out:
                    continue
                position_id = int(getattr(d, "position_id", 0) or 0)
                deal_ticket = int(getattr(d, "ticket", 0) or 0)
                group_id = position_id if position_id > 0 else deal_ticket
                if group_id <= 0:
                    continue
                symbol = str(getattr(d, "symbol", "") or "")
                close_ts = int(getattr(d, "time", 0) or 0)
                pnl = (
                    self._safe_float(getattr(d, "profit", 0.0), 0.0)
                    + self._safe_float(getattr(d, "swap", 0.0), 0.0)
                    + self._safe_float(getattr(d, "commission", 0.0), 0.0)
                )
                rec = grouped.setdefault(
                    group_id,
                    {
                        "group_id": group_id,
                        "position_id": (position_id if position_id > 0 else None),
                        "ticket": (deal_ticket if deal_ticket > 0 else None),
                        "symbol": symbol,
                        "pnl": 0.0,
                        "close_time": close_ts,
                        "deals": 0,
                    },
                )
                rec["pnl"] += pnl
                rec["deals"] += 1
                if close_ts >= int(rec.get("close_time", 0) or 0):
                    rec["close_time"] = close_ts
                    if symbol:
                        rec["symbol"] = symbol
                    if position_id > 0:
                        rec["position_id"] = position_id
                    if deal_ticket > 0:
                        rec["ticket"] = deal_ticket
            except Exception:
                continue

        # Key the lookup by both position_id and ticket for resilient matching against
        # signal_events rows that may store one or the other depending on bridge response.
        positions: dict[int, dict] = {}
        for rec in grouped.values():
            pid = int(rec.get("position_id", 0) or 0)
            tk = int(rec.get("ticket", 0) or 0)
            if pid > 0:
                positions.setdefault(pid, rec)
            if tk > 0:
                positions.setdefault(tk, rec)

        meta = {
            "query_mode": str(query_mode or ""),
            "deals_total": len(deals),
            "closed_positions": len(grouped),
            "lookup_keys": len(positions),
        }
        return True, "", positions, meta

    def sync_outcomes_from_mt5(self, days: int = 90) -> dict:
        if not config.NEURAL_BRAIN_ENABLED:
            return {"ok": False, "status": "disabled", "message": "neural brain disabled", "updated": 0}

        ok, msg, mt5_positions, mt5_meta = self._collect_mt5_closed_positions(days=days)
        if not ok:
            return {"ok": False, "status": "error", "message": msg, "updated": 0}

        updated = 0
        matched_by_id = 0
        matched_by_symbol_time = 0
        # Build a de-duplicated list of closed positions for fallback matching.
        unique_closed: list[dict] = []
        seen_group_ids: set[int] = set()
        for rec in mt5_positions.values():
            gid = int(rec.get("group_id", 0) or 0)
            if gid <= 0 or gid in seen_group_ids:
                continue
            seen_group_ids.add(gid)
            unique_closed.append(rec)

        with self._lock:
            with self._connect() as conn:
                consumed_groups: set[int] = set()
                prev = conn.execute(
                    """
                    SELECT mt5_message
                    FROM signal_events
                    WHERE resolved = 1
                      AND mt5_status = 'filled'
                      AND mt5_message LIKE 'synced_mt5_history:%'
                    """
                ).fetchall()
                for row_prev in prev:
                    msg_prev = str(row_prev[0] or "")
                    try:
                        gid_prev = int(msg_prev.rsplit(":", 1)[-1])
                        if gid_prev > 0:
                            consumed_groups.add(gid_prev)
                    except Exception:
                        continue

                rows = conn.execute(
                    """
                    SELECT id, position_id, ticket, signal_symbol, created_at
                    FROM signal_events
                    WHERE resolved = 0 AND mt5_status = 'filled'
                    """
                ).fetchall()

                # Candidate tuple:
                # (priority, delta_seconds, neg_row_id, row_id, group_id, mode, rec)
                # priority: 0=position_id, 1=ticket, 2=symbol_time fallback.
                candidates: list[tuple[int, int, int, int, int, str, dict]] = []

                for row in rows:
                    row_id = int(row[0])
                    position_id = int(row[1] or 0)
                    ticket = int(row[2] or 0)
                    signal_symbol = str(row[3] or "")
                    created_at_raw = str(row[4] or "")
                    created_at_dt = _parse_iso(created_at_raw)
                    created_ts = int(created_at_dt.timestamp()) if created_at_dt is not None else 0
                    has_direct = False

                    if position_id > 0:
                        rec = mt5_positions.get(position_id)
                        if rec is not None:
                            gid = int(rec.get("group_id", 0) or 0)
                            if gid > 0 and gid not in consumed_groups:
                                close_ts = int(rec.get("close_time", 0) or 0)
                                delta = abs(close_ts - created_ts) if (close_ts > 0 and created_ts > 0) else 0
                                candidates.append((0, int(delta), -row_id, row_id, gid, "position_id", rec))
                                has_direct = True

                    if ticket > 0:
                        rec = mt5_positions.get(ticket)
                        if rec is not None:
                            gid = int(rec.get("group_id", 0) or 0)
                            if gid > 0 and gid not in consumed_groups:
                                close_ts = int(rec.get("close_time", 0) or 0)
                                delta = abs(close_ts - created_ts) if (close_ts > 0 and created_ts > 0) else 0
                                candidates.append((1, int(delta), -row_id, row_id, gid, "ticket", rec))
                                has_direct = True

                    # Fallback only when no reliable direct key match exists for this row.
                    if not has_direct:
                        target_sym = canonical_symbol(signal_symbol)
                        for rec in unique_closed:
                            gid = int(rec.get("group_id", 0) or 0)
                            if gid <= 0 or gid in consumed_groups:
                                continue
                            if canonical_symbol(str(rec.get("symbol", "") or "")) != target_sym:
                                continue
                            close_ts = int(rec.get("close_time", 0) or 0)
                            if close_ts <= 0:
                                continue
                            delta = abs(close_ts - created_ts) if created_ts > 0 else 0
                            # Keep fallback strict to avoid accidental cross-matches.
                            if created_ts > 0 and delta > 7 * 24 * 3600:
                                continue
                            candidates.append((2, int(delta), -row_id, row_id, gid, "symbol_time", rec))

                candidates.sort(key=lambda x: (x[0], x[1], x[2]))

                used_rows: set[int] = set()
                used_groups: set[int] = set()
                assignments: dict[int, tuple[str, dict]] = {}
                for prio, _delta, _neg_row_id, row_id, gid, mode, rec in candidates:
                    if row_id in used_rows or gid in used_groups:
                        continue
                    assignments[row_id] = (mode, rec)
                    used_rows.add(row_id)
                    used_groups.add(gid)

                for row_id, (mode, match) in assignments.items():
                    pnl = float(match.get("pnl", 0.0))
                    close_time = int(match.get("close_time", 0) or 0)
                    closed_at = _iso(datetime.fromtimestamp(close_time, tz=timezone.utc)) if close_time > 0 else _iso(_utc_now())
                    outcome = 1 if pnl > 0 else 0
                    conn.execute(
                        """
                        UPDATE signal_events
                        SET resolved = 1,
                            outcome = ?,
                            pnl = ?,
                            closed_at = ?,
                            mt5_message = ?
                        WHERE id = ?
                        """,
                        (outcome, pnl, closed_at, f"synced_mt5_history:{int(match.get('group_id', 0) or 0)}", row_id),
                    )
                    if mode in {"position_id", "ticket"}:
                        matched_by_id += 1
                    else:
                        matched_by_symbol_time += 1
                    updated += 1
                conn.commit()

        return {
            "ok": True,
            "status": "ok",
            "message": "",
            "updated": updated,
            "closed_positions": int(mt5_meta.get("closed_positions", len(unique_closed))),
            "query_mode": str(mt5_meta.get("query_mode", "")),
            "history_deals": int(mt5_meta.get("deals_total", 0)),
            "lookup_keys": int(mt5_meta.get("lookup_keys", len(mt5_positions))),
            "matched_by_id": matched_by_id,
            "matched_by_symbol_time": matched_by_symbol_time,
        }

    def backtest_report(self, days: int = 30) -> dict:
        days_i = max(1, int(days))
        since = _utc_now() - timedelta(days=days_i)
        since_iso = _iso(since)

        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT signal_symbol, source, pnl, outcome, closed_at
                    FROM signal_events
                    WHERE resolved = 1 AND closed_at >= ?
                    ORDER BY closed_at DESC
                    """,
                    (since_iso,),
                ).fetchall()

        trades = len(rows)
        if trades == 0:
            return {
                "ok": True,
                "status": "no_data",
                "mode": "signal_log",
                "days": days_i,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "avg_pnl": 0.0,
                "profit_factor": 0.0,
                "top_symbols": [],
            }

        wins = 0
        losses = 0
        sum_pos = 0.0
        sum_neg = 0.0
        net = 0.0
        by_symbol: dict[str, dict] = {}
        for symbol, source, pnl, outcome, closed_at in rows:
            p = float(pnl or 0.0)
            net += p
            if p > 0:
                wins += 1
                sum_pos += p
            elif p < 0:
                losses += 1
                sum_neg += abs(p)
            sym = str(symbol or "UNKNOWN")
            rec = by_symbol.setdefault(sym, {"symbol": sym, "trades": 0, "wins": 0, "net_pnl": 0.0})
            rec["trades"] += 1
            rec["wins"] += 1 if p > 0 else 0
            rec["net_pnl"] += p

        for rec in by_symbol.values():
            rec["win_rate"] = round((100.0 * rec["wins"] / rec["trades"]) if rec["trades"] else 0.0, 1)
            rec["net_pnl"] = round(rec["net_pnl"], 4)

        profit_factor = (sum_pos / sum_neg) if sum_neg > 0 else (999.0 if sum_pos > 0 else 0.0)
        top_symbols = sorted(
            by_symbol.values(),
            key=lambda x: (x["net_pnl"], x["win_rate"], x["trades"]),
            reverse=True,
        )[:10]

        return {
            "ok": True,
            "status": "ok",
            "mode": "signal_log",
            "days": days_i,
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round(100.0 * wins / trades, 1),
            "net_pnl": round(net, 4),
            "avg_pnl": round(net / trades, 4),
            "profit_factor": round(float(profit_factor), 3),
            "top_symbols": top_symbols,
        }

    def _load_training_rows(self, days: int) -> list[sqlite3.Row]:
        since_iso = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        with self._lock:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT *
                    FROM signal_events
                    WHERE resolved = 1
                      AND outcome IN (0, 1)
                      AND COALESCE(closed_at, created_at) >= ?
                    ORDER BY COALESCE(closed_at, created_at) ASC
                    """,
                    (since_iso,),
                ).fetchall()
        return rows

    def _rows_to_xy(self, rows: list[sqlite3.Row], recency_weight_days: int = 30) -> tuple[np.ndarray, np.ndarray]:
        xs = []
        ys = []
        ws = []  # sample weights for recency
        now_utc = _utc_now()
        import math
        recency_cutoff = now_utc - timedelta(days=max(1, int(recency_weight_days)))
        for r in rows:
            entry = max(1e-12, self._safe_float(r["entry"], 1.0))
            sl = self._safe_float(r["stop_loss"], 0.0)
            tp2 = self._safe_float(r["take_profit_2"], 0.0)
            # Time-of-day from created_at
            created_at = _parse_iso(str(r["created_at"] or ""))
            hour_frac = 0.5  # default = noon
            if created_at:
                hour_frac = (created_at.hour + created_at.minute / 60.0) / 24.0
            # MAE from extra_json
            extra = self._safe_json_load(str(r["extra_json"] or ""))
            mae_hist = float(np.clip(self._safe_float(
                (extra.get("market_eval") or {}).get("mae_hist", 0.0), 0.0
            ), 0.0, 1.0))
            # Resolve age
            resolved_at = _parse_iso(str(r["closed_at"] or "")) if r["closed_at"] else None
            resolve_age_h = 0.0
            if created_at and resolved_at:
                resolve_age_h = float(np.clip((resolved_at - created_at).total_seconds() / 3600.0, 0.0, 96.0))
            feature_dict = {
                "confidence": np.clip(self._safe_float(r["confidence"], 0.0) / 100.0, 0.0, 1.5),
                "risk_reward": np.clip(self._safe_float(r["risk_reward"], 0.0) / 5.0, 0.0, 2.0),
                "rsi": np.clip(self._safe_float(r["rsi"], 50.0) / 100.0, 0.0, 1.0),
                "atr_pct": np.clip(
                    self._safe_float(r["atr"], 0.0) / entry,
                    0.0,
                    0.5,
                ),
                "edge": np.clip(self._safe_float(r["score_edge"], 0.0) / 100.0, 0.0, 2.0),
                "long_score": np.clip(self._safe_float(r["score_long"], 0.0) / 100.0, 0.0, 3.0),
                "short_score": np.clip(self._safe_float(r["score_short"], 0.0) / 100.0, 0.0, 3.0),
                "is_long": 1.0 if str(r["direction"] or "").lower() == "long" else 0.0,
                "hour_sin": float(math.sin(2 * math.pi * hour_frac)),
                "hour_cos": float(math.cos(2 * math.pi * hour_frac)),
                "sl_ratio": float(np.clip(abs(entry - sl) / entry, 0.0, 0.20)) if sl > 0 else 0.0,
                "tp_sl_ratio": float(np.clip(abs(tp2 - entry) / max(abs(entry - sl), 1e-12), 0.0, 5.0)) if (sl > 0 and tp2 > 0) else 0.0,
                "mae_hist": float(np.clip(mae_hist, 0.0, 1.0)),
                "resolve_age_h": float(np.clip(resolve_age_h / 96.0, 0.0, 1.0)),  # normalized to 96h
            }
            feature_dict.update(self._pattern_flags(str(r["pattern"] or "")))
            feature_dict.update(self._source_flags(str(r["source"] or "")))
            xs.append(self._to_vector(feature_dict))
            ys.append(float(r["outcome"]))
            # Recency weight: 2x for recent data, 1x for older
            w = 2.0 if (created_at and created_at >= recency_cutoff) else 1.0
            ws.append(w)
        X = np.vstack(xs)
        y = np.array(ys, dtype=np.float64)
        sample_weights = np.array(ws, dtype=np.float64)
        return X, y, sample_weights

    @staticmethod
    def _sigmoid(x: np.ndarray) -> np.ndarray:
        x = np.clip(x, -50, 50)
        return 1.0 / (1.0 + np.exp(-x))

    def train_backprop(self, days: int = 120, min_samples: Optional[int] = None) -> TrainResult:
        if not config.NEURAL_BRAIN_ENABLED:
            return TrainResult(False, "disabled", "neural brain disabled")

        rows = self._load_training_rows(days=days)
        required = max(10, int(min_samples if min_samples is not None else config.NEURAL_BRAIN_MIN_SAMPLES))
        if len(rows) < required:
            return TrainResult(
                False,
                "not_enough_data",
                f"need >= {required} labeled trades, have {len(rows)}",
                samples=len(rows),
            )

        X, y, sample_weights = self._rows_to_xy(rows)
        n, d = X.shape
        val_size = max(1, int(round(0.2 * n)))
        split = max(1, n - val_size)
        if split >= n:
            split = n - 1
        X_train, y_train = X[:split], y[:split]
        X_val, y_val = X[split:], y[split:]
        w_train = sample_weights[:split]
        if len(X_val) == 0:
            X_val, y_val = X_train, y_train

        mu = X_train.mean(axis=0)
        sigma = X_train.std(axis=0)
        sigma[sigma < 1e-8] = 1.0
        X_train = (X_train - mu) / sigma
        X_val = (X_val - mu) / sigma

        hidden = max(4, int(config.NEURAL_BRAIN_HIDDEN_UNITS))
        epochs = max(50, int(config.NEURAL_BRAIN_EPOCHS))
        lr = float(config.NEURAL_BRAIN_LR)
        l2 = 1e-4
        rng = np.random.default_rng(42)

        w1 = rng.normal(0.0, 0.15, size=(d, hidden))
        b1 = np.zeros((1, hidden))
        w2 = rng.normal(0.0, 0.15, size=(hidden, 1))
        b2 = np.zeros((1, 1))

        y_train_col = y_train.reshape(-1, 1)
        # Normalize sample weights for stable gradients
        w_norm = w_train / max(w_train.mean(), 1e-8)
        w_col = w_norm.reshape(-1, 1)
        for _ in range(epochs):
            z1 = X_train @ w1 + b1
            a1 = np.maximum(0.0, z1)
            z2 = a1 @ w2 + b2
            y_hat = self._sigmoid(z2)

            # Recency-weighted loss gradient
            dz2 = (y_hat - y_train_col) * w_col / len(X_train)
            dw2 = (a1.T @ dz2) + l2 * w2
            db2 = dz2.sum(axis=0, keepdims=True)
            da1 = dz2 @ w2.T
            dz1 = da1 * (z1 > 0).astype(np.float64)
            dw1 = (X_train.T @ dz1) + l2 * w1
            db1 = dz1.sum(axis=0, keepdims=True)

            w1 -= lr * dw1
            b1 -= lr * db1
            w2 -= lr * dw2
            b2 -= lr * db2

        train_pred = (self._sigmoid(np.maximum(0.0, X_train @ w1 + b1) @ w2 + b2) >= 0.5).astype(np.float64).ravel()
        val_pred = (self._sigmoid(np.maximum(0.0, X_val @ w1 + b1) @ w2 + b2) >= 0.5).astype(np.float64).ravel()
        train_acc = float((train_pred == y_train).mean()) if len(y_train) else 0.0
        val_acc = float((val_pred == y_val).mean()) if len(y_val) else 0.0
        win_rate = float(y.mean())

        np.savez(
            self.model_path,
            w1=w1,
            b1=b1,
            w2=w2,
            b2=b2,
            mu=mu,
            sigma=sigma,
            feature_names=np.array(self.FEATURE_NAMES, dtype=object),
            trained_at=np.array([_iso(_utc_now())], dtype=object),
            samples=np.array([n], dtype=np.int64),
            train_accuracy=np.array([train_acc], dtype=np.float64),
            val_accuracy=np.array([val_acc], dtype=np.float64),
            win_rate=np.array([win_rate], dtype=np.float64),
        )
        self._model_cache = None

        return TrainResult(
            True,
            "ok",
            "training complete",
            samples=n,
            train_accuracy=train_acc,
            val_accuracy=val_acc,
            win_rate=win_rate,
        )

    def _load_model(self) -> Optional[dict]:
        if self._model_cache is not None:
            return self._model_cache
        if not self.model_path.exists():
            return None
        try:
            data = np.load(self.model_path, allow_pickle=True)
            model = {
                "w1": data["w1"],
                "b1": data["b1"],
                "w2": data["w2"],
                "b2": data["b2"],
                "mu": data["mu"],
                "sigma": data["sigma"],
                "trained_at": str(data["trained_at"][0]) if "trained_at" in data else "",
                "samples": int(data["samples"][0]) if "samples" in data else 0,
                "train_accuracy": float(data["train_accuracy"][0]) if "train_accuracy" in data else 0.0,
                "val_accuracy": float(data["val_accuracy"][0]) if "val_accuracy" in data else 0.0,
                "win_rate": float(data["win_rate"][0]) if "win_rate" in data else 0.0,
            }
            self._model_cache = model
            return model
        except Exception as e:
            logger.warning("[NeuralBrain] model load failed: %s", e)
            return None

    def model_status(self) -> dict:
        model = self._load_model()
        if not model:
            return {"available": False}
        return {
            "available": True,
            "trained_at": model.get("trained_at", ""),
            "samples": int(model.get("samples", 0)),
            "train_accuracy": round(float(model.get("train_accuracy", 0.0)), 4),
            "val_accuracy": round(float(model.get("val_accuracy", 0.0)), 4),
            "win_rate": round(float(model.get("win_rate", 0.0)), 4),
        }

    def data_status(self, days: int = 120) -> dict:
        since_iso = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total_events,
                        SUM(CASE WHEN resolved = 1 AND outcome IN (0, 1) THEN 1 ELSE 0 END) AS labeled_events,
                        SUM(CASE WHEN mt5_status = 'telegram_sent' AND resolved = 0 THEN 1 ELSE 0 END) AS pending_feedback,
                        SUM(CASE WHEN mt5_status = 'filled' AND resolved = 0 THEN 1 ELSE 0 END) AS pending_mt5,
                        SUM(CASE WHEN mt5_status = 'filled' THEN 1 ELSE 0 END) AS mt5_filled_events
                    FROM signal_events
                    WHERE created_at >= ?
                    """,
                    (since_iso,),
                ).fetchone()
        if row is None:
            return {
                "days": max(1, int(days)),
                "total_events": 0,
                "labeled_events": 0,
                "pending_feedback": 0,
                "pending_mt5": 0,
                "mt5_filled_events": 0,
            }
        return {
            "days": max(1, int(days)),
            "total_events": int(row[0] or 0),
            "labeled_events": int(row[1] or 0),
            "pending_feedback": int(row[2] or 0),
            "pending_mt5": int(row[3] or 0),
            "mt5_filled_events": int(row[4] or 0),
        }

    def execution_filter_status(self) -> dict:
        """
        Decide whether neural execution filter is ready to gate live MT5 orders.
        """
        if not config.NEURAL_BRAIN_ENABLED:
            return {"enabled": False, "ready": False, "reason": "neural_brain_disabled"}

        model = self.model_status()
        if not model.get("available"):
            return {"enabled": True, "ready": False, "reason": "model_unavailable"}

        samples = int(model.get("samples", 0) or 0)
        val_acc = float(model.get("val_accuracy", 0.0) or 0.0)
        min_samples = max(
            int(config.NEURAL_BRAIN_MIN_SAMPLES),
            int(config.NEURAL_BRAIN_FILTER_MIN_SAMPLES),
        )
        min_val_acc = float(config.NEURAL_BRAIN_FILTER_MIN_VAL_ACC)

        trained_at_raw = str(model.get("trained_at", "") or "")
        trained_at_dt = _parse_iso(trained_at_raw)
        max_age_h = max(0, int(config.NEURAL_BRAIN_FILTER_MAX_MODEL_AGE_HOURS))
        age_h = None
        if trained_at_dt is not None:
            age_h = max(0.0, (_utc_now() - trained_at_dt).total_seconds() / 3600.0)

        if samples < min_samples:
            return {
                "enabled": True,
                "ready": False,
                "reason": "insufficient_samples",
                "samples": samples,
                "required_samples": min_samples,
                "val_accuracy": val_acc,
                "required_val_accuracy": min_val_acc,
                "age_hours": age_h,
            }
        if val_acc < min_val_acc:
            return {
                "enabled": True,
                "ready": False,
                "reason": "low_val_accuracy",
                "samples": samples,
                "required_samples": min_samples,
                "val_accuracy": val_acc,
                "required_val_accuracy": min_val_acc,
                "age_hours": age_h,
            }
        if (max_age_h > 0) and (age_h is not None) and (age_h > float(max_age_h)):
            return {
                "enabled": True,
                "ready": False,
                "reason": "model_stale",
                "samples": samples,
                "required_samples": min_samples,
                "val_accuracy": val_acc,
                "required_val_accuracy": min_val_acc,
                "age_hours": age_h,
                "max_age_hours": max_age_h,
            }

        return {
            "enabled": True,
            "ready": True,
            "reason": "ready",
            "samples": samples,
            "required_samples": min_samples,
            "val_accuracy": val_acc,
            "required_val_accuracy": min_val_acc,
            "age_hours": age_h,
            "max_age_hours": max_age_h,
            "trained_at": trained_at_raw,
        }

    def confidence_adjustment(self, signal, source: str) -> dict:
        """
        Soft-adjust signal confidence using neural probability and reason-study memory.
        Never blocks a signal by itself.
        """
        if not config.NEURAL_BRAIN_ENABLED:
            return {"applied": False, "reason": "neural_disabled"}
        if not config.NEURAL_BRAIN_SOFT_ADJUST:
            return {"applied": False, "reason": "soft_adjust_disabled"}

        base = self._safe_float(getattr(signal, "confidence", 0.0), 0.0)
        weight = float(np.clip(float(config.NEURAL_BRAIN_SOFT_ADJUST_WEIGHT), 0.0, 1.0))
        max_delta = abs(float(config.NEURAL_BRAIN_SOFT_ADJUST_MAX_DELTA))
        prob = None
        model_conf = None
        neural_delta = 0.0
        components = {}

        model = self.model_status()
        if model.get("available"):
            prob = self.predict_probability(signal, source=source)
            if prob is not None:
                model_conf = float(np.clip(prob * 100.0, 0.0, 100.0))
                blended = ((1.0 - weight) * base) + (weight * model_conf)
                neural_delta = float(blended - base)
                components["neural"] = {
                    "prob": round(float(prob), 4),
                    "model_confidence": round(model_conf, 3),
                    "delta": round(neural_delta, 3),
                    "weight": round(weight, 3),
                }

        reason_study = self.reason_confidence_adjustment(signal, source=source)
        reason_delta = float(reason_study.get("delta", 0.0) or 0.0) if reason_study.get("applied") else 0.0
        if reason_study.get("applied"):
            components["reason_study"] = {
                "delta": round(reason_delta, 3),
                "matched_count": int(reason_study.get("matched_count", 0) or 0),
                "matched_tags": list(reason_study.get("matched_tags") or []),
                "avg_score": round(float(reason_study.get("avg_score", 0.0) or 0.0), 4),
            }

        if not components:
            fallback_reason = "model_unavailable"
            if model.get("available") and prob is None:
                fallback_reason = "prob_unavailable"
            if reason_study and str(reason_study.get("reason", "") or "") not in {"", "applied"}:
                fallback_reason = f"{fallback_reason}|{str(reason_study.get('reason') or '')}"
            return {"applied": False, "reason": fallback_reason}

        delta = float(np.clip(neural_delta + reason_delta, -max_delta, max_delta))
        adjusted = float(np.clip(base + delta, 0.0, 100.0))

        out = {
            "applied": True,
            "reason": "applied",
            "prob": None if prob is None else float(prob),
            "base_confidence": round(base, 3),
            "adjusted_confidence": round(adjusted, 3),
            "delta": round(delta, 3),
            "weight": round(weight, 3),
            "max_delta": round(max_delta, 3),
            "model_confidence": round(model_conf, 3) if model_conf is not None else None,
            "components": components,
        }
        if reason_study:
            out["reason_study"] = reason_study
        return out

    def predict_probability(self, signal, source: str) -> Optional[float]:
        if not config.NEURAL_BRAIN_ENABLED:
            return None
        model = self._load_model()
        if not model:
            return None
        fnames_saved = list(model.get("feature_names", self.FEATURE_NAMES) or self.FEATURE_NAMES)
        fd = self._signal_feature_dict(signal, source)
        # Build vector respecting the saved feature order (handles model/code version mismatch)
        x = np.array([float(fd.get(k, 0.0)) for k in fnames_saved], dtype=np.float64).reshape(1, -1)
        mu = model["mu"]
        sigma = model["sigma"]
        # Pad/trim if feature count changed between model save and current code
        expected_d = mu.shape[0]
        actual_d = x.shape[1]
        if actual_d < expected_d:
            x = np.pad(x, ((0, 0), (0, expected_d - actual_d)))
        elif actual_d > expected_d:
            x = x[:, :expected_d]
        x = (x - mu) / sigma
        z1 = x @ model["w1"] + model["b1"]
        a1 = np.maximum(0.0, z1)
        z2 = a1 @ model["w2"] + model["b2"]
        p = float(self._sigmoid(z2).ravel()[0])
        return p

    def label_from_mt5_close(
        self,
        ticket: int,
        close_reason: str,
        pnl_r: float,
        symbol: str = "",
        direction: str = "",
    ) -> bool:
        """
        Instantly label a signal_events row from a real MT5 close outcome.
        Called by mt5_position_manager when a close action succeeds.
        close_reason: 'TP', 'SL', 'time_stop', 'partial', etc.
        pnl_r: realized R multiple (positive = win)
        Returns True if a row was updated.
        """
        if not config.NEURAL_BRAIN_ENABLED:
            return False
        outcome = 1 if float(pnl_r) > 0 else 0
        closed_at = _iso(_utc_now())
        label_msg = f"mt5_close:{close_reason}"
        ticket_i = int(ticket or 0)
        with self._lock:
            with self._connect() as conn:
                # First try by ticket
                if ticket_i > 0:
                    row = conn.execute(
                        "SELECT id FROM signal_events WHERE (ticket=? OR position_id=?) AND resolved=0 ORDER BY id DESC LIMIT 1",
                        (ticket_i, ticket_i),
                    ).fetchone()
                    if row:
                        conn.execute(
                            "UPDATE signal_events SET resolved=1, outcome=?, pnl=?, closed_at=?, mt5_message=? WHERE id=?",
                            (outcome, float(pnl_r), closed_at, label_msg, int(row[0])),
                        )
                        conn.commit()
                        logger.debug("[NeuralBrain] label_from_mt5_close ticket=%s reason=%s R=%.3f", ticket_i, close_reason, pnl_r)
                        return True
                # Fallback: match by symbol+direction in recent unresolved rows
                if symbol and direction:
                    sym_upper = str(symbol).upper()
                    dir_lower = str(direction).lower()
                    since = _iso(_utc_now() - timedelta(hours=48))
                    row = conn.execute(
                        """
                        SELECT id FROM signal_events
                        WHERE resolved=0
                          AND created_at >= ?
                          AND (UPPER(signal_symbol)=? OR UPPER(broker_symbol)=?)
                          AND direction=?
                        ORDER BY id DESC LIMIT 1
                        """,
                        (since, sym_upper, sym_upper, dir_lower),
                    ).fetchone()
                    if row:
                        conn.execute(
                            "UPDATE signal_events SET resolved=1, outcome=?, pnl=?, closed_at=?, mt5_message=? WHERE id=?",
                            (outcome, float(pnl_r), closed_at, label_msg, int(row[0])),
                        )
                        conn.commit()
                        logger.debug("[NeuralBrain] label_from_mt5_close sym=%s dir=%s reason=%s R=%.3f", symbol, direction, close_reason, pnl_r)
                        return True
        return False


neural_brain = NeuralBrain()
