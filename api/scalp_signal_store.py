"""
api/scalp_signal_store.py - Scalping Signal Store (Separate from main signals)
Tracks XAUUSD 1M/5M scalping signals with simulation outcomes.
Stores entry/TP/SL + tracks paper-trade result automatically.
"""
import sqlite3
import json
import time
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

SCALP_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "scalp_signal_history.db"
)

_SYMBOL_ALIAS_TO_CANON = {
    "XAU": "XAUUSD",
    "GOLD": "XAUUSD",
    "XAUUSD": "XAUUSD",
    "ETHUSD": "ETHUSD",
    "ETHUSDT": "ETHUSD",
    "ETH/USDT": "ETHUSD",
    "BTCUSD": "BTCUSD",
    "BTCUSDT": "BTCUSD",
    "BTC/USDT": "BTCUSD",
}

_CANON_TO_ALIAS = {
    "XAUUSD": ("XAUUSD", "XAU", "GOLD"),
    "ETHUSD": ("ETHUSD", "ETHUSDT", "ETH/USDT"),
    "BTCUSD": ("BTCUSD", "BTCUSDT", "BTC/USDT"),
}


def _canonical_symbol(raw: str) -> str:
    token = str(raw or "").strip().upper().replace(" ", "")
    if not token:
        return ""
    if token in _SYMBOL_ALIAS_TO_CANON:
        return _SYMBOL_ALIAS_TO_CANON[token]
    compact = token.replace("/", "")
    if compact in _SYMBOL_ALIAS_TO_CANON:
        return _SYMBOL_ALIAS_TO_CANON[compact]
    return token


def _symbol_candidates(raw: str) -> tuple[str, ...]:
    original = str(raw or "").strip().upper()
    canon = _canonical_symbol(original)
    aliases = tuple(_CANON_TO_ALIAS.get(canon, (canon, original)))
    out = []
    seen = set()
    for s in aliases + (original, original.replace(" ", "")):
        token = str(s or "").strip().upper()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return tuple(out)


@dataclass
class ScalpSignalRecord:
    id: Optional[int] = None
    timestamp: float = 0.0
    symbol: str = "XAUUSD"
    direction: str = ""             # long / short
    scalp_type: str = "5m"          # 1m / 5m / behavior
    confidence: float = 0.0
    entry: float = 0.0
    stop_loss: float = 0.0
    take_profit_1: float = 0.0
    take_profit_2: float = 0.0
    take_profit_3: float = 0.0
    risk_reward: float = 0.0
    session: str = ""
    pattern: str = ""               # e.g. "sweep_rejection+fvg_retest"
    setup_detail: str = ""          # JSON with FVG zone, sweep zone, etc.
    # Filters applied
    macro_shock_filter: str = ""    # "neutral" / "dxy_up" / "tnx_rise"
    kill_zone: str = ""             # "london_kill_zone" / "ny_open_drive" etc.
    sweep_detected: bool = False
    fvg_detected: bool = False
    # Simulation outcome
    outcome: str = "pending"        # pending / tp1_hit / tp2_hit / tp3_hit / sl_hit / expired
    exit_price: float = 0.0
    pnl_pips: float = 0.0
    pnl_usd: float = 0.0           # per 0.01 lot
    exit_timestamp: float = 0.0
    holding_time_minutes: float = 0.0
    sim_notified: bool = False      # whether Telegram summary was sent


class ScalpSignalStore:
    """
    SQLite store for XAUUSD scalping signals.
    Separate from main signal_history.db to keep scalping data clean.
    """

    def __init__(self, db_path: str = SCALP_DB_PATH):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scalp_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    symbol TEXT DEFAULT 'XAUUSD',
                    direction TEXT DEFAULT '',
                    scalp_type TEXT DEFAULT '5m',
                    confidence REAL DEFAULT 0,
                    entry REAL DEFAULT 0,
                    stop_loss REAL DEFAULT 0,
                    take_profit_1 REAL DEFAULT 0,
                    take_profit_2 REAL DEFAULT 0,
                    take_profit_3 REAL DEFAULT 0,
                    risk_reward REAL DEFAULT 0,
                    session TEXT DEFAULT '',
                    pattern TEXT DEFAULT '',
                    setup_detail TEXT DEFAULT '{}',
                    macro_shock_filter TEXT DEFAULT 'neutral',
                    kill_zone TEXT DEFAULT '',
                    sweep_detected INTEGER DEFAULT 0,
                    fvg_detected INTEGER DEFAULT 0,
                    outcome TEXT DEFAULT 'pending',
                    exit_price REAL DEFAULT 0,
                    pnl_pips REAL DEFAULT 0,
                    pnl_usd REAL DEFAULT 0,
                    exit_timestamp REAL DEFAULT 0,
                    holding_time_minutes REAL DEFAULT 0,
                    sim_notified INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scalp_symbol ON scalp_signals(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scalp_outcome ON scalp_signals(outcome)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scalp_ts ON scalp_signals(timestamp DESC)")

    def store(self, rec: ScalpSignalRecord) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("""
                INSERT INTO scalp_signals (
                    timestamp, symbol, direction, scalp_type, confidence,
                    entry, stop_loss, take_profit_1, take_profit_2, take_profit_3,
                    risk_reward, session, pattern, setup_detail,
                    macro_shock_filter, kill_zone, sweep_detected, fvg_detected
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                time.time() if not rec.timestamp else rec.timestamp,
                rec.symbol, rec.direction, rec.scalp_type,
                rec.confidence, rec.entry, rec.stop_loss,
                rec.take_profit_1, rec.take_profit_2, rec.take_profit_3,
                rec.risk_reward, rec.session, rec.pattern,
                rec.setup_detail if isinstance(rec.setup_detail, str) else json.dumps(rec.setup_detail),
                rec.macro_shock_filter, rec.kill_zone,
                1 if rec.sweep_detected else 0,
                1 if rec.fvg_detected else 0,
            ))
            signal_id = cur.lastrowid
            logger.info("[ScalpStore] Stored scalp #%d: %s %s @ %.2f (conf=%.1f%%)",
                        signal_id, rec.direction, rec.symbol, rec.entry, rec.confidence)
            return signal_id

    def update_outcome(
        self,
        signal_id: int,
        outcome: str,
        exit_price: float,
        pnl_pips: float,
        pnl_usd: float,
        exit_timestamp: Optional[float] = None,
        holding_time_minutes: Optional[float] = None,
    ) -> None:
        ts = exit_timestamp or time.time()
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT timestamp FROM scalp_signals WHERE id=?", (signal_id,)).fetchone()
            hold = 0.0
            if row and holding_time_minutes is None:
                hold = (ts - row[0]) / 60.0
            else:
                hold = holding_time_minutes or 0.0
            conn.execute("""
                UPDATE scalp_signals SET
                    outcome=?, exit_price=?, pnl_pips=?, pnl_usd=?,
                    exit_timestamp=?, holding_time_minutes=?
                WHERE id=?
            """, (outcome, exit_price, pnl_pips, pnl_usd, ts, round(hold, 1), signal_id))
        logger.info("[ScalpStore] Updated #%d → %s @ %.2f (pips=%.1f, usd=%.2f)",
                    signal_id, outcome, exit_price, pnl_pips, pnl_usd)

    def mark_notified(self, signal_id: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE scalp_signals SET sim_notified=1 WHERE id=?", (signal_id,))

    def get_pending(self) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM scalp_signals WHERE outcome='pending' ORDER BY timestamp ASC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_completed_unnotified(self) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM scalp_signals
                   WHERE outcome != 'pending' AND sim_notified=0
                   ORDER BY exit_timestamp ASC"""
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _append_where(base_where: str, extra: str) -> str:
        if not extra:
            return base_where
        if base_where:
            return f"{base_where} AND {extra}"
        return f"WHERE {extra}"

    def _scoped_where(
        self,
        symbol: Optional[str] = None,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
    ) -> tuple[str, tuple]:
        conds: list[str] = []
        params: list = []
        token = str(symbol or "").strip()
        if token:
            candidates = _symbol_candidates(token)
            if candidates:
                ph = ",".join(["?"] * len(candidates))
                conds.append(f"UPPER(symbol) IN ({ph})")
                params.extend(candidates)
        if start_ts is not None:
            conds.append("timestamp >= ?")
            params.append(float(start_ts))
        if end_ts is not None:
            conds.append("timestamp < ?")
            params.append(float(end_ts))
        return ("WHERE " + " AND ".join(conds)) if conds else "", tuple(params)

    def get_stats_filtered(
        self,
        *,
        symbol: Optional[str] = None,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
        last_n: Optional[int] = 50,
    ) -> dict:
        """Compute win rate/PNL for completed scalp trades with optional filters."""
        where_base, params_base = self._scoped_where(symbol=symbol, start_ts=start_ts, end_ts=end_ts)
        where_completed = self._append_where(where_base, "outcome != 'pending' AND outcome != 'expired'")
        where_pending = self._append_where(where_base, "outcome = 'pending'")
        sql = (
            "SELECT outcome, pnl_pips, pnl_usd, holding_time_minutes "
            f"FROM scalp_signals {where_completed} "
            "ORDER BY exit_timestamp DESC"
        )
        params = list(params_base)
        if last_n is not None:
            sql += " LIMIT ?"
            params.append(max(1, int(last_n)))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            total_row = conn.execute(
                f"SELECT COUNT(*) AS c FROM scalp_signals {where_base}",
                params_base,
            ).fetchone()
            pending_row = conn.execute(
                f"SELECT COUNT(*) AS c FROM scalp_signals {where_pending}",
                params_base,
            ).fetchone()
            rows = conn.execute(sql, tuple(params)).fetchall()
        total_signals = int(total_row["c"]) if total_row is not None else 0
        pending_count = int(pending_row["c"]) if pending_row is not None else 0
        if not rows:
            return {
                "total_signals": total_signals,
                "pending_count": pending_count,
                "count": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "avg_pips": 0.0,
                "avg_usd": 0.0,
                "profit_factor": 0.0,
                "best_pips": 0.0,
                "worst_pips": 0.0,
                "total_pips": 0.0,
                "total_usd": 0.0,
            }

        total = len(rows)
        wins = sum(1 for r in rows if r["outcome"] in ("tp1_hit", "tp2_hit", "tp3_hit"))
        pips = [float(r["pnl_pips"]) for r in rows]
        usd_list = [float(r["pnl_usd"]) for r in rows]
        gross_profit = sum(p for p in pips if p > 0) or 0.0
        gross_loss = abs(sum(p for p in pips if p < 0)) or 1e-9

        return {
            "total_signals": total_signals,
            "pending_count": pending_count,
            "count": total,
            "wins": wins,
            "losses": total - wins,
            "win_rate": round(wins / total * 100, 1),
            "avg_pips": round(sum(pips) / total, 2),
            "avg_usd": round(sum(usd_list) / total, 2),
            "profit_factor": round(gross_profit / gross_loss, 2),
            "best_pips": round(max(pips), 2),
            "worst_pips": round(min(pips), 2),
            "total_pips": round(sum(pips), 2),
            "total_usd": round(sum(usd_list), 2),
        }

    def get_stats(self, last_n: int = 50) -> dict:
        """Compute win rate, avg PnL, best/worst for last N completed scalp trades."""
        return self.get_stats_filtered(last_n=last_n)

    def get_recent(self, limit: int = 10) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM scalp_signals ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]


# Singleton
scalp_store = ScalpSignalStore()
