"""
api/signal_store.py - Tiger Signal Store
SQLite-based signal history with outcome tracking.

Tracks every signal: entry, TP hit, SL hit, partial close, final P&L.
Calculates rolling win rate, profit factor, expectancy.
"""
import sqlite3
import json
import time
import logging
import os
from dataclasses import dataclass, asdict
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "signal_history.db")

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


def _normalize_rows(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for row in rows or []:
        d = dict(row or {})
        d["symbol"] = _canonical_symbol(str(d.get("symbol", "") or ""))
        out.append(d)
    return out


@dataclass
class SignalRecord:
    """Represents a signal stored in the database."""
    id: Optional[int] = None
    timestamp: float = 0.0
    symbol: str = ""
    direction: str = ""
    confidence: float = 0.0
    entry: float = 0.0
    stop_loss: float = 0.0
    take_profit_1: float = 0.0
    take_profit_2: float = 0.0
    take_profit_3: float = 0.0
    risk_reward: float = 0.0
    timeframe: str = ""
    session: str = ""
    pattern: str = ""
    source: str = ""                  # 'gold' | 'fx' | 'crypto' | 'stocks'
    # Tiger Hunter metadata
    entry_type: str = "market"
    sl_type: str = "atr"
    tp_type: str = "rr"
    sl_liquidity_mapped: bool = False
    liquidity_pools_count: int = 0
    # Outcome tracking
    outcome: str = "pending"          # 'pending' | 'tp1_hit' | 'tp2_hit' | 'tp3_hit' | 'sl_hit' | 'expired' | 'cancelled'
    exit_price: float = 0.0
    pnl_pips: float = 0.0
    pnl_usd: float = 0.0
    exit_timestamp: float = 0.0
    holding_time_minutes: float = 0.0
    # Execution
    mt5_ticket: Optional[int] = None
    mt5_executed: bool = False
    execution_status: str = ""


class SignalStore:
    """SQLite-based signal history with outcome tracking."""

    def __init__(self, db_path: str = DB_PATH):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence REAL DEFAULT 0,
                    entry REAL DEFAULT 0,
                    stop_loss REAL DEFAULT 0,
                    take_profit_1 REAL DEFAULT 0,
                    take_profit_2 REAL DEFAULT 0,
                    take_profit_3 REAL DEFAULT 0,
                    risk_reward REAL DEFAULT 0,
                    timeframe TEXT DEFAULT '',
                    session TEXT DEFAULT '',
                    pattern TEXT DEFAULT '',
                    source TEXT DEFAULT '',
                    entry_type TEXT DEFAULT 'market',
                    sl_type TEXT DEFAULT 'atr',
                    tp_type TEXT DEFAULT 'rr',
                    sl_liquidity_mapped INTEGER DEFAULT 0,
                    liquidity_pools_count INTEGER DEFAULT 0,
                    outcome TEXT DEFAULT 'pending',
                    exit_price REAL DEFAULT 0,
                    pnl_pips REAL DEFAULT 0,
                    pnl_usd REAL DEFAULT 0,
                    exit_timestamp REAL DEFAULT 0,
                    holding_time_minutes REAL DEFAULT 0,
                    mt5_ticket INTEGER,
                    mt5_executed INTEGER DEFAULT 0,
                    execution_status TEXT DEFAULT '',
                    extra_json TEXT DEFAULT '{}'
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_outcome ON signals(outcome)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp DESC)
            """)

    def store_signal(self, signal, source: str = "", mt5_ticket: Optional[int] = None,
                     mt5_executed: bool = False, execution_status: str = "") -> int:
        """
        Store a signal from the Dexter engine.
        Returns the signal ID.
        """
        now = time.time()
        raw_symbol = str(getattr(signal, "symbol", "") or "")
        stored_symbol = _canonical_symbol(raw_symbol) or raw_symbol.strip().upper()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO signals (
                    timestamp, symbol, direction, confidence, entry,
                    stop_loss, take_profit_1, take_profit_2, take_profit_3,
                    risk_reward, timeframe, session, pattern, source,
                    entry_type, sl_type, tp_type, sl_liquidity_mapped,
                    liquidity_pools_count, mt5_ticket, mt5_executed,
                    execution_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                now,
                stored_symbol,
                str(getattr(signal, "direction", "") or ""),
                float(getattr(signal, "confidence", 0) or 0),
                float(getattr(signal, "entry", 0) or 0),
                float(getattr(signal, "stop_loss", 0) or 0),
                float(getattr(signal, "take_profit_1", 0) or 0),
                float(getattr(signal, "take_profit_2", 0) or 0),
                float(getattr(signal, "take_profit_3", 0) or 0),
                float(getattr(signal, "risk_reward", 0) or 0),
                str(getattr(signal, "timeframe", "") or ""),
                str(getattr(signal, "session", "") or ""),
                str(getattr(signal, "pattern", "") or ""),
                str(source),
                str(getattr(signal, "entry_type", "market") or "market"),
                str(getattr(signal, "sl_type", "atr") or "atr"),
                str(getattr(signal, "tp_type", "rr") or "rr"),
                1 if getattr(signal, "sl_liquidity_mapped", False) else 0,
                int(getattr(signal, "liquidity_pools_count", 0) or 0),
                mt5_ticket,
                1 if mt5_executed else 0,
                str(execution_status),
            ))
            signal_id = cursor.lastrowid
            logger.info("[SignalStore] Stored signal #%d: %s %s @ %.4f (conf=%.1f)",
                        signal_id,
                        getattr(signal, "direction", ""),
                        stored_symbol,
                        getattr(signal, "entry", 0),
                        getattr(signal, "confidence", 0))
            return signal_id

    def update_outcome(self, signal_id: int, outcome: str, exit_price: float = 0.0,
                       pnl_pips: float = 0.0, pnl_usd: float = 0.0):
        """Update the outcome of a stored signal."""
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            # Get original timestamp for holding time
            row = conn.execute(
                "SELECT timestamp FROM signals WHERE id = ?", (signal_id,)
            ).fetchone()
            holding_minutes = 0.0
            if row:
                holding_minutes = (now - row[0]) / 60.0

            conn.execute("""
                UPDATE signals SET
                    outcome = ?,
                    exit_price = ?,
                    pnl_pips = ?,
                    pnl_usd = ?,
                    exit_timestamp = ?,
                    holding_time_minutes = ?
                WHERE id = ?
            """, (outcome, exit_price, pnl_pips, pnl_usd, now, holding_minutes, signal_id))
            logger.info("[SignalStore] Updated signal #%d: outcome=%s, pnl=%.2f pips, $%.2f",
                        signal_id, outcome, pnl_pips, pnl_usd)

    def get_all_pending(self, limit: int = 100) -> list[dict]:
        """Get all pending signals for simulator. No limit by default."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM signals WHERE outcome = 'pending' ORDER BY timestamp ASC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_active_signals(self, limit: int = 20) -> list[dict]:
        """Get signals with pending outcome."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM signals WHERE outcome = 'pending' ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return _normalize_rows([dict(r) for r in rows])

    def get_recent_signals(self, limit: int = 50) -> list[dict]:
        """Get most recent signals regardless of outcome."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return _normalize_rows([dict(r) for r in rows])

    def get_signal_history(self, symbol: str = None, limit: int = 100) -> list[dict]:
        """Get completed signals, optionally filtered by symbol."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if symbol:
                candidates = _symbol_candidates(str(symbol or ""))
                ph = ",".join(["?"] * len(candidates))
                rows = conn.execute(
                    f"SELECT * FROM signals WHERE outcome != 'pending' AND UPPER(symbol) IN ({ph}) "
                    "ORDER BY timestamp DESC LIMIT ?",
                    tuple(candidates) + (limit,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM signals WHERE outcome != 'pending' "
                    "ORDER BY timestamp DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            return _normalize_rows([dict(r) for r in rows])

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

    def get_performance_stats_filtered(
        self,
        symbol: Optional[str] = None,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
    ) -> dict:
        """Calculate rolling performance statistics with optional symbol/time filters."""
        where_base, params_base = self._scoped_where(symbol=symbol, start_ts=start_ts, end_ts=end_ts)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Total signals
            total = conn.execute(
                f"SELECT COUNT(*) as c FROM signals {where_base}",
                params_base,
            ).fetchone()["c"]
            where_completed = self._append_where(where_base, "outcome != 'pending'")
            completed = conn.execute(
                f"SELECT COUNT(*) as c FROM signals {where_completed}",
                params_base,
            ).fetchone()["c"]

            if completed == 0:
                return {
                    "total_signals": total,
                    "completed_signals": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "total_pnl_usd": 0.0,
                    "total_pnl_pips": 0.0,
                    "avg_pnl_per_trade": 0.0,
                    "avg_holding_minutes": 0.0,
                    "best_trade_usd": 0.0,
                    "worst_trade_usd": 0.0,
                    "tiger_stats": {
                        "anti_sweep_sl_pct": 0.0,
                        "liquidity_tp_pct": 0.0,
                        "limit_entry_pct": 0.0,
                    },
                }

            # Win/loss counts
            where_wins = self._append_where(where_base, "outcome IN ('tp1_hit', 'tp2_hit', 'tp3_hit')")
            wins = conn.execute(
                f"SELECT COUNT(*) as c FROM signals {where_wins}",
                params_base,
            ).fetchone()["c"]
            where_losses = self._append_where(where_base, "outcome = 'sl_hit'")
            losses = conn.execute(
                f"SELECT COUNT(*) as c FROM signals {where_losses}",
                params_base,
            ).fetchone()["c"]

            # P&L aggregates
            where_profit = self._append_where(where_base, "pnl_usd > 0")
            total_profit = conn.execute(
                f"SELECT COALESCE(SUM(pnl_usd), 0) as s FROM signals {where_profit}",
                params_base,
            ).fetchone()["s"]
            where_loss_sum = self._append_where(where_base, "pnl_usd < 0")
            total_loss = abs(conn.execute(
                f"SELECT COALESCE(SUM(pnl_usd), 0) as s FROM signals {where_loss_sum}",
                params_base,
            ).fetchone()["s"])

            total_pnl_usd = conn.execute(
                f"SELECT COALESCE(SUM(pnl_usd), 0) as s FROM signals {where_completed}",
                params_base,
            ).fetchone()["s"]
            total_pnl_pips = conn.execute(
                f"SELECT COALESCE(SUM(pnl_pips), 0) as s FROM signals {where_completed}",
                params_base,
            ).fetchone()["s"]

            avg_holding = conn.execute(
                f"SELECT AVG(holding_time_minutes) as a FROM signals {where_completed}",
                params_base,
            ).fetchone()["a"] or 0.0

            best = conn.execute(
                f"SELECT MAX(pnl_usd) as m FROM signals {where_base}",
                params_base,
            ).fetchone()["m"] or 0.0
            worst = conn.execute(
                f"SELECT MIN(pnl_usd) as m FROM signals {where_base}",
                params_base,
            ).fetchone()["m"] or 0.0

            # Tiger Hunter stats
            where_anti_sweep = self._append_where(where_base, "sl_type = 'anti_sweep'")
            anti_sweep_count = conn.execute(
                f"SELECT COUNT(*) as c FROM signals {where_anti_sweep}",
                params_base,
            ).fetchone()["c"]
            where_liq_tp = self._append_where(where_base, "tp_type = 'liquidity'")
            liq_tp_count = conn.execute(
                f"SELECT COUNT(*) as c FROM signals {where_liq_tp}",
                params_base,
            ).fetchone()["c"]
            where_limit_entry = self._append_where(where_base, "entry_type = 'limit'")
            limit_entry_count = conn.execute(
                f"SELECT COUNT(*) as c FROM signals {where_limit_entry}",
                params_base,
            ).fetchone()["c"]

            win_rate = (wins / completed * 100) if completed > 0 else 0.0
            profit_factor = (total_profit / max(total_loss, 0.01))

            return {
                "total_signals": total,
                "completed_signals": completed,
                "wins": wins,
                "losses": losses,
                "win_rate": round(win_rate, 1),
                "profit_factor": round(profit_factor, 2),
                "total_pnl_usd": round(total_pnl_usd, 2),
                "total_pnl_pips": round(total_pnl_pips, 1),
                "avg_pnl_per_trade": round(total_pnl_usd / max(completed, 1), 2),
                "avg_holding_minutes": round(avg_holding, 1),
                "best_trade_usd": round(best, 2),
                "worst_trade_usd": round(worst, 2),
                "tiger_stats": {
                    "anti_sweep_sl_pct": round(anti_sweep_count / max(total, 1) * 100, 1),
                    "liquidity_tp_pct": round(liq_tp_count / max(total, 1) * 100, 1),
                    "limit_entry_pct": round(limit_entry_count / max(total, 1) * 100, 1),
                },
            }

    def get_performance_stats(self) -> dict:
        """Calculate rolling performance statistics."""
        return self.get_performance_stats_filtered(symbol=None, start_ts=None, end_ts=None)

    def get_equity_curve(self, initial_equity: float = 15.0) -> list[dict]:
        """Build equity curve from signal history."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT timestamp, pnl_usd FROM signals "
                "WHERE outcome != 'pending' ORDER BY exit_timestamp ASC"
            ).fetchall()

        curve = [{"timestamp": 0, "equity": initial_equity}]
        equity = initial_equity
        for row in rows:
            equity += float(row["pnl_usd"])
            curve.append({
                "timestamp": float(row["timestamp"]),
                "equity": round(equity, 2),
            })
        return curve


# Singleton
signal_store = SignalStore()
