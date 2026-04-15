"""
Enhanced SQLite State Store — adapted from hermes-agent's hermes_state.py.

Provides persistent session storage with FTS5 full-text search for trade
history, signal history, and pattern learning. Replaces the in-memory-only
approach of the original mempalace2.

Key features:
- WAL mode for concurrent readers + one writer
- FTS5 virtual table for fast search across trade history
- Session tracking with token/cost metrics
- Pattern storage for self-learning trade skills
"""

import json
import logging
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("mempalace2.enhanced.state_store")

DEFAULT_DB_PATH = Path.home() / ".mempalace2" / "state.db"
SCHEMA_VERSION = 3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'cli',
    started_at REAL NOT NULL,
    ended_at REAL,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    total_pnl REAL DEFAULT 0.0,
    max_drawdown_pct REAL DEFAULT 0.0,
    sharpe_ratio REAL DEFAULT 0.0,
    symbols TEXT DEFAULT '[]',
    config TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS trade_history (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    stop_loss REAL NOT NULL,
    take_profit_1 REAL,
    take_profit_2 REAL,
    take_profit_3 REAL,
    quantity REAL NOT NULL,
    position_size_pct REAL,
    strategy TEXT,
    confidence REAL,
    risk_reward_ratio REAL,
    status TEXT NOT NULL DEFAULT 'open',
    pnl REAL DEFAULT 0.0,
    pnl_pct REAL DEFAULT 0.0,
    reasoning TEXT,
    signal_data TEXT DEFAULT '{}',
    market_context TEXT DEFAULT '{}',
    tp_hits TEXT DEFAULT '[]',
    opened_at REAL NOT NULL,
    closed_at REAL,
    close_reason TEXT
);

CREATE TABLE IF NOT EXISTS signal_history (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    direction TEXT NOT NULL,
    setup_type TEXT,
    confidence REAL,
    approved INTEGER DEFAULT 0,
    rejection_reason TEXT,
    signal_data TEXT DEFAULT '{}',
    indicators TEXT DEFAULT '{}',
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_patterns (
    id TEXT PRIMARY KEY,
    pattern_name TEXT NOT NULL,
    pattern_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    conditions TEXT NOT NULL DEFAULT '{}',
    win_rate REAL DEFAULT 0.0,
    avg_pnl_pct REAL DEFAULT 0.0,
    avg_risk_reward REAL DEFAULT 0.0,
    sample_count INTEGER DEFAULT 0,
    last_seen REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS market_context_snapshots (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    price REAL NOT NULL,
    indicators TEXT DEFAULT '{}',
    sentiment TEXT DEFAULT 'neutral',
    volatility_regime TEXT DEFAULT 'normal',
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS learning_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    trade_id TEXT,
    description TEXT,
    lesson TEXT,
    metadata TEXT DEFAULT '{}',
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trade_history(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_session ON trade_history(session_id);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trade_history(status);
CREATE INDEX IF NOT EXISTS idx_trades_opened ON trade_history(opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signal_history(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_session ON signal_history(session_id);
CREATE INDEX IF NOT EXISTS idx_patterns_type ON trade_patterns(pattern_type);
CREATE INDEX IF NOT EXISTS idx_patterns_symbol ON trade_patterns(symbol);
CREATE INDEX IF NOT EXISTS idx_learning_type ON learning_events(event_type);
"""

FTS_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS trade_history_fts USING fts5(
    reasoning,
    strategy,
    signal_data,
    content=trade_history,
    content_rowid=rowid
);

CREATE TRIGGER IF NOT EXISTS trade_fts_insert AFTER INSERT ON trade_history BEGIN
    INSERT INTO trade_history_fts(rowid, reasoning, strategy, signal_data)
    VALUES (new.rowid, new.reasoning, new.strategy, new.signal_data);
END;

CREATE TRIGGER IF NOT EXISTS trade_fts_delete AFTER DELETE ON trade_history BEGIN
    INSERT INTO trade_history_fts(trade_history_fts, rowid, reasoning, strategy, signal_data)
    VALUES ('delete', old.rowid, old.reasoning, old.strategy, old.signal_data);
END;

CREATE TRIGGER IF NOT EXISTS trade_fts_update AFTER UPDATE ON trade_history BEGIN
    INSERT INTO trade_history_fts(trade_history_fts, rowid, reasoning, strategy, signal_data)
    VALUES ('delete', old.rowid, old.reasoning, old.strategy, old.signal_data);
    INSERT INTO trade_history_fts(rowid, reasoning, strategy, signal_data)
    VALUES (new.rowid, new.reasoning, new.strategy, new.signal_data);
END;
"""

MIGRATIONS = {
    1: [],  # initial
    2: [
        """CREATE TABLE IF NOT EXISTS learning_events (
            id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            trade_id TEXT,
            description TEXT,
            lesson TEXT,
            metadata TEXT DEFAULT '{}',
            created_at REAL NOT NULL
        );""",
    ],
    3: [
        """CREATE TABLE IF NOT EXISTS market_context_snapshots (
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            price REAL NOT NULL,
            indicators TEXT DEFAULT '{}',
            sentiment TEXT DEFAULT 'neutral',
            volatility_regime TEXT DEFAULT 'normal',
            created_at REAL NOT NULL
        );""",
    ],
}


class StateStore:
    """Persistent state store with FTS5 search for trade intelligence."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._conn_lock = threading.RLock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Thread-local connection with WAL mode."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                str(self.db_path),
                timeout=10,
                check_same_thread=False,
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    @contextmanager
    def _transaction(self):
        """Context manager for transactions."""
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _init_db(self):
        """Initialize or migrate the database."""
        with self._transaction() as conn:
            # Check current version
            try:
                row = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
                current = row["version"] if row else 0
            except sqlite3.OperationalError:
                current = 0

            if current == 0:
                # Fresh install
                conn.executescript(SCHEMA_SQL)
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
                logger.info(f"State store initialized at {self.db_path}")
            elif current < SCHEMA_VERSION:
                # Migrate
                for ver in range(current + 1, SCHEMA_VERSION + 1):
                    for sql in MIGRATIONS.get(ver, []):
                        conn.execute(sql)
                conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))
                logger.info(f"State store migrated to v{SCHEMA_VERSION}")

            # Always ensure FTS exists
            try:
                conn.executescript(FTS_SQL)
            except sqlite3.OperationalError:
                pass  # Already exists

    # ── Session Management ──────────────────────────────

    def create_session(self, source: str = "cli", symbols: List[str] = None,
                       config: Dict = None) -> str:
        """Create a new trading session."""
        session_id = uuid.uuid4().hex[:16]
        with self._transaction() as conn:
            conn.execute(
                """INSERT INTO sessions (id, source, started_at, symbols, config)
                   VALUES (?, ?, ?, ?, ?)""",
                (session_id, source, time.time(),
                 json.dumps(symbols or []), json.dumps(config or {}))
            )
        logger.info(f"Session created: {session_id}")
        return session_id

    def end_session(self, session_id: str, stats: Dict = None):
        """End a trading session with summary stats."""
        with self._transaction() as conn:
            conn.execute(
                """UPDATE sessions SET ended_at = ?, total_trades = ?,
                   winning_trades = ?, losing_trades = ?, total_pnl = ?,
                   max_drawdown_pct = ?, sharpe_ratio = ?
                   WHERE id = ?""",
                (time.time(),
                 stats.get("total_trades", 0),
                 stats.get("winning_trades", 0),
                 stats.get("losing_trades", 0),
                 stats.get("total_pnl", 0.0),
                 stats.get("max_drawdown_pct", 0.0),
                 stats.get("sharpe_ratio", 0.0),
                 session_id)
            )

    # ── Trade History ───────────────────────────────────

    def record_trade(self, session_id: str, trade: Dict) -> str:
        """Record a trade (open or update). Returns trade_id."""
        trade_id = trade.get("id", uuid.uuid4().hex[:12])
        with self._transaction() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO trade_history
                   (id, session_id, symbol, direction, entry_price, exit_price,
                    stop_loss, take_profit_1, take_profit_2, take_profit_3,
                    quantity, position_size_pct, strategy, confidence,
                    risk_reward_ratio, status, pnl, pnl_pct, reasoning,
                    signal_data, market_context, tp_hits, opened_at, closed_at,
                    close_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_id, session_id,
                 trade.get("symbol", ""), trade.get("direction", ""),
                 trade.get("entry_price", 0), trade.get("exit_price"),
                 trade.get("stop_loss", 0),
                 trade.get("take_profit_1", 0), trade.get("take_profit_2", 0),
                 trade.get("take_profit_3", 0),
                 trade.get("quantity", 0), trade.get("position_size_pct", 0),
                 trade.get("strategy", ""), trade.get("confidence", 0),
                 trade.get("risk_reward_ratio", 0),
                 trade.get("status", "open"),
                 trade.get("pnl", 0), trade.get("pnl_pct", 0),
                 trade.get("reasoning", ""),
                 json.dumps(trade.get("signal_data", {})),
                 json.dumps(trade.get("market_context", {})),
                 json.dumps(trade.get("tp_hits", [])),
                 trade.get("opened_at", time.time()),
                 trade.get("closed_at"),
                 trade.get("close_reason", ""))
            )
        return trade_id

    def close_trade(self, trade_id: str, exit_price: float, pnl: float,
                    pnl_pct: float, reason: str):
        """Close a trade and update stats."""
        with self._transaction() as conn:
            conn.execute(
                """UPDATE trade_history
                   SET status = 'closed', exit_price = ?, pnl = ?, pnl_pct = ?,
                       closed_at = ?, close_reason = ?
                   WHERE id = ?""",
                (exit_price, pnl, pnl_pct, time.time(), reason, trade_id)
            )

    # ── Signal History ──────────────────────────────────

    def record_signal(self, session_id: str, signal: Dict) -> str:
        """Record a trade signal (approved or rejected)."""
        sig_id = signal.get("id", uuid.uuid4().hex[:12])
        with self._transaction() as conn:
            conn.execute(
                """INSERT INTO signal_history
                   (id, session_id, symbol, timeframe, direction, setup_type,
                    confidence, approved, rejection_reason, signal_data,
                    indicators, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (sig_id, session_id,
                 signal.get("symbol", ""), signal.get("timeframe", ""),
                 signal.get("direction", ""), signal.get("setup_type", ""),
                 signal.get("confidence", 0),
                 1 if signal.get("approved", False) else 0,
                 signal.get("rejection_reason", ""),
                 json.dumps(signal.get("data", {})),
                 json.dumps(signal.get("indicators", {})),
                 time.time())
            )
        return sig_id

    # ── Trade Patterns (Self-Learning) ──────────────────

    def record_pattern(self, pattern: Dict) -> str:
        """Record or update a detected trade pattern."""
        pattern_id = pattern.get("id", uuid.uuid4().hex[:12])
        now = time.time()
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT id FROM trade_patterns WHERE pattern_name = ? AND symbol = ?",
                (pattern.get("pattern_name", ""), pattern.get("symbol", ""))
            ).fetchone()

            if existing:
                conn.execute(
                    """UPDATE trade_patterns
                       SET win_rate = ?, avg_pnl_pct = ?, avg_risk_reward = ?,
                           sample_count = sample_count + 1, last_seen = ?, updated_at = ?
                       WHERE id = ?""",
                    (pattern.get("win_rate", 0), pattern.get("avg_pnl_pct", 0),
                     pattern.get("avg_risk_reward", 0), now, now, existing["id"])
                )
                return existing["id"]
            else:
                conn.execute(
                    """INSERT INTO trade_patterns
                       (id, pattern_name, pattern_type, symbol, timeframe,
                        conditions, win_rate, avg_pnl_pct, avg_risk_reward,
                        sample_count, last_seen, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                    (pattern_id,
                     pattern.get("pattern_name", ""), pattern.get("pattern_type", ""),
                     pattern.get("symbol", ""), pattern.get("timeframe", ""),
                     json.dumps(pattern.get("conditions", {})),
                     pattern.get("win_rate", 0), pattern.get("avg_pnl_pct", 0),
                     pattern.get("avg_risk_reward", 0),
                     now, now, now)
                )
        return pattern_id

    def get_best_patterns(self, symbol: str = None, min_samples: int = 5,
                          limit: int = 10) -> List[Dict]:
        """Get the best-performing patterns by win rate."""
        with self._transaction() as conn:
            if symbol:
                rows = conn.execute(
                    """SELECT * FROM trade_patterns
                       WHERE symbol = ? AND sample_count >= ?
                       ORDER BY win_rate DESC, avg_pnl_pct DESC LIMIT ?""",
                    (symbol, min_samples, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM trade_patterns
                       WHERE sample_count >= ?
                       ORDER BY win_rate DESC, avg_pnl_pct DESC LIMIT ?""",
                    (min_samples, limit)
                ).fetchall()
        return [dict(r) for r in rows]

    # ── Learning Events ─────────────────────────────────

    def record_learning_event(self, event_type: str, trade_id: str = None,
                              description: str = "", lesson: str = "",
                              metadata: Dict = None):
        """Record a learning event (mistake, insight, pattern discovery)."""
        with self._transaction() as conn:
            conn.execute(
                """INSERT INTO learning_events
                   (id, event_type, trade_id, description, lesson, metadata, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (uuid.uuid4().hex[:12], event_type, trade_id,
                 description, lesson, json.dumps(metadata or {}), time.time())
            )

    def get_learning_events(self, event_type: str = None, limit: int = 20) -> List[Dict]:
        """Get recent learning events."""
        with self._transaction() as conn:
            if event_type:
                rows = conn.execute(
                    """SELECT * FROM learning_events WHERE event_type = ?
                       ORDER BY created_at DESC LIMIT ?""",
                    (event_type, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM learning_events
                       ORDER BY created_at DESC LIMIT ?""",
                    (limit,)
                ).fetchall()
        return [dict(r) for r in rows]

    # ── Market Context Snapshots ────────────────────────

    def record_market_snapshot(self, snapshot: Dict) -> str:
        """Record a market context snapshot for pattern analysis."""
        snap_id = uuid.uuid4().hex[:12]
        with self._transaction() as conn:
            conn.execute(
                """INSERT INTO market_context_snapshots
                   (id, symbol, timeframe, price, indicators, sentiment,
                    volatility_regime, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (snap_id,
                 snapshot.get("symbol", ""), snapshot.get("timeframe", ""),
                 snapshot.get("price", 0),
                 json.dumps(snapshot.get("indicators", {})),
                 snapshot.get("sentiment", "neutral"),
                 snapshot.get("volatility_regime", "normal"),
                 time.time())
            )
        return snap_id

    # ── Full-Text Search ────────────────────────────────

    def search_trades(self, query: str, limit: int = 20) -> List[Dict]:
        """Full-text search across trade reasoning and strategy."""
        with self._transaction() as conn:
            rows = conn.execute(
                """SELECT t.*, rank
                   FROM trade_history_fts fts
                   JOIN trade_history t ON t.rowid = fts.rowid
                   WHERE trade_history_fts MATCH ?
                   ORDER BY rank LIMIT ?""",
                (query, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Analytics ───────────────────────────────────────

    def get_trade_stats(self, session_id: str = None, symbol: str = None) -> Dict:
        """Get aggregate trade statistics."""
        query = "SELECT COUNT(*) as total, SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins, SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses, SUM(pnl) as total_pnl, AVG(pnl_pct) as avg_pnl_pct, AVG(risk_reward_ratio) as avg_rr, MAX(pnl) as best_trade, MIN(pnl) as worst_trade FROM trade_history WHERE status = 'closed'"
        params = []
        if session_id:
            query += " AND session_id = ?"
            params.append(session_id)
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)

        with self._transaction() as conn:
            row = conn.execute(query, params).fetchone()
            if row and row["total"] > 0:
                return {
                    "total_trades": row["total"],
                    "wins": row["wins"],
                    "losses": row["losses"],
                    "win_rate": row["wins"] / row["total"] if row["total"] > 0 else 0,
                    "total_pnl": row["total_pnl"] or 0,
                    "avg_pnl_pct": row["avg_pnl_pct"] or 0,
                    "avg_risk_reward": row["avg_rr"] or 0,
                    "best_trade": row["best_trade"] or 0,
                    "worst_trade": row["worst_trade"] or 0,
                    "profit_factor": (
                        abs(row["total_pnl"] / min(row["worst_trade"], -0.01))
                        if row["worst_trade"] and row["worst_trade"] < 0 else 0
                    ),
                }
        return {"total_trades": 0, "win_rate": 0, "total_pnl": 0}

    def get_strategy_performance(self) -> List[Dict]:
        """Get performance breakdown by strategy."""
        with self._transaction() as conn:
            rows = conn.execute(
                """SELECT strategy, COUNT(*) as trades,
                          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                          SUM(pnl) as total_pnl, AVG(pnl_pct) as avg_pnl_pct,
                          AVG(risk_reward_ratio) as avg_rr
                   FROM trade_history
                   WHERE status = 'closed' AND strategy != ''
                   GROUP BY strategy
                   ORDER BY total_pnl DESC"""
            ).fetchall()
        return [dict(r) for r in rows]

    def get_hourly_performance(self) -> List[Dict]:
        """Get win rate by hour of day (for timing analysis)."""
        with self._transaction() as conn:
            rows = conn.execute(
                """SELECT CAST(strftime('%H', opened_at, 'unixepoch') AS INTEGER) as hour,
                          COUNT(*) as trades,
                          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                          AVG(pnl_pct) as avg_pnl_pct
                   FROM trade_history
                   WHERE status = 'closed'
                   GROUP BY hour
                   ORDER BY hour"""
            ).fetchall()
        return [dict(r) for r in rows]
