"""
backtest/candle_store.py — SQLite cache for historical OHLCV candles.
Accumulates data over time, solving yfinance's short history limits.
"""
from __future__ import annotations

import csv
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).parent / "candle_data.db"


class CandleStore:
    """Persistent candle storage backed by SQLite."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = str(db_path or DEFAULT_DB)
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_schema()

    # ── connection ──────────────────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                symbol  TEXT    NOT NULL,
                tf      TEXT    NOT NULL,
                ts      TEXT    NOT NULL,
                open    REAL    NOT NULL,
                high    REAL    NOT NULL,
                low     REAL    NOT NULL,
                close   REAL    NOT NULL,
                volume  REAL    NOT NULL DEFAULT 0,
                PRIMARY KEY (symbol, tf, ts)
            )
        """)
        conn.commit()

    # ── ingest ──────────────────────────────────────────────────────────────

    def ingest(self, symbol: str, tf: str, df: pd.DataFrame) -> int:
        """Upsert a DataFrame of OHLCV rows. Returns rows written."""
        if df is None or df.empty:
            return 0
        conn = self._get_conn()
        rows = []
        for ts, row in df.iterrows():
            ts_str = str(ts)
            rows.append((
                symbol.upper(), tf,
                ts_str,
                float(row["open"]), float(row["high"]),
                float(row["low"]), float(row["close"]),
                float(row.get("volume", 0)),
            ))
        conn.executemany(
            "INSERT OR REPLACE INTO candles (symbol,tf,ts,open,high,low,close,volume) "
            "VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
        logger.info("[CandleStore] Ingested %d bars for %s/%s", len(rows), symbol, tf)
        return len(rows)

    def ingest_from_yfinance(self, symbol: str = "XAUUSD", tf: str = "5m") -> int:
        """Convenience: download from yfinance via xauusd_provider and store."""
        from market.data_fetcher import xauusd_provider
        df = xauusd_provider.fetch(tf, bars=9999)
        if df is None or df.empty:
            logger.warning("[CandleStore] yfinance returned no data for %s/%s", symbol, tf)
            return 0
        return self.ingest(symbol, tf, df)

    def ingest_from_csv(self, csv_path: str, symbol: str = "XAUUSD", tf: str = "5m") -> int:
        """Import from CSV. Expected columns: timestamp/date/time, open, high, low, close, volume."""
        path = Path(csv_path)
        if not path.exists():
            logger.error("[CandleStore] CSV not found: %s", csv_path)
            return 0
        df = pd.read_csv(csv_path, parse_dates=True, index_col=0)
        df.columns = [str(c).strip().lower() for c in df.columns]
        required = ["open", "high", "low", "close"]
        if not all(c in df.columns for c in required):
            logger.error("[CandleStore] CSV missing required columns: %s", required)
            return 0
        if "volume" not in df.columns:
            df["volume"] = 0.0
        return self.ingest(symbol, tf, df)

    def ingest_from_ctrader(
        self, symbol: str = "XAUUSD", tf: str = "5m", days: int = 60
    ) -> int:
        """Fetch historical bars from cTrader OpenAPI and store.

        Uses ProtoOAGetTrendbarsReq via ctrader_executor subprocess worker.
        cTrader provides ~48-70 days of M5 data (vs yfinance's 5 days).

        Args:
            symbol: e.g. "XAUUSD"
            tf: timeframe string matching cTrader periods (1m,5m,15m,30m,1h,4h,1d)
            days: how many days back to fetch

        Returns:
            Number of bars ingested
        """
        import time as _time
        try:
            from execution.ctrader_executor import ctrader_executor
        except ImportError:
            logger.error("[CandleStore] ctrader_executor not available")
            return 0

        to_ms = int(_time.time() * 1000)
        from_ms = to_ms - (days * 24 * 60 * 60 * 1000)

        total_ingested = 0
        chunk_from = from_ms

        # Fetch in chunks to handle the 14,000 bar limit
        while chunk_from < to_ms:
            result = ctrader_executor.fetch_trendbars(
                symbol=symbol,
                timeframe=tf,
                from_ms=chunk_from,
                to_ms=to_ms,
                count=5000,
            )
            if not result.get("ok"):
                logger.warning(
                    "[CandleStore] cTrader fetch failed: %s",
                    result.get("message", result.get("status", "unknown")),
                )
                break

            bars = result.get("bars", [])
            if not bars:
                break

            # Convert to DataFrame
            rows = []
            for bar in bars:
                rows.append({
                    "timestamp": pd.Timestamp(bar["ts_ms"], unit="ms", tz="UTC"),
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"]),
                    "volume": float(bar.get("volume", 0)),
                })
            if not rows:
                break
            df = pd.DataFrame(rows)
            df.set_index("timestamp", inplace=True)
            n = self.ingest(symbol, tf, df)
            total_ingested += n

            # Move chunk forward past last bar
            last_ts_ms = bars[-1]["ts_ms"]
            if last_ts_ms <= chunk_from:
                break  # no progress, avoid infinite loop
            chunk_from = last_ts_ms + 1

            has_more = result.get("has_more", False)
            if not has_more:
                break

            logger.info("[CandleStore] cTrader chunk: +%d bars, continuing...", n)

        logger.info("[CandleStore] cTrader total: %d bars for %s/%s (%d days)", total_ingested, symbol, tf, days)
        return total_ingested

    # ── fetch ───────────────────────────────────────────────────────────────

    def fetch(self, symbol: str, tf: str,
              start: Optional[datetime] = None,
              end: Optional[datetime] = None,
              bars: Optional[int] = None) -> Optional[pd.DataFrame]:
        """Return candles as DataFrame matching xauusd_provider.fetch() format.

        If bars is given without start, returns the last `bars` candles before `end`.
        """
        conn = self._get_conn()
        query = "SELECT ts, open, high, low, close, volume FROM candles WHERE symbol=? AND tf=?"
        params: list = [symbol.upper(), tf]

        if start is not None:
            query += " AND ts >= ?"
            params.append(str(start))
        if end is not None:
            query += " AND ts <= ?"
            params.append(str(end))

        query += " ORDER BY ts ASC"

        if bars is not None and start is None:
            # Fetch all up to end, then tail
            rows = conn.execute(query, params).fetchall()
            rows = rows[-bars:] if bars else rows
        else:
            rows = conn.execute(query, params).fetchall()

        if not rows:
            return None

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        df = df.astype(float)
        return df

    # ── metadata ────────────────────────────────────────────────────────────

    def coverage(self, symbol: str, tf: str) -> Tuple[Optional[str], Optional[str], int]:
        """Return (earliest_ts, latest_ts, bar_count) for a symbol/tf pair."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT MIN(ts), MAX(ts), COUNT(*) FROM candles WHERE symbol=? AND tf=?",
            (symbol.upper(), tf),
        ).fetchone()
        if row is None or row[2] == 0:
            return None, None, 0
        return row[0], row[1], row[2]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
