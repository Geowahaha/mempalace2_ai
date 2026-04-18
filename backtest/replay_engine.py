"""
backtest/replay_engine.py — Monkey-patches data providers to serve
historical candles from CandleStore, making scanners think they see live data.
Supports XAUUSD (xauusd_provider) and crypto (_fetch_ctrader_ohlcv on scanner).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from backtest.candle_store import CandleStore

logger = logging.getLogger(__name__)

# Symbols that route through xauusd_provider
_XAU_SYMBOLS = {"XAUUSD", "GOLD", "GC"}
_CRYPTO_SYMBOLS = {"BTCUSD", "ETHUSD"}


class ReplayEngine:
    """Replay historical candles through the live scanner pipeline."""

    def __init__(self, store: CandleStore, symbol: str = "XAUUSD"):
        self.store = store
        self.symbol = symbol.upper()
        self._cursor: Optional[datetime] = None
        self._original_xau_fetch = None
        self._original_ctrader_ohlcv = None
        self._installed = False

    # ── cursor ──────────────────────────────────────────────────────────────

    def set_cursor(self, ts: datetime) -> None:
        """Move the replay head to this timestamp."""
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        self._cursor = ts

    @property
    def cursor(self) -> Optional[datetime]:
        return self._cursor

    # ── patched fetch ───────────────────────────────────────────────────────

    def _patched_xau_fetch(self, tf: str, bars: int = 200) -> Optional[pd.DataFrame]:
        """Drop-in replacement for xauusd_provider.fetch()."""
        if self._cursor is None:
            return None
        return self.store.fetch(symbol=self.symbol, tf=tf, end=self._cursor, bars=bars)

    def _patched_ctrader_ohlcv(self, symbol: str, tf: str, bars: int = 200) -> Optional[pd.DataFrame]:
        """Drop-in replacement for ScalpingScanner._fetch_ctrader_ohlcv().

        Serves historical candles from CandleStore instead of live cTrader API.
        """
        if self._cursor is None:
            return None
        canonical = symbol.replace("/", "").replace("USDT", "USD").replace("BUSD", "USD").upper()
        return self.store.fetch(symbol=canonical, tf=tf, end=self._cursor, bars=bars)

    # ── install / uninstall ─────────────────────────────────────────────────

    def install(self) -> None:
        """Monkey-patch the appropriate provider(s) with replay version."""
        if self._installed:
            return

        is_xau = any(x in self.symbol for x in _XAU_SYMBOLS)
        is_crypto = any(x in self.symbol for x in _CRYPTO_SYMBOLS)

        if is_xau:
            from market.data_fetcher import xauusd_provider
            self._original_xau_fetch = xauusd_provider.fetch
            xauusd_provider.fetch = self._patched_xau_fetch

        if is_crypto:
            from scanners.scalping_scanner import ScalpingScanner
            self._original_ctrader_ohlcv = ScalpingScanner._fetch_ctrader_ohlcv
            ScalpingScanner._fetch_ctrader_ohlcv = staticmethod(self._patched_ctrader_ohlcv)

        self._installed = True
        logger.info("[ReplayEngine] Installed for %s", self.symbol)

    def uninstall(self) -> None:
        """Restore original provider fetch methods."""
        if not self._installed:
            return

        if self._original_xau_fetch is not None:
            from market.data_fetcher import xauusd_provider
            xauusd_provider.fetch = self._original_xau_fetch
            self._original_xau_fetch = None
        if self._original_ctrader_ohlcv is not None:
            from scanners.scalping_scanner import ScalpingScanner
            ScalpingScanner._fetch_ctrader_ohlcv = self._original_ctrader_ohlcv
            self._original_ctrader_ohlcv = None

        self._installed = False
        logger.info("[ReplayEngine] Uninstalled for %s", self.symbol)

    def __enter__(self):
        self.install()
        return self

    def __exit__(self, *exc):
        self.uninstall()
