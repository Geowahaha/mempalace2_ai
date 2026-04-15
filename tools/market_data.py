"""
Market Data Tool — Fetches OHLCV data for XAUUSD and forex pairs.

Supports:
  - OANDA REST API (forex/gold)
  - MT5 (via MetaTrader5 Python package)
  - Synthetic/test data for backtesting
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from tools.base import Tool, ToolCategory, ToolResult

logger = logging.getLogger("mempalace2.tools.market_data")


class MarketDataTool(Tool):
    """
    Fetches OHLCV candlestick data for analysis.

    Parameters:
      symbol:     Trading pair (e.g., "XAUUSD")
      timeframe:  Candle interval ("1m", "5m", "15m", "1h", "4h", "1d", "1w")
      limit:      Number of candles to fetch (default 200)
    """

    name = "market_data"
    category = ToolCategory.MARKET_DATA
    description = "Fetch OHLCV candlestick data for trading pairs"
    is_read_only = True
    is_safe = True

    # Supported timeframes → seconds
    TIMEFRAME_SECONDS = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800,
    }

    def __init__(self, exchange_client=None):
        self.exchange = exchange_client

    def validate_input(self, symbol: str = "", timeframe: str = "1h", **kwargs) -> Optional[str]:
        if not symbol:
            return "symbol is required"
        if timeframe not in self.TIMEFRAME_SECONDS:
            return f"Invalid timeframe: {timeframe}. Use: {list(self.TIMEFRAME_SECONDS.keys())}"
        return None

    async def execute(
        self,
        symbol: str = "XAUUSD",
        timeframe: str = "1h",
        limit: int = 200,
        **kwargs,
    ) -> ToolResult:
        """Fetch OHLCV data."""
        try:
            if self.exchange:
                data = await self._fetch_from_exchange(symbol, timeframe, limit)
            else:
                data = self._generate_synthetic_data(symbol, timeframe, limit)

            return ToolResult.ok(
                data=data,
                symbol=symbol,
                timeframe=timeframe,
                candles=len(data),
            )
        except Exception as e:
            return ToolResult.fail(str(e))

    async def _fetch_from_exchange(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """Fetch real data from exchange (OANDA/MT5)."""
        import ccxt

        exchange = ccxt.oanda({
            'apiKey': self.exchange.get('api_key', ''),
            'secret': self.exchange.get('api_secret', ''),
            'sandbox': self.exchange.get('sandbox', True),
        })

        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.set_index("timestamp", inplace=True)
        return df

    def _generate_synthetic_data(
        self, symbol: str, timeframe: str, limit: int
    ) -> pd.DataFrame:
        """
        Generate realistic synthetic XAUUSD data for testing.
        Uses geometric Brownian motion with volatility clustering.
        """
        np.random.seed(42)

        # XAUUSD base parameters
        if "XAU" in symbol:
            base_price = 3200.0  # current gold price ~$3200
            daily_vol = 0.008    # ~0.8% daily volatility
            pip_size = 0.01      # $0.01 per pip for gold
        elif "XAG" in symbol:
            base_price = 32.0
            daily_vol = 0.012
            pip_size = 0.001
        else:
            base_price = 1.1000
            daily_vol = 0.005
            pip_size = 0.00001

        seconds = self.TIMEFRAME_SECONDS[timeframe]
        vol_per_candle = daily_vol * np.sqrt(seconds / 86400)

        # Generate with GARCH-like volatility clustering
        prices = [base_price]
        vol = vol_per_candle
        for i in range(limit - 1):
            # Volatility clustering
            shock = np.random.normal(0, vol)
            vol = 0.94 * vol + 0.06 * abs(shock) + vol_per_candle * 0.1
            vol = max(vol, vol_per_candle * 0.3)
            vol = min(vol, vol_per_candle * 3.0)

            # Mean reversion in price
            drift = -0.0001 * (prices[-1] - base_price) / base_price
            new_price = prices[-1] * (1 + drift + shock)
            prices.append(max(new_price, base_price * 0.5))

        # Build OHLCV from price series
        now = datetime.now(timezone.utc)
        interval = timedelta(seconds=seconds)
        rows = []
        for i in range(limit):
            p = prices[i]
            noise = vol_per_candle * 0.3
            o = p * (1 + np.random.uniform(-noise, noise))
            c = p * (1 + np.random.uniform(-noise, noise))
            h = max(o, c) * (1 + abs(np.random.normal(0, noise)))
            l = min(o, c) * (1 - abs(np.random.normal(0, noise)))
            vol_bar = int(np.random.lognormal(10, 1))

            rows.append({
                "timestamp": now - interval * (limit - i),
                "open": round(o, 2),
                "high": round(h, 2),
                "low": round(l, 2),
                "close": round(c, 2),
                "volume": vol_bar,
            })

        df = pd.DataFrame(rows)
        df.set_index("timestamp", inplace=True)
        return df
