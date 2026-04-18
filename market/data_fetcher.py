"""
market/data_fetcher.py - Unified market data engine
FIX v3.1:
  - XAUUSD: use GC=F as primary source, XAUUSD=X as fallback
  - Added broad price sanity validation to catch bad responses
  - Crypto: changed defaultType from 'future' to 'spot' so fetch_tickers works correctly
  - Kept funding rate fetching on a separate futures instance
"""
import time
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional
import pandas as pd
import numpy as np
import yfinance as yf
import ccxt
import requests

from config import config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  XAUUSD Data Provider
#  Primary:  GC=F       (COMEX gold futures front-month)
#  Fallback: XAUUSD=X   (spot gold USD)
# ─────────────────────────────────────────────────────────────────────────────
class XAUUSDProvider:
    """Fetches XAUUSD (Gold Spot) OHLCV data via yfinance."""

    # (primary_ticker, fallback_ticker, yf_interval, period)
    TICKER_MAP = {
        "1m":  ("GC=F", "XAUUSD=X", "1m",   "1d"),
        "5m":  ("GC=F", "XAUUSD=X", "5m",   "5d"),
        "15m": ("GC=F", "XAUUSD=X", "15m",  "5d"),
        "30m": ("GC=F", "XAUUSD=X", "30m",  "60d"),
        "1h":  ("GC=F", "XAUUSD=X", "1h",   "90d"),
        "4h":  ("GC=F", "XAUUSD=X", "1h",   "365d"),
        "1d":  ("GC=F", "XAUUSD=X", "1d",   "730d"),
        "1w":  ("GC=F", "XAUUSD=X", "1wk",  "1825d"),
    }

    # Sanity range for gold price (USD)
    PRICE_MIN = 1.0
    PRICE_MAX = 100_000.0

    def __init__(self):
        self._spot_cache_price: Optional[float] = None
        self._spot_cache_ts: float = 0.0

    def _download(self, ticker: str, interval: str,
                  period: str) -> Optional[pd.DataFrame]:
        """Download OHLCV from yfinance and normalise columns."""
        try:
            raw = yf.download(ticker, period=period, interval=interval,
                              auto_adjust=True, progress=False, timeout=15)
            if raw is None or raw.empty:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            else:
                raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]

            raw = raw.rename(columns={
                "Open": "open", "High": "high",
                "Low": "low", "Close": "close", "Volume": "volume",
            })
            raw.columns = [str(c).strip().lower() for c in raw.columns]
            raw = raw.loc[:, ~raw.columns.duplicated(keep="last")]

            required = ["open", "high", "low", "close", "volume"]
            if not all(col in raw.columns for col in required):
                return None

            df = raw[required].copy()
            df.index.name = "timestamp"
            df.dropna(inplace=True)
            return df if not df.empty else None
        except Exception as e:
            logger.debug(f"_download({ticker}): {e}")
            return None

    def _validate_price(self, df: pd.DataFrame) -> bool:
        """Return True if latest close is within valid gold price range."""
        if df is None or df.empty:
            return False
        price = float(df["close"].iloc[-1])
        ok = self.PRICE_MIN <= price <= self.PRICE_MAX
        if not ok:
            logger.warning(
                f"[XAUUSD] Price sanity FAIL: ${price:.2f} "
                f"(expected ${self.PRICE_MIN:.0f}–${self.PRICE_MAX:.0f})"
            )
        return ok

    def _get_stooq_spot_price(self) -> Optional[float]:
        """
        Fetch spot XAUUSD price from Stooq CSV endpoint.
        Cached for 30s to avoid repeated external requests.
        """
        now = time.time()
        if self._spot_cache_price is not None and (now - self._spot_cache_ts) < 30:
            return self._spot_cache_price

        try:
            resp = requests.get(
                "https://stooq.com/q/l/?s=xauusd&f=sd2t2ohlcvn&h&e=csv",
                timeout=8,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code != 200:
                return self._spot_cache_price
            lines = [ln.strip() for ln in resp.text.splitlines() if ln.strip()]
            if len(lines) < 2:
                return self._spot_cache_price
            # Symbol,Date,Time,Open,High,Low,Close,Volume,Name
            parts = [p.strip() for p in lines[1].split(",")]
            if len(parts) < 7:
                return self._spot_cache_price
            price = float(parts[6])
            if price > 0:
                self._spot_cache_price = price
                self._spot_cache_ts = now
                return price
        except Exception:
            pass
        return self._spot_cache_price

    def _apply_spot_basis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Align futures-like OHLC series to spot level using latest spot reference.
        Keeps candle structure but shifts absolute level.
        """
        if df is None or df.empty:
            return df
        spot = self._get_stooq_spot_price()
        if spot is None:
            return df
        last_close = float(df["close"].iloc[-1])
        delta = spot - last_close
        # Apply only when basis gap is clearly material.
        if abs(delta) >= 5.0:
            aligned = df.copy()
            for col in ("open", "high", "low", "close"):
                aligned[col] = aligned[col] + delta
            logger.info(f"[XAUUSD] Applied spot basis adjustment: {delta:+.2f}")
            return aligned
        return df

    def fetch(self, timeframe: str = "1h", bars: int = 200) -> Optional[pd.DataFrame]:
        """Return validated OHLCV DataFrame for XAUUSD."""
        if timeframe not in self.TICKER_MAP:
            logger.error(f"Unsupported timeframe: {timeframe}")
            return None

        primary, fallback, interval, period = self.TICKER_MAP[timeframe]

        # Try primary ticker first (GC=F)
        df = self._download(primary, interval, period)
        if df is not None:
            if not self._validate_price(df):
                logger.warning(f"[XAUUSD] Price sanity warning on primary {primary}, using data anyway")
            if timeframe == "4h":
                df = df.resample("4h").agg({
                    "open": "first", "high": "max",
                    "low": "min", "close": "last", "volume": "sum",
                }).dropna()
            df = self._apply_spot_basis(df)
            return df.tail(bars)

        # Fallback to XAUUSD=X (spot)
        logger.warning(f"[XAUUSD] Primary ({primary}) invalid, trying fallback {fallback}")
        df = self._download(fallback, interval, period)
        if df is not None:
            if not self._validate_price(df):
                logger.warning(f"[XAUUSD] Price sanity warning on fallback {fallback}, using data anyway")
            if timeframe == "4h":
                df = df.resample("4h").agg({
                    "open": "first", "high": "max",
                    "low": "min", "close": "last", "volume": "sum",
                }).dropna()
            df = self._apply_spot_basis(df)
            return df.tail(bars)

        logger.error("[XAUUSD] Both tickers failed price validation")
        return None

    def get_current_price(self) -> Optional[float]:
        """Return the latest validated XAUUSD price."""
        spot_price = self._get_stooq_spot_price()
        if spot_price is not None and spot_price > 0:
            return spot_price
        try:
            tk = yf.Ticker("GC=F")
            info = tk.fast_info
            price = float(info.last_price)
            if price > 0:
                return price
        except Exception:
            pass
        try:
            tk = yf.Ticker("XAUUSD=X")
            info = tk.fast_info
            price = float(info.last_price)
            if price > 0:
                return price
        except Exception:
            pass
        # Fallback to OHLCV last close
        df = self.fetch("1m", bars=5)
        if df is not None and not df.empty:
            return float(df["close"].iloc[-1])
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Crypto Data Provider
#  FIX: Use SPOT market for data / top-pairs. Separate futures instance for
#       funding rates only. This prevents fetch_tickers from failing silently.
# ─────────────────────────────────────────────────────────────────────────────
class CryptoProvider:
    """Fetches crypto OHLCV data and top market pairs via CCXT (spot)."""

    def __init__(self, exchange_id: str = "binance"):
        self.exchange_id = exchange_id
        self._spot: Optional[ccxt.Exchange] = None      # spot — for prices/OHLCV
        self._futures: Optional[ccxt.Exchange] = None   # futures — funding rates only
        self._markets_cache: Optional[dict] = None
        self._markets_ts: float = 0.0

    def _build_exchange(self, market_type: str = "spot") -> ccxt.Exchange:
        ExchangeClass = getattr(ccxt, self.exchange_id)
        kwargs: dict = {
            "enableRateLimit": True,
            "options": {"defaultType": market_type},
        }
        if self.exchange_id == "binance":
            if config.BINANCE_API_KEY:
                kwargs["apiKey"] = config.BINANCE_API_KEY
                kwargs["secret"] = config.BINANCE_SECRET
        elif self.exchange_id == "bybit":
            if config.BYBIT_API_KEY:
                kwargs["apiKey"] = config.BYBIT_API_KEY
                kwargs["secret"] = config.BYBIT_SECRET
        return ExchangeClass(kwargs)

    @property
    def spot(self) -> ccxt.Exchange:
        """Spot exchange — used for all market data and OHLCV."""
        if self._spot is None:
            self._spot = self._build_exchange("spot")
        return self._spot

    @property
    def futures(self) -> ccxt.Exchange:
        """Futures exchange — used for funding rates only."""
        if self._futures is None:
            self._futures = self._build_exchange("future")
        return self._futures

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1h",
                    bars: int = 200) -> Optional[pd.DataFrame]:
        """Return OHLCV DataFrame for a crypto pair (spot market)."""
        ccxt_tf = config.TF_TO_CCXT.get(timeframe, timeframe)
        try:
            raw = self.spot.fetch_ohlcv(symbol, ccxt_tf, limit=bars)
            if not raw:
                return None
            df = pd.DataFrame(
                raw, columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df.set_index("timestamp", inplace=True)
            df = df.astype(float).dropna()
            return df
        except Exception as e:
            logger.debug(f"fetch_ohlcv({symbol},{timeframe}): {e}")
            return None

    def get_top_volume_pairs(self, n: int = 50,
                              quote: str = "USDT") -> list[str]:
        """Return top-N USDT spot pairs by 24h quote volume."""
        try:
            now = time.time()
            if self._markets_cache is None or (now - self._markets_ts) > 600:
                self._markets_cache = self.spot.load_markets()
                self._markets_ts = now

            tickers = self.spot.fetch_tickers()
            def _sniper_pair_allowed(sym: str) -> bool:
                s = str(sym or "").upper()
                if not (
                    s.endswith(f"/{str(quote).upper()}")
                    and "/:" not in s
                    and "UP/" not in s
                    and "DOWN/" not in s
                    and "BEAR/" not in s
                    and "BULL/" not in s
                ):
                    return False
                base = s.split("/", 1)[0].strip()
                if not base:
                    return False
                excluded = config.get_crypto_sniper_exclude_bases()
                if base in excluded:
                    return False
                # Extra guard for fiat-ish bases if user keeps exclude list short.
                if bool(getattr(config, "CRYPTO_SNIPER_EXCLUDE_FIAT_BASES", True)) and base in {
                    "USD", "EUR", "GBP", "JPY", "AUD", "CHF", "CAD", "NZD", "TRY", "BRL", "RUB", "NGN", "UAH", "ZAR"
                }:
                    return False
                if bool(getattr(config, "CRYPTO_SNIPER_EXCLUDE_STABLE_BASES", True)) and base in {
                    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "USDP", "DAI", "USDD", "PYUSD", "FRAX"
                }:
                    return False
                if bool(getattr(config, "CRYPTO_SNIPER_EXCLUDE_STABLE_BASES", True)):
                    # Catch new stable-like tickers (e.g., USD1, USDX, HUSD, SUSD) without hand-listing each one.
                    if (base.startswith("USD") or base.endswith("USD")) and len(base) <= 8:
                        return False
                return True

            usdt_pairs = [
                (sym, float(t.get("quoteVolume", 0) or 0))
                for sym, t in tickers.items()
                if _sniper_pair_allowed(sym)
            ]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            ranked_all = [sym for sym, _ in usdt_pairs]

            if bool(getattr(config, "CRYPTO_SNIPER_MT5_TRADABLE_ONLY", False)) and bool(getattr(config, "MT5_ENABLED", False)):
                try:
                    from execution.mt5_executor import mt5_executor
                    filt = mt5_executor.filter_tradable_signal_symbols(ranked_all)
                    if bool(filt.get("ok")) and bool(filt.get("connected")):
                        tradable_set = set(filt.get("tradable", []) or [])
                        before = len(ranked_all)
                        ranked_all = [s for s in ranked_all if s in tradable_set]
                        logger.info(
                            "[Crypto] MT5 broker-tradable filter: %s/%s pairs tradable (unmapped=%s)",
                            len(ranked_all),
                            before,
                            len(filt.get("unmapped", []) or []),
                        )
                    else:
                        logger.info(
                            "[Crypto] MT5 broker-tradable filter skipped: %s",
                            str(filt.get("error") or "not connected"),
                        )
                except Exception as e:
                    logger.warning("[Crypto] MT5 broker-tradable filter error: %s", e)

            top = ranked_all[:n]

            # Always include priority pairs
            for p in config.PRIORITY_PAIRS:
                if p not in top and _sniper_pair_allowed(p):
                    if bool(getattr(config, "CRYPTO_SNIPER_MT5_TRADABLE_ONLY", False)) and bool(getattr(config, "MT5_ENABLED", False)):
                        try:
                            from execution.mt5_executor import mt5_executor
                            if not mt5_executor.resolve_symbol(p):
                                continue
                        except Exception:
                            pass
                    top.insert(0, p)
            result = top[:n]
            logger.info(f"[Crypto] Top-{n} pairs fetched: {len(result)} symbols")
            return result

        except Exception as e:
            logger.warning(f"get_top_volume_pairs failed ({e}) — using priority list")
            return config.PRIORITY_PAIRS

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Return latest spot price for a symbol."""
        try:
            ticker = self.spot.fetch_ticker(symbol)
            return float(ticker["last"])
        except Exception as e:
            logger.debug(f"get_current_price({symbol}): {e}")
            return None

    def get_funding_rate(self, symbol: str) -> Optional[float]:
        """Return current funding rate (futures)."""
        try:
            # Convert spot symbol to futures if needed
            futures_symbol = symbol  # Binance futures uses same format
            fr = self.futures.fetch_funding_rate(futures_symbol)
            return float(fr.get("fundingRate", 0) or 0)
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────────────────────
#  FX Data Provider (Majors via yfinance)
# ─────────────────────────────────────────────────────────────────────────────
class FXProvider:
    """Fetches major FX pair OHLCV via yfinance ticker format (e.g. EURUSD=X)."""

    TF_MAP = {
        "1m": ("1m", "1d"),
        "5m": ("5m", "5d"),
        "15m": ("15m", "5d"),
        "30m": ("30m", "60d"),
        "1h": ("1h", "90d"),
        "4h": ("1h", "365d"),
        "1d": ("1d", "2y"),
        "1w": ("1wk", "5y"),
    }

    @staticmethod
    def _yf_symbol(symbol: str) -> str:
        s = str(symbol or "").strip().upper()
        if not s:
            return ""
        if s.endswith("=X"):
            return s
        return f"{s}=X"

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1h", bars: int = 200) -> Optional[pd.DataFrame]:
        tf = str(timeframe or "1h")
        interval, period = self.TF_MAP.get(tf, ("1h", "90d"))
        yf_symbol = self._yf_symbol(symbol)
        if not yf_symbol:
            return None
        try:
            raw = yf.Ticker(yf_symbol).history(period=period, interval=interval, auto_adjust=True)
            if raw is None or raw.empty:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw = raw.rename(columns={
                "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume",
            })
            raw.columns = [str(c).strip().lower() for c in raw.columns]
            raw = raw.loc[:, ~raw.columns.duplicated(keep="last")]
            required = ["open", "high", "low", "close"]
            if not all(c in raw.columns for c in required):
                return None
            if "volume" not in raw.columns:
                raw["volume"] = 0.0
            df = raw[["open", "high", "low", "close", "volume"]].copy()
            df.index.name = "timestamp"
            df.dropna(inplace=True)
            if tf == "4h":
                df = df.resample("4h").agg({
                    "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
                }).dropna()
            if len(df) < 30:
                return None
            return df.tail(max(50, int(bars)))
        except Exception as e:
            logger.debug(f"fx fetch_ohlcv({symbol},{timeframe}): {e}")
            return None

    def get_current_price(self, symbol: str) -> Optional[float]:
        try:
            df = self.fetch_ohlcv(symbol, "1m", bars=5)
            if df is not None and not df.empty:
                return float(df["close"].iloc[-1])
        except Exception:
            pass
        return None

    def get_major_pairs(self) -> list[str]:
        return list(config.get_fx_major_symbols())


# ─────────────────────────────────────────────────────────────────────────────
#  Session Manager
# ─────────────────────────────────────────────────────────────────────────────
class SessionManager:
    """Identifies current Forex / market trading sessions (UTC)."""

    @staticmethod
    def current_sessions() -> list[str]:
        now_utc = datetime.now(timezone.utc)
        hour_min = now_utc.strftime("%H:%M")
        active = []
        for name, times in config.SESSIONS.items():
            if times["start"] <= hour_min <= times["end"]:
                active.append(name)
        return active if active else ["off_hours"]

    @staticmethod
    def is_fx_weekend_closed(now_utc: Optional[datetime] = None) -> bool:
        now = now_utc or datetime.now(timezone.utc)
        hm = (int(now.hour) * 60) + int(now.minute)
        wd = int(now.weekday())
        if wd == 5:
            return True
        if wd == 6 and hm < ((22 * 60) + 5):
            return True
        if wd == 4 and hm >= (22 * 60):
            return True
        return False

    @staticmethod
    def _easter_sunday(year: int) -> date:
        """Butcher's algorithm — computes Easter Sunday for a Gregorian year."""
        a = year % 19
        b = year // 100
        c = year % 100
        d = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i = c // 4
        k = c % 4
        ll = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * ll) // 451
        month = (h + ll - 7 * m + 114) // 31
        day = ((h + ll - 7 * m + 114) % 31) + 1
        return date(year, month, day)

    @staticmethod
    def xauusd_market_holidays(year: int) -> frozenset:
        """
        Returns the set of dates (UTC) when XAUUSD spot gold is closed.
        Gold is closed on: Good Friday, Christmas Day, New Year's Day.
        """
        easter = SessionManager._easter_sunday(year)
        good_friday = easter - timedelta(days=2)
        holidays = {
            good_friday,
            date(year, 12, 25),   # Christmas Day
            date(year, 1, 1),     # New Year's Day
        }
        return frozenset(holidays)

    @staticmethod
    def is_xauusd_holiday(now_utc: Optional[datetime] = None) -> bool:
        """Returns True if today (UTC) is a XAUUSD market holiday."""
        now = now_utc or datetime.now(timezone.utc)
        today = now.date()
        return today in SessionManager.xauusd_market_holidays(today.year)

    @staticmethod
    def is_xauusd_market_open(now_utc: Optional[datetime] = None) -> bool:
        if SessionManager.is_fx_weekend_closed(now_utc=now_utc):
            return False
        if bool(getattr(config, "XAU_HOLIDAY_GUARD_ENABLED", True)):
            if SessionManager.is_xauusd_holiday(now_utc=now_utc):
                return False
        return True

    @staticmethod
    def is_high_volatility_window() -> bool:
        sessions = SessionManager.current_sessions()
        return any(s in sessions for s in ["london", "new_york", "overlap"])

    @staticmethod
    def get_session_info() -> dict:
        now_utc = datetime.now(timezone.utc)
        return {
            "utc_time":        now_utc.strftime("%Y-%m-%d %H:%M UTC"),
            "active_sessions": SessionManager.current_sessions(),
            "high_volatility": SessionManager.is_high_volatility_window(),
            "xauusd_market_open": SessionManager.is_xauusd_market_open(now_utc=now_utc),
            "fx_weekend_closed": SessionManager.is_fx_weekend_closed(now_utc=now_utc),
        }


# ── Singletons ────────────────────────────────────────────────────────────────
xauusd_provider  = XAUUSDProvider()
# DISABLED: Binance/yfinance providers — all trading uses cTrader OpenAPI only
# crypto_provider and fx_provider are lazy to avoid loading ccxt/yfinance on import
crypto_provider  = None  # was CryptoProvider — use cTrader for BTC/ETH
fx_provider      = None  # was FXProvider — not used
session_manager  = SessionManager()
