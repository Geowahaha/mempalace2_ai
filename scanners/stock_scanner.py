"""
scanners/stock_scanner.py - Professional Global Stock Scanner
Covers: US, UK, Germany, France, Japan, Hong Kong, China,
        Thailand (SET50), Singapore, India, Australia
Uses yfinance for all data - no API keys required for market data.
"""
import logging
import time
from datetime import datetime, timezone, time as dt_time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional
import threading
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from analysis.technical import TechnicalAnalysis
from analysis.smc import SMCAnalyzer
from analysis.signals import SignalGenerator, TradeSignal
from market.stock_universe import (
    MARKET_GROUPS, PRIORITY_STOCKS, MARKET_HOURS,
    get_all_stocks, get_active_markets_now
)
from market.data_fetcher import session_manager
from config import config

logger = logging.getLogger(__name__)
ta = TechnicalAnalysis()
smc_analyzer = SMCAnalyzer()
sig = SignalGenerator(min_confidence=config.STOCK_MIN_CONFIDENCE)

# yfinance interval map
TF_YF = {
    "1h":  ("1h",  "6mo"),
    "4h":  ("1h",  "1y"),    # resample to 4h
    "1d":  ("1d",  "2y"),
    "1wk": ("1wk", "5y"),
}

_YF_BAD_SYMBOL_LOCK = threading.Lock()
_YF_BAD_SYMBOL_UNTIL: dict[str, float] = {}
_YF_BAD_SYMBOL_REASON: dict[str, str] = {}
_YF_FAIL_COUNTS: dict[str, int] = {}


def _stock_alias_map() -> dict[str, str]:
    try:
        return config.get_stock_yf_symbol_alias_map()
    except Exception:
        return {}


def _stock_symbol_alias(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    return _stock_alias_map().get(sym, sym)


def _yf_symbol_blacklisted(symbol: str) -> bool:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return False
    now = time.time()
    ttl = max(300, int(getattr(config, "STOCK_YF_BAD_SYMBOL_CACHE_TTL_SEC", 86400)))
    with _YF_BAD_SYMBOL_LOCK:
        exp = float(_YF_BAD_SYMBOL_UNTIL.get(sym, 0.0) or 0.0)
        if exp <= 0:
            return False
        if now > exp:
            _YF_BAD_SYMBOL_UNTIL.pop(sym, None)
            _YF_BAD_SYMBOL_REASON.pop(sym, None)
            _YF_FAIL_COUNTS.pop(sym, None)
            return False
        return True


def _yf_record_fail(symbol: str, reason: str = "") -> None:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return
    now = time.time()
    threshold = max(1, int(getattr(config, "STOCK_YF_EMPTY_FAILS_TO_BLACKLIST", 2)))
    ttl = max(300, int(getattr(config, "STOCK_YF_BAD_SYMBOL_CACHE_TTL_SEC", 86400)))
    with _YF_BAD_SYMBOL_LOCK:
        n = int(_YF_FAIL_COUNTS.get(sym, 0) or 0) + 1
        _YF_FAIL_COUNTS[sym] = n
        hard = any(k in str(reason or "").lower() for k in ("delisted", "no data found", "404", "not found"))
        if hard or n >= threshold:
            was_blacklisted = sym in _YF_BAD_SYMBOL_UNTIL
            _YF_BAD_SYMBOL_UNTIL[sym] = now + ttl
            _YF_BAD_SYMBOL_REASON[sym] = str(reason or "empty history")
            if not was_blacklisted:
                logger.info("[STOCKS] Yahoo blacklist cache added: %s (ttl=%ss, reason=%s)", sym, ttl, _YF_BAD_SYMBOL_REASON[sym])


def _yf_record_success(symbol: str) -> None:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return
    with _YF_BAD_SYMBOL_LOCK:
        _YF_FAIL_COUNTS.pop(sym, None)
        _YF_BAD_SYMBOL_UNTIL.pop(sym, None)
        _YF_BAD_SYMBOL_REASON.pop(sym, None)


@dataclass
class StockOpportunity:
    signal: TradeSignal
    market: str               # US | UK | DE | TH | JP | HK | etc.
    sector: str = ""
    market_cap: str = ""      # Large/Mid/Small
    setup_type: str = ""      # direction-aware label, e.g. BULLISH_OB_BOUNCE
    base_setup_type: str = "" # base family, e.g. OB_BOUNCE
    vol_vs_avg: float = 1.0   # volume vs 20-day average
    dollar_volume: float = 0.0
    setup_win_rate: float = 0.0
    quality_score: int = 0
    quality_tag: str = "LOW"
    pe_ratio: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None

    @property
    def composite_score(self) -> float:
        base = self.signal.confidence
        setup_bonus = {
            "BB_SQUEEZE": 12, "CHOCH": 14, "OB_BOUNCE": 10,
            "FVG_FILL": 8, "DIVERGENCE": 9, "TREND_CONT": 5,
        }.get(self.setup_type, 0)
        vol_bonus = min(8.0, (self.vol_vs_avg - 1) * 4) if self.vol_vs_avg > 1 else 0
        return base + setup_bonus + vol_bonus

    @property
    def us_open_rank_score(self) -> float:
        # Emphasize liquidity and expected setup win-rate for US open day-trading.
        liquidity = min(100.0, (self.dollar_volume / 80_000_000.0) * 100.0)
        win_rate = self.setup_win_rate * 100.0
        return (0.5 * liquidity) + (0.3 * win_rate) + (0.2 * self.signal.confidence)


SETUP_WINRATE_MAP = {
    "CHOCH": 0.62,
    "OB_BOUNCE": 0.59,
    "FVG_FILL": 0.56,
    "BB_SQUEEZE": 0.55,
    "DIVERGENCE": 0.54,
    "TREND_CONT": 0.53,
}


def detect_market(symbol: str) -> str:
    """Detect market from ticker suffix."""
    if symbol.endswith(".L"):   return "UK"
    if symbol.endswith(".DE"):  return "DE"
    if symbol.endswith(".PA"):  return "FR"
    if symbol.endswith(".T"):   return "JP"
    if symbol.endswith(".HK"):  return "HK"
    if symbol.endswith(".BK"):  return "TH"
    if symbol.endswith(".SI"):  return "SG"
    if symbol.endswith(".NS"):  return "IN"
    if symbol.endswith(".BO"):  return "IN"
    if symbol.endswith(".AX"):  return "AU"
    if symbol.startswith("^"):  return "INDEX"
    return "US"


def fetch_stock_ohlcv(symbol: str, timeframe: str = "1h",
                      bars: int = 200) -> Optional[pd.DataFrame]:
    """Fetch OHLCV data for any global stock via yfinance."""
    req_symbol = str(symbol or "").strip().upper()
    yf_symbol = _stock_symbol_alias(req_symbol)
    if _yf_symbol_blacklisted(req_symbol):
        return None
    if yf_symbol != req_symbol and _yf_symbol_blacklisted(yf_symbol):
        return None

    yf_interval, period = TF_YF.get(timeframe, ("1h", "6mo"))
    try:
        # Use per-symbol Ticker.history for better thread safety under parallel scans.
        raw = yf.Ticker(yf_symbol).history(
            period=period,
            interval=yf_interval,
            auto_adjust=True,
        )
        if raw is None or raw.empty:
            _yf_record_fail(req_symbol, reason="empty history")
            if yf_symbol != req_symbol:
                _yf_record_fail(yf_symbol, reason=f"alias empty for {req_symbol}")
            return None

        # Flatten MultiIndex if present
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        else:
            raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]

        raw = raw.rename(columns={
            "Open": "open", "High": "high",
            "Low": "low", "Close": "close", "Volume": "volume",
        })
        raw.columns = [str(c).strip().lower() for c in raw.columns]

        # yfinance can occasionally emit duplicate OHLCV columns under heavy concurrency.
        raw = raw.loc[:, ~raw.columns.duplicated(keep="last")]

        required = ["open", "high", "low", "close", "volume"]
        if not all(col in raw.columns for col in required):
            _yf_record_fail(req_symbol, reason="missing ohlcv columns")
            if yf_symbol != req_symbol:
                _yf_record_fail(yf_symbol, reason=f"alias missing ohlcv for {req_symbol}")
            return None

        df = raw[required].copy()
        df.index.name = "timestamp"
        df.dropna(inplace=True)

        # Resample to 4h if needed
        if timeframe == "4h":
            df = df.resample("4h").agg({
                "open": "first", "high": "max",
                "low": "min", "close": "last", "volume": "sum",
            }).dropna()

        if len(df) < 30:
            _yf_record_fail(req_symbol, reason="insufficient bars")
            if yf_symbol != req_symbol:
                _yf_record_fail(yf_symbol, reason=f"alias insufficient bars for {req_symbol}")
            return None

        _yf_record_success(req_symbol)
        if yf_symbol != req_symbol:
            _yf_record_success(yf_symbol)
        return df.tail(bars)

    except Exception as e:
        _yf_record_fail(req_symbol, reason=str(e))
        if yf_symbol != req_symbol:
            _yf_record_fail(yf_symbol, reason=f"alias exception: {e}")
        logger.debug(f"fetch_stock_ohlcv({req_symbol}): {e}")
        return None


def get_stock_info(symbol: str) -> dict:
    """Get fundamental info for a stock."""
    req_symbol = str(symbol or "").strip().upper()
    yf_symbol = _stock_symbol_alias(req_symbol)
    if _yf_symbol_blacklisted(req_symbol):
        return {}
    try:
        tk = yf.Ticker(yf_symbol)
        info = tk.info
        return {
            "sector":       info.get("sector", ""),
            "industry":     info.get("industry", ""),
            "market_cap":   info.get("marketCap", 0),
            "pe_ratio":     info.get("trailingPE"),
            "forward_pe":   info.get("forwardPE"),
            "peg_ratio":    info.get("pegRatio"),
            "price_to_book": info.get("priceToBook"),
            "price_to_sales": info.get("priceToSalesTrailing12Months"),
            "enterprise_to_revenue": info.get("enterpriseToRevenue"),
            "enterprise_to_ebitda": info.get("enterpriseToEbitda"),
            "return_on_equity": info.get("returnOnEquity"),
            "return_on_assets": info.get("returnOnAssets"),
            "gross_margin": info.get("grossMargins"),
            "operating_margin": info.get("operatingMargins"),
            "profit_margin": info.get("profitMargins"),
            "ebitda_margin": info.get("ebitdaMargins"),
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            "debt_to_equity": info.get("debtToEquity"),
            "current_ratio": info.get("currentRatio"),
            "quick_ratio": info.get("quickRatio"),
            "free_cashflow": info.get("freeCashflow"),
            "operating_cashflow": info.get("operatingCashflow"),
            "total_revenue": info.get("totalRevenue"),
            "book_value": info.get("bookValue"),
            "shares_outstanding": info.get("sharesOutstanding"),
            "current_price": info.get("currentPrice"),
            "average_volume": info.get("averageVolume"),
            "beta": info.get("beta"),
            "dividend_yield": info.get("dividendYield"),
            "payout_ratio": info.get("payoutRatio"),
            "52w_high":     info.get("fiftyTwoWeekHigh"),
            "52w_low":      info.get("fiftyTwoWeekLow"),
            "short_name":   info.get("shortName", symbol),
        }
    except Exception:
        _yf_record_fail(req_symbol, reason="info fetch failed")
        return {}


def detect_setup(df: pd.DataFrame) -> str:
    """Identify the primary chart setup."""
    try:
        df_ta = ta.add_all(df.copy())
        last = df_ta.iloc[-1]

        bb_width = float(last.get("bb_width", 1))
        vol_ratio = float(last.get("vol_ratio", 1))
        hist_bw = float(df_ta["bb_width"].quantile(0.2)) if "bb_width" in df_ta else 1
        if bb_width < hist_bw and vol_ratio > 1.5:
            return "BB_SQUEEZE"

        ctx = smc_analyzer.analyze(df_ta)
        if ctx.recent_bos and ctx.recent_bos.level_type == "ChoCH":
            return "CHOCH"
        if ctx.nearest_ob and not ctx.nearest_ob.tested:
            return "OB_BOUNCE"
        if ctx.nearest_fvg and not ctx.nearest_fvg.filled:
            return "FVG_FILL"

        div = ta.detect_rsi_divergence(df_ta)
        if div in ("bullish_div", "bearish_div"):
            return "DIVERGENCE"

        return "TREND_CONT"
    except Exception:
        return "UNKNOWN"


class GlobalStockScanner:
    """
    Scans global stock markets and returns ranked trading opportunities.
    Respects market hours — only scans open markets.
    """

    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.scan_count = 0
        self.total_signals = 0
        self._info_cache: dict[str, tuple[float, dict]] = {}
        self._info_cache_lock = threading.Lock()
        self._last_scan_diag: dict[str, dict] = {}
        self._last_us_open_diag: dict = {}
        self._mt5_tradable_only_override: Optional[bool] = None

    def set_mt5_tradable_only(self, enabled: Optional[bool]) -> bool:
        """Runtime toggle for MT5 broker-match stock filtering (None resets to config)."""
        if enabled is None:
            self._mt5_tradable_only_override = None
            return bool(getattr(config, "STOCK_SCANNER_MT5_TRADABLE_ONLY", False))
        self._mt5_tradable_only_override = bool(enabled)
        return bool(self._mt5_tradable_only_override)

    def get_mt5_tradable_only(self) -> dict:
        runtime = self._mt5_tradable_only_override
        env_default = bool(getattr(config, "STOCK_SCANNER_MT5_TRADABLE_ONLY", False))
        effective = env_default if runtime is None else bool(runtime)
        return {
            "effective": bool(effective),
            "runtime_override": (None if runtime is None else bool(runtime)),
            "env_default": bool(env_default),
        }

    @staticmethod
    def _directional_setup_label(setup: str, direction: str) -> str:
        side = "BULLISH" if direction == "long" else "BEARISH"
        if setup in {"OB_BOUNCE", "FVG_FILL", "CHOCH", "DIVERGENCE", "TREND_CONT", "BB_SQUEEZE"}:
            return f"{side}_{setup}"
        return setup

    @staticmethod
    def _stock_quality_score(signal: TradeSignal, vol_ratio: float) -> tuple[int, str]:
        """
        Stock quality filter based on liquidity + momentum + score edge.
        Score range: 0..3, pass threshold is >=2.
        """
        score = 0
        if vol_ratio >= config.STOCK_MIN_VOL_RATIO:
            score += 1

        edge = float(signal.raw_scores.get("edge", 0) if signal.raw_scores else 0)
        if edge >= config.STOCK_MIN_EDGE:
            score += 1

        long_mom = signal.direction == "long" and signal.rsi >= config.STOCK_MIN_MOMENTUM_RSI and signal.trend == "bullish"
        short_mom = signal.direction == "short" and signal.rsi <= (100.0 - config.STOCK_MIN_MOMENTUM_RSI) and signal.trend == "bearish"
        if long_mom or short_mom:
            score += 1

        if score >= 3 and vol_ratio >= 1.0:
            tag = "HIGH"
        elif score == 2:
            tag = "MEDIUM"
        else:
            tag = "LOW"
        return score, tag

    def _get_stock_info_cached(self, symbol: str) -> dict:
        now_ts = time.time()
        ttl = max(60, int(getattr(config, "STOCK_INFO_CACHE_TTL_SEC", 21600)))
        with self._info_cache_lock:
            cached = self._info_cache.get(symbol)
            if cached:
                ts, payload = cached
                if (now_ts - ts) <= ttl:
                    return dict(payload)
        payload = get_stock_info(symbol)
        with self._info_cache_lock:
            self._info_cache[symbol] = (now_ts, dict(payload))
        return payload

    def _prefilter_mt5_tradable_symbols(self, symbols: list[str], label: str = "") -> tuple[list[str], dict]:
        """
        Optional broker-match prefilter for stock scanners.
        Keeps only symbols resolvable at the connected MT5 broker when enabled.
        """
        diag = {
            "enabled": bool(self.get_mt5_tradable_only().get("effective")),
            "applied": False,
            "input": len(symbols or []),
            "kept": len(symbols or []),
            "removed": 0,
            "unmapped": 0,
            "error": "",
        }
        if not diag["enabled"] or not bool(getattr(config, "MT5_ENABLED", False)):
            return list(symbols or []), diag
        if not symbols:
            diag["applied"] = True
            return [], diag
        try:
            from execution.mt5_executor import mt5_executor
            filt = mt5_executor.filter_tradable_signal_symbols(list(symbols or []))
            if not bool(filt.get("ok")) or not bool(filt.get("connected")):
                diag["error"] = str(filt.get("error") or "mt5 filter unavailable")
                logger.info("[STOCKS:%s] MT5 broker-match filter skipped: %s", (label or "-"), diag["error"])
                return list(symbols or []), diag
            tradable_upper = {str(s).upper() for s in (filt.get("tradable") or [])}
            filtered = [s for s in list(symbols or []) if str(s).upper() in tradable_upper]
            diag["applied"] = True
            diag["kept"] = len(filtered)
            diag["removed"] = max(0, diag["input"] - diag["kept"])
            diag["unmapped"] = len(filt.get("unmapped") or [])
            logger.info(
                "[STOCKS:%s] MT5 broker-match filter: %s/%s tradable (unmapped=%s)",
                (label or "-"),
                diag["kept"],
                diag["input"],
                diag["unmapped"],
            )
            return filtered, diag
        except Exception as e:
            diag["error"] = str(e)
            logger.warning("[STOCKS:%s] MT5 broker-match filter error: %s", (label or "-"), e)
            return list(symbols or []), diag

    @staticmethod
    def _label_prefers_mt5_filter(label: str) -> bool:
        """
        Apply broker-match filter where tradability matters most (US / execution-facing flows).
        Non-US regional scans (e.g. TH_SET50) should stay usable for analysis even if broker lacks coverage.
        """
        l = str(label or "").upper()
        if l in {"US", "OPEN_MARKETS", "PRIORITY"}:
            return True
        if "US" in l:
            return True
        return False

    @staticmethod
    def _clip01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        try:
            if value is None:
                return None
            v = float(value)
            if pd.isna(v):
                return None
            return float(v)
        except Exception:
            return None

    def _range_position_component(self, opp: StockOpportunity) -> tuple[float, Optional[float]]:
        if (
            opp.week_52_high is None
            or opp.week_52_low is None
            or float(opp.week_52_high) <= float(opp.week_52_low)
            or float(opp.signal.entry) <= 0
        ):
            return 0.50, None
        high = float(opp.week_52_high)
        low = float(opp.week_52_low)
        pos = self._clip01((float(opp.signal.entry) - low) / (high - low))
        return (1.0 - pos), pos

    @staticmethod
    def _market_cap_bucket(market_cap: Optional[float]) -> str:
        mc = 0.0 if market_cap is None else float(market_cap)
        if mc <= 0:
            return "unknown"
        if mc < 2_000_000_000:
            return "small"
        if mc < 20_000_000_000:
            return "mid"
        if mc < 100_000_000_000:
            return "large"
        return "mega"

    def _vi_profile_scores(self, opp: StockOpportunity, info: Optional[dict] = None) -> dict:
        """
        Buffett-inspired quality/value + turnaround/re-rating scoring.
        Bounded heuristic scoring (not a replacement for full fundamental due diligence).
        """
        info = dict(info or {})
        pe = self._safe_float(info.get("pe_ratio") if info else opp.pe_ratio)
        forward_pe = self._safe_float(info.get("forward_pe"))
        pb = self._safe_float(info.get("price_to_book"))
        peg = self._safe_float(info.get("peg_ratio"))
        ev_ebitda = self._safe_float(info.get("enterprise_to_ebitda"))
        roe = self._safe_float(info.get("return_on_equity"))
        roa = self._safe_float(info.get("return_on_assets"))
        gross_margin = self._safe_float(info.get("gross_margin"))
        operating_margin = self._safe_float(info.get("operating_margin"))
        profit_margin = self._safe_float(info.get("profit_margin"))
        revenue_growth = self._safe_float(info.get("revenue_growth"))
        earnings_growth = self._safe_float(info.get("earnings_growth"))
        debt_to_equity = self._safe_float(info.get("debt_to_equity"))
        current_ratio = self._safe_float(info.get("current_ratio"))
        quick_ratio = self._safe_float(info.get("quick_ratio"))
        free_cashflow = self._safe_float(info.get("free_cashflow"))
        operating_cashflow = self._safe_float(info.get("operating_cashflow"))
        total_revenue = self._safe_float(info.get("total_revenue"))
        market_cap_val = self._safe_float(info.get("market_cap"))
        shares_outstanding = self._safe_float(info.get("shares_outstanding"))
        current_price = self._safe_float(info.get("current_price"))

        range_component, range_pos = self._range_position_component(opp)

        # Legacy/base VI components (kept for continuity)
        if pe is None or pe <= 0:
            pe_component = 0.45
        elif pe <= 12:
            pe_component = 1.00
        elif pe <= 20:
            pe_component = 0.85
        elif pe <= 30:
            pe_component = 0.60
        elif pe <= 40:
            pe_component = 0.30
        else:
            pe_component = 0.10

        value_score = (0.70 * pe_component) + (0.30 * range_component)

        conf_component = self._clip01((float(opp.signal.confidence) - 60.0) / 30.0)
        vol_component = self._clip01(float(opp.vol_vs_avg) / 1.5)
        wr_component = self._clip01(float(opp.setup_win_rate) / 0.65)
        trend_component = 1.0 if str(opp.signal.trend).lower() == "bullish" else 0.35
        if opp.signal.direction == "long":
            rsi = float(opp.signal.rsi)
            if 52 <= rsi <= 68:
                rsi_component = 1.0
            elif 45 <= rsi <= 75:
                rsi_component = 0.70
            else:
                rsi_component = 0.35
        else:
            rsi_component = 0.20
            trend_component = min(trend_component, 0.25)

        trend_score = (
            (0.30 * conf_component)
            + (0.25 * vol_component)
            + (0.20 * wr_component)
            + (0.15 * rsi_component)
            + (0.10 * trend_component)
        )
        liquidity_score = self._clip01(float(opp.dollar_volume) / 50_000_000.0)
        total_score = (0.45 * value_score) + (0.45 * trend_score) + (0.10 * liquidity_score)
        pe_cap = float(getattr(config, "VI_MAX_PE_RATIO", 35))
        if pe is not None and pe > pe_cap:
            total_score *= 0.82

        # Buffett-inspired "compounder" score (quality + prudence + fair price + trend confirmation)
        quality_parts: list[float] = []
        valuation_parts: list[float] = []
        balance_parts: list[float] = []
        cashflow_parts: list[float] = []
        growth_quality_parts: list[float] = []

        if roe is not None:
            quality_parts.append(self._clip01((roe - 0.10) / 0.20))
        if roa is not None:
            quality_parts.append(self._clip01((roa - 0.03) / 0.08))
        if gross_margin is not None:
            quality_parts.append(self._clip01((gross_margin - 0.25) / 0.35))
        if operating_margin is not None:
            quality_parts.append(self._clip01((operating_margin - 0.08) / 0.22))
        if profit_margin is not None:
            quality_parts.append(self._clip01((profit_margin - 0.05) / 0.20))

        if pe is not None and pe > 0:
            valuation_parts.append(self._clip01((28.0 - pe) / 20.0))
        if forward_pe is not None and forward_pe > 0:
            valuation_parts.append(self._clip01((25.0 - forward_pe) / 18.0))
        if pb is not None and pb > 0:
            valuation_parts.append(self._clip01((6.0 - pb) / 4.5))
        if peg is not None and peg > 0:
            valuation_parts.append(self._clip01((2.0 - peg) / 1.5))
        if ev_ebitda is not None and ev_ebitda > 0:
            valuation_parts.append(self._clip01((16.0 - ev_ebitda) / 10.0))
        valuation_parts.append(range_component)

        if debt_to_equity is not None:
            balance_parts.append(self._clip01((150.0 - debt_to_equity) / 150.0))
        if current_ratio is not None:
            balance_parts.append(self._clip01((current_ratio - 1.0) / 1.5))
        if quick_ratio is not None:
            balance_parts.append(self._clip01((quick_ratio - 0.8) / 1.2))

        if free_cashflow is not None:
            cashflow_parts.append(1.0 if free_cashflow > 0 else 0.0)
        if operating_cashflow is not None:
            cashflow_parts.append(1.0 if operating_cashflow > 0 else 0.0)
        if free_cashflow is not None and total_revenue and total_revenue > 0:
            cashflow_parts.append(self._clip01((free_cashflow / total_revenue + 0.02) / 0.12))
        if operating_cashflow is not None and total_revenue and total_revenue > 0:
            cashflow_parts.append(self._clip01((operating_cashflow / total_revenue) / 0.20))

        if revenue_growth is not None:
            growth_quality_parts.append(self._clip01((revenue_growth + 0.02) / 0.18))
        if earnings_growth is not None:
            growth_quality_parts.append(self._clip01((earnings_growth + 0.05) / 0.25))

        quality_component = (sum(quality_parts) / len(quality_parts)) if quality_parts else 0.45
        valuation_component = (sum(valuation_parts) / len(valuation_parts)) if valuation_parts else value_score
        balance_component = (sum(balance_parts) / len(balance_parts)) if balance_parts else 0.50
        cashflow_component = (sum(cashflow_parts) / len(cashflow_parts)) if cashflow_parts else 0.45
        growth_quality_component = (sum(growth_quality_parts) / len(growth_quality_parts)) if growth_quality_parts else 0.50

        # Simple intrinsic value estimate (owner earnings proxy / FCF yield / margin-of-safety band)
        owner_earnings = None
        if free_cashflow is not None and operating_cashflow is not None:
            owner_earnings = min(free_cashflow, operating_cashflow * 0.9)
        elif free_cashflow is not None:
            owner_earnings = free_cashflow
        elif operating_cashflow is not None:
            owner_earnings = operating_cashflow * 0.8

        fcf_yield = None
        if free_cashflow is not None and market_cap_val and market_cap_val > 0:
            fcf_yield = free_cashflow / market_cap_val
        owner_earnings_yield = None
        if owner_earnings is not None and market_cap_val and market_cap_val > 0:
            owner_earnings_yield = owner_earnings / market_cap_val

        owner_earnings_per_share = None
        if owner_earnings is not None and shares_outstanding and shares_outstanding > 0:
            owner_earnings_per_share = owner_earnings / shares_outstanding

        intrinsic_multiple = 12.0
        intrinsic_multiple += 4.0 * self._clip01(growth_quality_component)
        intrinsic_multiple += 4.0 * self._clip01(quality_component)
        intrinsic_multiple += 2.0 * self._clip01(balance_component)
        intrinsic_multiple -= 3.0 * (1.0 - self._clip01(valuation_component))
        intrinsic_multiple = max(8.0, min(24.0, intrinsic_multiple))

        try:
            entry_px = float(opp.signal.entry)
        except Exception:
            entry_px = 0.0
        price_for_mos = self._safe_float(entry_px if entry_px > 0 else current_price)
        intrinsic_value_est = None
        mos_pct = None
        if owner_earnings_per_share is not None and owner_earnings_per_share > 0:
            intrinsic_value_est = owner_earnings_per_share * intrinsic_multiple
            if price_for_mos and price_for_mos > 0:
                mos_pct = (intrinsic_value_est / price_for_mos) - 1.0
        if mos_pct is None and fcf_yield is not None:
            mos_pct = max(-0.50, min(1.00, (fcf_yield - 0.05) / 0.05))

        if mos_pct is None:
            mos_band = "UNKNOWN"
            mos_component = 0.50
        elif mos_pct >= 0.35:
            mos_band = "DEEP_VALUE"
            mos_component = 1.00
        elif mos_pct >= 0.15:
            mos_band = "VALUE"
            mos_component = 0.82
        elif mos_pct >= 0.00:
            mos_band = "FAIR"
            mos_component = 0.60
        elif mos_pct >= -0.15:
            mos_band = "RICH"
            mos_component = 0.35
        else:
            mos_band = "EXPENSIVE"
            mos_component = 0.10

        compounder_score = (
            0.23 * quality_component
            + 0.17 * cashflow_component
            + 0.15 * balance_component
            + 0.14 * valuation_component
            + 0.12 * mos_component
            + 0.09 * self._clip01(((owner_earnings_yield or 0.05) - 0.02) / 0.08)
            + 0.10 * trend_score
            + 0.05 * liquidity_score
        )
        if profit_margin is not None and profit_margin < 0:
            compounder_score *= 0.72
        if debt_to_equity is not None and debt_to_equity > 220:
            compounder_score *= 0.78
        if str(opp.signal.direction).lower() != "long":
            compounder_score *= 0.60

        range_low_bias = 0.50 if range_pos is None else self._clip01((0.65 - range_pos) / 0.65)
        rerating_val = 0.50
        if pe is not None and pe > 0:
            rerating_val = self._clip01((22.0 - pe) / 14.0)
        elif pe is None or pe <= 0:
            rerating_val = 0.62

        growth_inflect_parts: list[float] = []
        if revenue_growth is not None:
            growth_inflect_parts.append(self._clip01((revenue_growth + 0.03) / 0.22))
        if earnings_growth is not None:
            growth_inflect_parts.append(self._clip01((earnings_growth + 0.10) / 0.45))
        if operating_margin is not None:
            growth_inflect_parts.append(self._clip01((operating_margin + 0.02) / 0.15))
        growth_inflect = (sum(growth_inflect_parts) / len(growth_inflect_parts)) if growth_inflect_parts else 0.45

        technical_breakout = (
            0.35 * conf_component
            + 0.25 * vol_component
            + 0.20 * wr_component
            + 0.10 * trend_component
            + 0.10 * self._clip01((float(opp.signal.rsi) - 48.0) / 24.0)
        )
        if str(opp.base_setup_type).upper() in {"CHOCH", "OB_BOUNCE", "BB_SQUEEZE"}:
            technical_breakout = min(1.0, technical_breakout + 0.08)

        market_cap_bucket = self._market_cap_bucket(market_cap_val)
        asymmetry_size = {
            "small": 1.00,
            "mid": 0.88,
            "large": 0.60,
            "mega": 0.42,
            "unknown": 0.50,
        }.get(market_cap_bucket, 0.50)

        turnaround_score = (
            0.26 * technical_breakout
            + 0.22 * range_low_bias
            + 0.18 * growth_inflect
            + 0.12 * rerating_val
            + 0.12 * asymmetry_size
            + 0.10 * liquidity_score
        )
        if debt_to_equity is not None and debt_to_equity > 350:
            turnaround_score *= 0.80
        if (free_cashflow is not None and free_cashflow < 0) and (operating_cashflow is not None and operating_cashflow < 0):
            turnaround_score *= 0.85
        if str(opp.signal.direction).lower() != "long":
            turnaround_score *= 0.55

        compounder_score = self._clip01(compounder_score)
        turnaround_score = self._clip01(turnaround_score)
        score_gap = compounder_score - turnaround_score
        buffett_tie_bias = (
            score_gap >= -0.015
            and quality_component >= 0.70
            and cashflow_component >= 0.65
            and balance_component >= 0.55
        )
        if turnaround_score >= (compounder_score + 0.08):
            primary_profile = "TURNAROUND"
            primary_score = turnaround_score
        elif compounder_score >= (turnaround_score + 0.05) or buffett_tie_bias:
            primary_profile = "BUFFETT"
            primary_score = compounder_score
        else:
            primary_profile = "BLEND"
            primary_score = max(compounder_score, turnaround_score)

        detailed_reasons: list[str] = []
        if primary_profile == "BUFFETT":
            detailed_reasons.append("Buffett-inspired: quality business + reasonable valuation + trend confirmation")
            if roe is not None or operating_margin is not None or profit_margin is not None:
                qm = []
                if roe is not None:
                    qm.append(f"ROE {roe*100:.1f}%")
                if operating_margin is not None:
                    qm.append(f"OpMargin {operating_margin*100:.1f}%")
                if profit_margin is not None:
                    qm.append(f"ProfitMargin {profit_margin*100:.1f}%")
                if qm:
                    detailed_reasons.append("Quality metrics: " + ", ".join(qm))
            if debt_to_equity is not None or current_ratio is not None:
                bm = []
                if debt_to_equity is not None:
                    bm.append(f"D/E {debt_to_equity:.1f}")
                if current_ratio is not None:
                    bm.append(f"CurrentRatio {current_ratio:.2f}")
                if bm:
                    detailed_reasons.append("Balance-sheet discipline: " + ", ".join(bm))
            vm = []
            if pe is not None:
                vm.append(f"P/E {pe:.1f}")
            if pb is not None:
                vm.append(f"P/B {pb:.2f}")
            if ev_ebitda is not None:
                vm.append(f"EV/EBITDA {ev_ebitda:.1f}")
            if vm:
                detailed_reasons.append("Valuation check: " + ", ".join(vm))
            iv = []
            if owner_earnings_yield is not None:
                iv.append(f"OwnerYield {owner_earnings_yield*100:.1f}%")
            elif fcf_yield is not None:
                iv.append(f"FCFYield {fcf_yield*100:.1f}%")
            if intrinsic_value_est is not None and price_for_mos is not None:
                iv.append(f"Intrinsic~{intrinsic_value_est:.2f} vs Price~{price_for_mos:.2f}")
            if mos_pct is not None:
                iv.append(f"MOS {mos_pct*100:.1f}% ({mos_band})")
            if iv:
                detailed_reasons.append("Intrinsic estimate (simple): " + ", ".join(iv))
        elif primary_profile == "TURNAROUND":
            detailed_reasons.append("Turnaround / re-rating: improving setup with asymmetry potential")
            if range_pos is not None:
                detailed_reasons.append(f"52w range position {range_pos*100:.1f}% (lower is more asymmetric)")
            gm = []
            if revenue_growth is not None:
                gm.append(f"RevGrowth {revenue_growth*100:.1f}%")
            if earnings_growth is not None:
                gm.append(f"EarningsGrowth {earnings_growth*100:.1f}%")
            if operating_margin is not None:
                gm.append(f"OpMargin {operating_margin*100:.1f}%")
            if gm:
                detailed_reasons.append("Improvement proxies: " + ", ".join(gm))
            detailed_reasons.append(
                f"Re-rating tape: {opp.base_setup_type} | vol {opp.vol_vs_avg:.2f}x | conf {opp.signal.confidence:.1f}% | WR {opp.setup_win_rate*100:.1f}%"
            )
            detailed_reasons.append(f"Asymmetry bucket: {market_cap_bucket} cap")
            if mos_pct is not None:
                detailed_reasons.append(f"Value support / MOS band: {mos_band} ({mos_pct*100:.1f}%)")
        else:
            detailed_reasons.append("Blend profile: quality and turnaround signals are both present")
            if range_pos is not None:
                detailed_reasons.append(f"52w range position {range_pos*100:.1f}% with bullish trend confirmation")
            detailed_reasons.append(
                f"Setup {opp.base_setup_type} | vol {opp.vol_vs_avg:.2f}x | conf {opp.signal.confidence:.1f}% | RSI {opp.signal.rsi:.1f}"
            )
            vm = []
            if pe is not None:
                vm.append(f"P/E {pe:.1f}")
            if pb is not None:
                vm.append(f"P/B {pb:.2f}")
            if revenue_growth is not None:
                vm.append(f"RevGrowth {revenue_growth*100:.1f}%")
            if vm:
                detailed_reasons.append("Mixed quality/value cues: " + ", ".join(vm))
            if mos_pct is not None:
                detailed_reasons.append(f"Simple MOS band: {mos_band} ({mos_pct*100:.1f}%)")

        detailed_reasons.append(
            f"Execution context: {opp.signal.direction.upper()} {opp.signal.trend} | vol {opp.vol_vs_avg:.2f}x | $vol {opp.dollar_volume:,.0f} | setup {opp.setup_win_rate*100:.1f}%"
        )

        return {
            "value_score": float(value_score),
            "trend_score": float(trend_score),
            "liquidity_score": float(liquidity_score),
            "total_score": float(total_score),
            "compounder_score": float(compounder_score),
            "turnaround_score": float(turnaround_score),
            "primary_profile": str(primary_profile),
            "primary_score": float(primary_score),
            "range_position": range_pos,
            "market_cap_bucket": market_cap_bucket,
            "metrics": {
                "pe": pe,
                "forward_pe": forward_pe,
                "pb": pb,
                "peg": peg,
                "ev_ebitda": ev_ebitda,
                "roe": roe,
                "roa": roa,
                "gross_margin": gross_margin,
                "operating_margin": operating_margin,
                "profit_margin": profit_margin,
                "revenue_growth": revenue_growth,
                "earnings_growth": earnings_growth,
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "quick_ratio": quick_ratio,
                "free_cashflow": free_cashflow,
                "operating_cashflow": operating_cashflow,
                "market_cap": market_cap_val,
                "shares_outstanding": shares_outstanding,
                "current_price": current_price,
                "owner_earnings": owner_earnings,
                "owner_earnings_yield": owner_earnings_yield,
                "owner_earnings_per_share": owner_earnings_per_share,
                "fcf_yield": fcf_yield,
                "intrinsic_multiple": intrinsic_multiple,
                "intrinsic_value_est": intrinsic_value_est,
                "mos_pct": mos_pct,
                "mos_band": mos_band,
            },
            "detailed_reasons": [str(x) for x in detailed_reasons if str(x).strip()],
        }

    def _vi_scores(self, opp: StockOpportunity) -> tuple[float, float, float]:
        prof = self._vi_profile_scores(opp)
        return float(prof["value_score"]), float(prof["trend_score"]), float(prof["total_score"])

    def filter_quality(self, opportunities: list[StockOpportunity], min_score: int = 2) -> list[StockOpportunity]:
        return [
            o for o in opportunities
            if o.vol_vs_avg >= config.STOCK_MIN_VOL_RATIO
            and o.quality_score >= min_score
        ]

    def filter_watchlist(self, opportunities: list[StockOpportunity]) -> list[StockOpportunity]:
        """Keep watchlist informative but tradable by enforcing baseline volume/confidence floors."""
        filtered = [
            o for o in opportunities
            if o.vol_vs_avg >= config.WATCHLIST_MIN_VOL_RATIO
            and o.signal.confidence >= config.WATCHLIST_MIN_CONFIDENCE
        ]
        filtered.sort(
            key=lambda o: (o.composite_score, o.vol_vs_avg, o.signal.confidence),
            reverse=True,
        )
        return filtered

    def _is_market_open(self, market: str) -> bool:
        """Check if a specific market is currently open."""
        if market == "US":
            ny_now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            if ny_now.weekday() >= 5:
                return False
            return dt_time(9, 30) <= ny_now.time() <= dt_time(16, 0)

        now_utc = datetime.now(timezone.utc).strftime("%H:%M")
        hours = MARKET_HOURS.get(market, {})
        if not hours:
            return True  # assume open if unknown
        o, c = hours["open"], hours["close"]
        if o <= c:
            return o <= now_utc <= c
        return now_utc >= o or now_utc <= c

    def _analyze_stock_explain(
        self,
        symbol: str,
        *,
        allow_closed_markets: bool = False,
    ) -> tuple[Optional[StockOpportunity], str]:
        """Analyze a single stock and return (opportunity, reason_code)."""
        try:
            market = detect_market(symbol)

            # Skip if market is closed (for intraday TF)
            if (not allow_closed_markets) and (not self._is_market_open(market)):
                return None, "market_closed"

            # Fetch entry (1h) and trend (1d) data
            df_entry = fetch_stock_ohlcv(symbol, "1h", bars=200)
            if df_entry is None or len(df_entry) < 50:
                return None, "no_entry_data"
            df_entry_ta = ta.add_all(df_entry.copy())

            df_trend = fetch_stock_ohlcv(symbol, "1d", bars=100)

            session_info = session_manager.get_session_info()
            # Adapt session info for stock markets
            session_info["active_sessions"] = [market.lower()]

            signal = sig.score_signal(
                df_entry=df_entry_ta,
                df_trend=df_trend,
                symbol=symbol,
                timeframe="1h",
                session_info=session_info,
            )

            if signal is None:
                return None, "no_signal"

            setup = detect_setup(df_entry_ta)
            setup_labeled = self._directional_setup_label(setup, signal.direction)
            signal.pattern = setup_labeled

            # Volume vs average
            vol_ratio = 1.0
            if "vol_ratio" in df_entry_ta.columns:
                vol_ratio = float(df_entry_ta["vol_ratio"].iloc[-1])
            if not pd.notna(vol_ratio):
                vol_ratio = 1.0

            last_close = float(df_entry_ta["close"].iloc[-1])
            last_volume = float(df_entry_ta["volume"].iloc[-1])
            dollar_volume = max(0.0, last_close * max(0.0, last_volume))
            setup_win_rate = SETUP_WINRATE_MAP.get(setup, 0.5)
            quality_score, quality_tag = self._stock_quality_score(signal, float(vol_ratio))
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            raw_scores["vol_ratio"] = round(float(vol_ratio), 4)
            raw_scores["dollar_volume"] = round(float(dollar_volume), 2)
            raw_scores["setup_win_rate"] = round(float(setup_win_rate), 4)
            raw_scores["quality_score"] = int(quality_score)
            raw_scores["quality_tag"] = str(quality_tag)
            raw_scores["market"] = str(market)
            signal.raw_scores = raw_scores

            return StockOpportunity(
                signal=signal,
                market=market,
                setup_type=setup_labeled,
                base_setup_type=setup,
                vol_vs_avg=round(vol_ratio, 2),
                dollar_volume=dollar_volume,
                setup_win_rate=setup_win_rate,
                quality_score=quality_score,
                quality_tag=quality_tag,
            ), "ok"

        except Exception as e:
            logger.debug(f"_analyze_stock({symbol}): {e}")
            return None, "exception"

    def _analyze_stock(self, symbol: str) -> Optional[StockOpportunity]:
        """Analyze a single stock and return an opportunity if found."""
        opp, _ = self._analyze_stock_explain(symbol)
        return opp

    def scan_group(self, group_name: str) -> list[StockOpportunity]:
        """Scan a named market group (e.g. 'US_MEGA_CAP', 'TH_SET50')."""
        symbols = MARKET_GROUPS.get(group_name, [])
        return self._scan_symbols(symbols, label=group_name)

    def scan_all_open_markets(self) -> list[StockOpportunity]:
        """Scan all stocks in currently open markets only."""
        now_utc = datetime.now(timezone.utc).strftime("%H:%M")
        active_markets = get_active_markets_now(now_utc)
        logger.info(f"[STOCKS] Active markets at {now_utc} UTC: {active_markets}")

        # Collect tickers for active markets
        market_to_group = {
            "US": ["US_MEGA_CAP", "US_ETFS"],
            "UK": ["UK_FTSE100"],
            "DE": ["DE_DAX40"],
            "FR": ["FR_CAC40"],
            "JP": ["JP_NIKKEI"],
            "HK": ["HK_HANGSENG", "CN_STOCKS"],
            "TH": ["TH_SET50"],
            "SG": ["SG_STI"],
            "IN": ["IN_NIFTY"],
            "AU": ["AU_ASX"],
        }

        tickers = []
        for mkt in active_markets:
            for grp in market_to_group.get(mkt, []):
                tickers.extend(MARKET_GROUPS.get(grp, []))

        if not tickers:
            logger.info("[STOCKS] No markets open right now, scanning priority stocks only")
            tickers = PRIORITY_STOCKS

        tickers = list(dict.fromkeys(tickers))  # deduplicate
        return self._scan_symbols(tickers, label="OPEN_MARKETS")

    def scan_priority(self) -> list[StockOpportunity]:
        """Quick scan of high-priority stocks across all markets."""
        return self._scan_symbols(PRIORITY_STOCKS, label="PRIORITY")

    def scan_thailand(self) -> list[StockOpportunity]:
        """Dedicated Thailand SET50 scan."""
        return self._scan_symbols(
            MARKET_GROUPS["TH_SET50"],
            label="SET50_TH",
            collect_diag=True,
            use_mt5_prefilter=False,
        )

    def scan_us(self, allow_premarket: bool = False) -> list[StockOpportunity]:
        """US mega cap + ETF scan."""
        symbols = MARKET_GROUPS["US_MEGA_CAP"] + MARKET_GROUPS["US_ETFS"]
        return self._scan_symbols(symbols, label="US", allow_closed_markets=allow_premarket, collect_diag=True)

    def scan_us_open_daytrade(self, top_n: int = 10, allow_premarket: bool = False) -> list[StockOpportunity]:
        """
        US-open day-trade selector:
        rank by liquidity + setup win-rate + confidence, return top N.
        """
        top_n = max(1, min(int(top_n), 20))
        opportunities = self.scan_us(allow_premarket=allow_premarket)
        if not opportunities:
            base_diag = dict(self._last_scan_diag.get("US", {}) or {})
            self._last_us_open_diag = {
                "mode": "premarket" if allow_premarket else "regular",
                "upstream": base_diag,
                "strict_filter": {
                    "total_opportunities": 0,
                    "passed": 0,
                    "fail_confidence": 0,
                    "fail_volume": 0,
                    "fail_dollar_volume": 0,
                },
                "thresholds": {
                    "min_confidence": int(config.STOCK_MIN_CONFIDENCE),
                    "min_vol_ratio": float(config.US_OPEN_MIN_VOL_RATIO),
                    "min_dollar_volume": float(config.US_OPEN_MIN_DOLLAR_VOLUME),
                },
            }
            if base_diag:
                reasons = base_diag.get("reject_reasons", {})
                logger.info(
                    "[STOCKS:US_OPEN] No raw opportunities (%s mode) | "
                    "market_closed=%s no_data=%s no_signal=%s exception=%s",
                    self._last_us_open_diag["mode"],
                    reasons.get("market_closed", 0),
                    reasons.get("no_entry_data", 0),
                    reasons.get("no_signal", 0),
                    reasons.get("exception", 0),
                )
            return []

        # Keep only liquid, higher-quality candidates for 1-2h opening session trades.
        strict_diag = {
            "total_opportunities": len(opportunities),
            "passed": 0,
            "fail_confidence": 0,
            "fail_volume": 0,
            "fail_dollar_volume": 0,
        }
        filtered: list[StockOpportunity] = []
        min_conf = int(config.STOCK_MIN_CONFIDENCE)
        min_vol = float(config.US_OPEN_MIN_VOL_RATIO)
        min_dv = float(config.US_OPEN_MIN_DOLLAR_VOLUME)
        for o in opportunities:
            ok = True
            if o.signal.confidence < min_conf:
                strict_diag["fail_confidence"] += 1
                ok = False
            if o.vol_vs_avg < min_vol:
                strict_diag["fail_volume"] += 1
                ok = False
            if o.dollar_volume < min_dv:
                strict_diag["fail_dollar_volume"] += 1
                ok = False
            if ok:
                strict_diag["passed"] += 1
                filtered.append(o)
        self._last_us_open_diag = {
            "mode": "premarket" if allow_premarket else "regular",
            "upstream": dict(self._last_scan_diag.get("US", {}) or {}),
            "strict_filter": strict_diag,
            "thresholds": {
                "min_confidence": min_conf,
                "min_vol_ratio": min_vol,
                "min_dollar_volume": min_dv,
            },
        }
        if not filtered:
            logger.info(
                "[STOCKS:US_OPEN] No candidates passed strict filter (%s mode) | "
                "raw=%s passed=%s fail_conf=%s fail_vol=%s fail_dv=%s | "
                "thresholds conf>=%s vol_ratio>=%s dollar_volume>=%s",
                self._last_us_open_diag["mode"],
                strict_diag["total_opportunities"],
                strict_diag["passed"],
                strict_diag["fail_confidence"],
                strict_diag["fail_volume"],
                strict_diag["fail_dollar_volume"],
                min_conf,
                min_vol,
                f"{min_dv:,.0f}",
            )
            return []

        filtered.sort(
            key=lambda o: (o.us_open_rank_score, o.dollar_volume, o.setup_win_rate, o.signal.confidence),
            reverse=True,
        )
        return filtered[:top_n]

    def scan_us_value_trend(self, top_n: int = 10) -> list[StockOpportunity]:
        """
        US value + trend candidates for VI-style stock selection.
        Focuses on fundamentally reasonable valuation with strong momentum.
        """
        top_n = max(1, min(int(top_n), 20))
        us_market_open = self._is_market_open("US")
        # VI/value-style scan should be available off-hours; use historical bars and relaxed gating when market is closed.
        if us_market_open:
            opportunities = self.scan_us()
        else:
            opportunities = self.scan_us(allow_premarket=True)
            if not opportunities:
                opportunities = self._scan_symbols(
                    MARKET_GROUPS["US_MEGA_CAP"] + MARKET_GROUPS["US_ETFS"],
                    label="US",
                    allow_closed_markets=True,
                    collect_diag=True,
                    use_mt5_prefilter=False,
                )
        if not opportunities:
            return []

        vi_conf = float(getattr(config, "VI_MIN_CONFIDENCE", config.STOCK_MIN_CONFIDENCE))
        vi_vol = float(getattr(config, "VI_MIN_VOL_RATIO", 0.7))
        vi_dv = float(getattr(config, "VI_MIN_DOLLAR_VOLUME", 8_000_000))
        vi_wr = float(getattr(config, "VI_MIN_SETUP_WIN_RATE", 0.56))
        vi_rsi_min = float(getattr(config, "VI_RSI_MIN", 54))
        vi_rsi_max = float(getattr(config, "VI_RSI_MAX", 67))
        vi_qs = max(1, int(getattr(config, "VI_REQUIRE_QUALITY_SCORE", 2)))
        vi_long_only = bool(getattr(config, "VI_LONG_ONLY", True))

        if not us_market_open:
            # Off-hours: relax intraday/tape-sensitive thresholds; still keep trend + valuation discipline.
            vi_conf = min(vi_conf, 65.0)
            vi_vol = min(vi_vol, 0.7)
            vi_dv = min(vi_dv, 12_000_000.0)
            vi_wr = min(vi_wr, 0.54)
            vi_rsi_min = min(vi_rsi_min, 48.0)
            vi_rsi_max = max(vi_rsi_max, 74.0)
            vi_qs = min(vi_qs, 1)
            logger.info(
                "[STOCKS:US_VI] Off-hours relaxed mode | conf>=%s vol_ratio>=%s dollar_volume>=%s setup_wr>=%s rsi=%s-%s qscore>=%s",
                vi_conf, vi_vol, f"{vi_dv:,.0f}", vi_wr, vi_rsi_min, vi_rsi_max, vi_qs,
            )

        base = [
            o for o in opportunities
            if o.signal.confidence >= vi_conf
            and o.vol_vs_avg >= vi_vol
            and o.dollar_volume >= vi_dv
            and o.quality_score >= vi_qs
            and o.setup_win_rate >= vi_wr
            and vi_rsi_min <= float(o.signal.rsi) <= vi_rsi_max
            and str(o.signal.trend).lower() == "bullish"
        ]
        if not base:
            return []

        if vi_long_only:
            candidates = [o for o in base if str(o.signal.direction).lower() == "long"]
            if not candidates:
                return []
        else:
            candidates = list(base)
        candidates.sort(key=lambda o: (o.composite_score, o.signal.confidence), reverse=True)
        cap = max(top_n, int(getattr(config, "VI_MAX_CANDIDATES", 20)))
        candidates = candidates[:cap]

        for opp in candidates:
            info = self._get_stock_info_cached(opp.signal.symbol)
            opp.sector = str(info.get("sector", "") or "")
            pe_val = info.get("pe_ratio")
            high_52 = info.get("52w_high")
            low_52 = info.get("52w_low")
            try:
                opp.pe_ratio = float(pe_val) if pe_val is not None else None
            except Exception:
                opp.pe_ratio = None
            try:
                opp.week_52_high = float(high_52) if high_52 is not None else None
            except Exception:
                opp.week_52_high = None
            try:
                opp.week_52_low = float(low_52) if low_52 is not None else None
            except Exception:
                opp.week_52_low = None

            vi_meta = self._vi_profile_scores(opp, info)
            value_score = float(vi_meta["value_score"])
            trend_score = float(vi_meta["trend_score"])
            vi_score = float(vi_meta["total_score"])
            raw = dict(getattr(opp.signal, "raw_scores", {}) or {})
            raw["vi_value_score"] = round(value_score * 100.0, 2)
            raw["vi_trend_score"] = round(trend_score * 100.0, 2)
            raw["vi_total_score"] = round(vi_score * 100.0, 2)
            raw["vi_compounder_score"] = round(float(vi_meta["compounder_score"]) * 100.0, 2)
            raw["vi_turnaround_score"] = round(float(vi_meta["turnaround_score"]) * 100.0, 2)
            raw["vi_primary_profile"] = str(vi_meta["primary_profile"])
            raw["vi_primary_score"] = round(float(vi_meta["primary_score"]) * 100.0, 2)
            if vi_meta.get("range_position") is not None:
                raw["vi_range_position_pct"] = round(float(vi_meta["range_position"]) * 100.0, 2)
            raw["vi_market_cap_bucket"] = str(vi_meta.get("market_cap_bucket") or "unknown")
            raw["vi_reasons_detailed"] = list(vi_meta.get("detailed_reasons") or [])
            metrics = dict(vi_meta.get("metrics") or {})
            for k, v in metrics.items():
                if v is None:
                    continue
                if isinstance(v, (int, float)):
                    raw[f"vi_metric_{k}"] = round(float(v), 6)
                else:
                    raw[f"vi_metric_{k}"] = v
            opp.signal.raw_scores = raw

        compounders = [
            o for o in candidates
            if str((o.signal.raw_scores or {}).get("vi_primary_profile", "")).upper() == "BUFFETT"
        ]
        turnarounds = [
            o for o in candidates
            if str((o.signal.raw_scores or {}).get("vi_primary_profile", "")).upper() == "TURNAROUND"
        ]
        blends = [
            o for o in candidates
            if str((o.signal.raw_scores or {}).get("vi_primary_profile", "")).upper() not in {"BUFFETT", "TURNAROUND"}
        ]
        compounders.sort(
            key=lambda o: (
                float((o.signal.raw_scores or {}).get("vi_compounder_score", 0)),
                float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
                o.signal.confidence,
                o.dollar_volume,
            ),
            reverse=True,
        )
        turnarounds.sort(
            key=lambda o: (
                float((o.signal.raw_scores or {}).get("vi_turnaround_score", 0)),
                float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
                o.signal.confidence,
                o.dollar_volume,
            ),
            reverse=True,
        )
        blends.sort(
            key=lambda o: (
                float((o.signal.raw_scores or {}).get("vi_primary_score", 0)),
                float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
                o.signal.confidence,
                o.dollar_volume,
            ),
            reverse=True,
        )
        selected: list[StockOpportunity] = []
        seen: set[str] = set()
        target_turn = min(len(turnarounds), max(1, top_n // 3)) if turnarounds else 0
        target_buff = min(len(compounders), max(1, top_n - target_turn))
        def _take_from(pool: list[StockOpportunity], limit: int) -> None:
            if limit <= 0:
                return
            taken = 0
            for o in pool:
                sym = str(o.signal.symbol).upper()
                if sym in seen:
                    continue
                selected.append(o)
                seen.add(sym)
                taken += 1
                if taken >= limit or len(selected) >= top_n:
                    break

        _take_from(compounders, target_buff)
        if len(selected) < top_n:
            _take_from(turnarounds, target_turn)
        if len(selected) < top_n:
            _take_from(blends, top_n - len(selected))
        if len(selected) < top_n:
            all_ranked = compounders + turnarounds + blends
            all_ranked.sort(
                key=lambda o: (
                    float((o.signal.raw_scores or {}).get("vi_primary_score", 0)),
                    float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
                    o.setup_win_rate,
                    o.signal.confidence,
                    o.dollar_volume,
                ),
                reverse=True,
            )
            for o in all_ranked:
                sym = str(o.signal.symbol).upper()
                if sym in seen:
                    continue
                selected.append(o)
                seen.add(sym)
                if len(selected) >= top_n:
                    break
        return selected[:top_n]

    def scan_us_value_trend_profile(self, profile: str, top_n: int = 10) -> list[StockOpportunity]:
        """US VI scan filtered by primary profile (BUFFETT / TURNAROUND)."""
        p = str(profile or "").strip().upper()
        base = self.scan_us_value_trend(top_n=max(12, int(top_n) * 3))
        if not base:
            return []
        if p not in {"BUFFETT", "TURNAROUND"}:
            return base[:max(1, int(top_n))]
        exact = [o for o in base if str((o.signal.raw_scores or {}).get("vi_primary_profile", "")).upper() == p]
        key_name = "vi_compounder_score" if p == "BUFFETT" else "vi_turnaround_score"
        if exact:
            exact.sort(key=lambda o: (
                float((o.signal.raw_scores or {}).get(key_name, 0)),
                float((o.signal.raw_scores or {}).get("vi_primary_score", 0)),
                float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
                o.signal.confidence,
            ), reverse=True)
            return exact[:max(1, int(top_n))]
        blends = [o for o in base if str((o.signal.raw_scores or {}).get("vi_primary_profile", "")).upper() == "BLEND"]
        blends.sort(key=lambda o: (
            float((o.signal.raw_scores or {}).get(key_name, 0)),
            float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
            o.signal.confidence,
        ), reverse=True)
        return blends[:max(1, int(top_n))]

    def scan_thailand_value_trend(self, top_n: int = 10) -> list[StockOpportunity]:
        """
        Thailand SET50 value + trend candidates.
        Off-hours friendly by design (uses historical bars, not intraday market-open gating).
        """
        top_n = max(1, min(int(top_n), 20))
        th_market_open = self._is_market_open("TH")
        opportunities = self._scan_symbols(
            MARKET_GROUPS["TH_SET50"],
            label="TH_VI",
            allow_closed_markets=True,
            collect_diag=True,
            use_mt5_prefilter=False,
        )
        if not opportunities:
            return []

        vi_conf = float(getattr(config, "TH_VI_MIN_CONFIDENCE", getattr(config, "VI_MIN_CONFIDENCE", config.STOCK_MIN_CONFIDENCE)))
        vi_vol = float(getattr(config, "TH_VI_MIN_VOL_RATIO", getattr(config, "VI_MIN_VOL_RATIO", 0.7)))
        vi_dv = float(getattr(config, "TH_VI_MIN_DOLLAR_VOLUME", getattr(config, "VI_MIN_DOLLAR_VOLUME", 8_000_000)))
        vi_wr = float(getattr(config, "TH_VI_MIN_SETUP_WIN_RATE", getattr(config, "VI_MIN_SETUP_WIN_RATE", 0.56)))
        vi_rsi_min = float(getattr(config, "TH_VI_RSI_MIN", getattr(config, "VI_RSI_MIN", 54)))
        vi_rsi_max = float(getattr(config, "TH_VI_RSI_MAX", getattr(config, "VI_RSI_MAX", 67)))
        vi_qs = max(1, int(getattr(config, "TH_VI_REQUIRE_QUALITY_SCORE", getattr(config, "VI_REQUIRE_QUALITY_SCORE", 2))))
        vi_long_only = bool(getattr(config, "TH_VI_LONG_ONLY", getattr(config, "VI_LONG_ONLY", True)))

        if not th_market_open:
            vi_conf = min(vi_conf, 65.0)
            vi_vol = min(vi_vol, 0.7)
            vi_dv = min(vi_dv, 6_000_000.0)
            vi_wr = min(vi_wr, 0.53)
            vi_rsi_min = min(vi_rsi_min, 48.0)
            vi_rsi_max = max(vi_rsi_max, 74.0)
            vi_qs = min(vi_qs, 1)
            logger.info(
                "[STOCKS:TH_VI] Off-hours relaxed mode | conf>=%s vol_ratio>=%s dollar_volume>=%s setup_wr>=%s rsi=%s-%s qscore>=%s",
                vi_conf, vi_vol, f"{vi_dv:,.0f}", vi_wr, vi_rsi_min, vi_rsi_max, vi_qs,
            )

        vi_diag = {
            "thresholds": {
                "min_confidence": vi_conf,
                "min_vol_ratio": vi_vol,
                "min_dollar_volume": vi_dv,
                "min_setup_win_rate": vi_wr,
                "rsi_min": vi_rsi_min,
                "rsi_max": vi_rsi_max,
                "min_quality_score": vi_qs,
                "long_only": bool(vi_long_only),
                "market_open": bool(th_market_open),
            },
            "raw_opportunities": len(opportunities),
            "base_passed": 0,
            "fail_confidence": 0,
            "fail_volume": 0,
            "fail_dollar_volume": 0,
            "fail_quality": 0,
            "fail_setup_wr": 0,
            "fail_rsi": 0,
            "fail_trend": 0,
            "after_direction": 0,
            "fail_direction": 0,
            "final_returned": 0,
        }
        base: list[StockOpportunity] = []
        for o in opportunities:
            ok = True
            if o.signal.confidence < vi_conf:
                vi_diag["fail_confidence"] += 1
                ok = False
            if o.vol_vs_avg < vi_vol:
                vi_diag["fail_volume"] += 1
                ok = False
            if o.dollar_volume < vi_dv:
                vi_diag["fail_dollar_volume"] += 1
                ok = False
            if o.quality_score < vi_qs:
                vi_diag["fail_quality"] += 1
                ok = False
            if o.setup_win_rate < vi_wr:
                vi_diag["fail_setup_wr"] += 1
                ok = False
            try:
                rsi_val = float(o.signal.rsi)
            except Exception:
                rsi_val = 50.0
            if not (vi_rsi_min <= rsi_val <= vi_rsi_max):
                vi_diag["fail_rsi"] += 1
                ok = False
            if str(o.signal.trend).lower() != "bullish":
                vi_diag["fail_trend"] += 1
                ok = False
            if ok:
                base.append(o)
        vi_diag["base_passed"] = len(base)
        base_scan_diag = dict((self._last_scan_diag or {}).get("TH_VI", {}) or {})
        base_scan_diag["vi_filter"] = vi_diag
        self._last_scan_diag["TH_VI"] = base_scan_diag
        if not base:
            logger.info(
                "[STOCKS:TH_VI] No candidates passed VI filter | raw=%s pass=%s fail_conf=%s fail_vol=%s fail_dv=%s fail_q=%s fail_wr=%s fail_rsi=%s fail_trend=%s",
                vi_diag["raw_opportunities"],
                vi_diag["base_passed"],
                vi_diag["fail_confidence"],
                vi_diag["fail_volume"],
                vi_diag["fail_dollar_volume"],
                vi_diag["fail_quality"],
                vi_diag["fail_setup_wr"],
                vi_diag["fail_rsi"],
                vi_diag["fail_trend"],
            )
            return []

        if vi_long_only:
            candidates = [o for o in base if str(o.signal.direction).lower() == "long"]
            vi_diag["after_direction"] = len(candidates)
            vi_diag["fail_direction"] = max(0, len(base) - len(candidates))
            if not candidates:
                base_scan_diag = dict((self._last_scan_diag or {}).get("TH_VI", {}) or {})
                base_scan_diag["vi_filter"] = vi_diag
                self._last_scan_diag["TH_VI"] = base_scan_diag
                logger.info(
                    "[STOCKS:TH_VI] No candidates after long-only filter | base=%s long=%s fail_direction=%s",
                    len(base), len(candidates), vi_diag["fail_direction"],
                )
                return []
        else:
            candidates = list(base)
            vi_diag["after_direction"] = len(candidates)
        candidates.sort(key=lambda o: (o.composite_score, o.signal.confidence), reverse=True)
        cap = max(top_n, int(getattr(config, "VI_MAX_CANDIDATES", 20)))
        candidates = candidates[:cap]

        for opp in candidates:
            info = self._get_stock_info_cached(opp.signal.symbol)
            opp.sector = str(info.get("sector", "") or "")
            pe_val = info.get("pe_ratio")
            high_52 = info.get("52w_high")
            low_52 = info.get("52w_low")
            try:
                opp.pe_ratio = float(pe_val) if pe_val is not None else None
            except Exception:
                opp.pe_ratio = None
            try:
                opp.week_52_high = float(high_52) if high_52 is not None else None
            except Exception:
                opp.week_52_high = None
            try:
                opp.week_52_low = float(low_52) if low_52 is not None else None
            except Exception:
                opp.week_52_low = None

            vi_meta = self._vi_profile_scores(opp, info)
            value_score = float(vi_meta["value_score"])
            trend_score = float(vi_meta["trend_score"])
            vi_score = float(vi_meta["total_score"])
            raw = dict(getattr(opp.signal, "raw_scores", {}) or {})
            raw["vi_value_score"] = round(value_score * 100.0, 2)
            raw["vi_trend_score"] = round(trend_score * 100.0, 2)
            raw["vi_total_score"] = round(vi_score * 100.0, 2)
            raw["vi_compounder_score"] = round(float(vi_meta["compounder_score"]) * 100.0, 2)
            raw["vi_turnaround_score"] = round(float(vi_meta["turnaround_score"]) * 100.0, 2)
            raw["vi_primary_profile"] = str(vi_meta["primary_profile"])
            raw["vi_primary_score"] = round(float(vi_meta["primary_score"]) * 100.0, 2)
            if vi_meta.get("range_position") is not None:
                raw["vi_range_position_pct"] = round(float(vi_meta["range_position"]) * 100.0, 2)
            raw["vi_market_cap_bucket"] = str(vi_meta.get("market_cap_bucket") or "unknown")
            raw["vi_reasons_detailed"] = list(vi_meta.get("detailed_reasons") or [])
            raw["vi_region"] = "TH"
            metrics = dict(vi_meta.get("metrics") or {})
            for k, v in metrics.items():
                if v is None:
                    continue
                if isinstance(v, (int, float)):
                    raw[f"vi_metric_{k}"] = round(float(v), 6)
                else:
                    raw[f"vi_metric_{k}"] = v
            opp.signal.raw_scores = raw

        candidates.sort(
            key=lambda o: (
                float((o.signal.raw_scores or {}).get("vi_primary_score", 0)),
                float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
                o.setup_win_rate,
                o.signal.confidence,
                o.dollar_volume,
            ),
            reverse=True,
        )
        out = candidates[:top_n]
        vi_diag["final_returned"] = len(out)
        base_scan_diag = dict((self._last_scan_diag or {}).get("TH_VI", {}) or {})
        base_scan_diag["vi_filter"] = vi_diag
        self._last_scan_diag["TH_VI"] = base_scan_diag
        return out

    def scan_thailand_value_trend_profile(self, profile: str, top_n: int = 10) -> list[StockOpportunity]:
        """Thailand VI scan filtered by primary profile (BUFFETT / TURNAROUND)."""
        p = str(profile or "").strip().upper()
        base = self.scan_thailand_value_trend(top_n=max(12, int(top_n) * 3))
        if not base:
            return []
        if p not in {"BUFFETT", "TURNAROUND"}:
            return base[:max(1, int(top_n))]
        exact = [o for o in base if str((o.signal.raw_scores or {}).get("vi_primary_profile", "")).upper() == p]
        key_name = "vi_compounder_score" if p == "BUFFETT" else "vi_turnaround_score"
        if exact:
            exact.sort(key=lambda o: (
                float((o.signal.raw_scores or {}).get(key_name, 0)),
                float((o.signal.raw_scores or {}).get("vi_primary_score", 0)),
                float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
                o.signal.confidence,
            ), reverse=True)
            return exact[:max(1, int(top_n))]
        blends = [o for o in base if str((o.signal.raw_scores or {}).get("vi_primary_profile", "")).upper() == "BLEND"]
        blends.sort(key=lambda o: (
            float((o.signal.raw_scores or {}).get(key_name, 0)),
            float((o.signal.raw_scores or {}).get("vi_total_score", 0)),
            o.signal.confidence,
        ), reverse=True)
        return blends[:max(1, int(top_n))]

    def _scan_symbols(
        self,
        symbols: list[str],
        label: str = "",
        *,
        allow_closed_markets: bool = False,
        collect_diag: bool = False,
        use_mt5_prefilter: Optional[bool] = None,
    ) -> list[StockOpportunity]:
        """Parallel scan a list of symbols."""
        self.scan_count += 1
        start = time.time()
        symbols_in = list(symbols or [])
        prefilter_diag: dict = {}
        symbols = symbols_in
        try:
            if use_mt5_prefilter is None:
                do_prefilter = self._label_prefers_mt5_filter(label)
            else:
                do_prefilter = bool(use_mt5_prefilter)
            if do_prefilter:
                symbols, prefilter_diag = self._prefilter_mt5_tradable_symbols(symbols_in, label=label)
            else:
                symbols, prefilter_diag = list(symbols_in), {
                    "enabled": bool(self.get_mt5_tradable_only().get("effective")),
                    "applied": False,
                    "skipped_by_scan_policy": True,
                    "input": len(symbols_in),
                    "kept": len(symbols_in),
                    "removed": 0,
                    "unmapped": 0,
                    "error": "",
                }
        except Exception:
            symbols = symbols_in
            prefilter_diag = {}
        logger.info(f"[STOCKS:{label}] Scan #{self.scan_count} | {len(symbols)} symbols")

        opportunities = []
        reject_reasons: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._analyze_stock_explain, sym, allow_closed_markets=allow_closed_markets): sym
                       for sym in symbols}
            for future in as_completed(futures):
                try:
                    opp, reason = future.result(timeout=45)
                    if opp is not None:
                        opportunities.append(opp)
                    elif collect_diag:
                        reject_reasons[reason] = int(reject_reasons.get(reason, 0) or 0) + 1
                except Exception as e:
                    logger.debug(f"Future error: {e}")
                    if collect_diag:
                        reject_reasons["future_error"] = int(reject_reasons.get("future_error", 0) or 0) + 1

        opportunities.sort(key=lambda x: x.composite_score, reverse=True)
        opportunities.sort(
            key=lambda x: (x.quality_score, x.composite_score, x.vol_vs_avg, x.signal.confidence),
            reverse=True,
        )

        elapsed = time.time() - start
        self.total_signals += len(opportunities)
        if collect_diag:
            self._last_scan_diag[label] = {
                "label": label,
                "symbols": len(symbols),
                "symbols_input": len(symbols_in),
                "opportunities": len(opportunities),
                "reject_reasons": dict(sorted(reject_reasons.items())),
                "elapsed_sec": round(elapsed, 3),
                "allow_closed_markets": bool(allow_closed_markets),
                "prefilter": dict(prefilter_diag or {}),
            }
        logger.info(
            f"[STOCKS:{label}] ✅ {len(opportunities)} signals found "
            f"from {len(symbols)} stocks in {elapsed:.1f}s"
        )
        return opportunities

    def get_last_us_open_diagnostics(self) -> dict:
        return dict(self._last_us_open_diag or {})

    def get_last_scan_diagnostics(self, label: str) -> dict:
        return dict((self._last_scan_diag or {}).get(str(label or ""), {}) or {})

    def get_market_overview(self) -> dict:
        """Return a status summary of all global markets."""
        now_utc = datetime.now(timezone.utc).strftime("%H:%M")
        active = get_active_markets_now(now_utc)
        overview = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "active_markets": active,
            "all_markets": {},
        }
        for market, hours in MARKET_HOURS.items():
            overview["all_markets"][market] = {
                "open": self._is_market_open(market),
                "hours_utc": f"{hours['open']} - {hours['close']}",
                "tz": hours["tz"],
            }
        return overview

    def get_stats(self) -> dict:
        return {
            "total_scans":   self.scan_count,
            "total_signals": self.total_signals,
            "markets":       list(MARKET_HOURS.keys()),
        }


stock_scanner = GlobalStockScanner()
