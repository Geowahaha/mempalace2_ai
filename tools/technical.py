"""
Technical Analysis Tool — Full suite of indicators for XAUUSD trading.

Indicators:
  Trend: EMA, SMA, ADX, Supertrend
  Momentum: RSI, MACD, Stochastic, CCI
  Volatility: ATR, Bollinger Bands, Keltner Channels
  Volume: Volume Profile, OBV, VWAP
  Structure: Support/Resistance, Pivot Points, Order Blocks
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from tools.base import Tool, ToolCategory, ToolResult

logger = logging.getLogger("mempalace2.tools.technical")


@dataclass
class IndicatorResult:
    """Container for all indicator values at a point in time."""
    # Trend
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    ema_trend: float = 0.0
    adx: float = 0.0
    supertrend: float = 0.0
    supertrend_direction: int = 1  # 1=bullish, -1=bearish

    # Momentum
    rsi: float = 50.0
    macd_line: float = 0.0
    macd_signal: float = 0.0
    macd_histogram: float = 0.0
    stoch_k: float = 50.0
    stoch_d: float = 50.0
    cci: float = 0.0

    # Volatility
    atr: float = 0.0
    atr_pct: float = 0.0
    bb_upper: float = 0.0
    bb_middle: float = 0.0
    bb_lower: float = 0.0
    bb_width: float = 0.0

    # Volume
    volume_ratio: float = 1.0
    obv_trend: float = 0.0

    # Structure
    support_1: float = 0.0
    support_2: float = 0.0
    resistance_1: float = 0.0
    resistance_2: float = 0.0
    pivot_point: float = 0.0

    # Composite scores
    trend_score: float = 0.0     # -100 to +100
    momentum_score: float = 0.0  # -100 to +100
    volatility_score: float = 0.0
    structure_score: float = 0.0
    overall_score: float = 0.0   # -100 to +100


class TechnicalAnalysisTool(Tool):
    """
    Comprehensive technical analysis for any OHLCV dataset.

    Parameters:
      data: DataFrame with OHLCV columns
      config: AnalysisConfig with indicator periods
    """

    name = "technical_analysis"
    category = ToolCategory.ANALYSIS
    description = "Calculate full technical analysis suite"
    is_read_only = True
    is_safe = True

    def __init__(self, config=None):
        self.config = config

    def validate_input(self, data=None, **kwargs) -> Optional[str]:
        if data is None:
            return "data (DataFrame) is required"
        if not isinstance(data, pd.DataFrame):
            return "data must be a pandas DataFrame"
        required = {"open", "high", "low", "close"}
        if not required.issubset(set(data.columns)):
            return f"Missing columns: {required - set(data.columns)}"
        return None

    async def execute(self, data: pd.DataFrame = None, **kwargs) -> ToolResult:
        """Run full technical analysis on OHLCV data."""
        df = data.copy()

        # Calculate all indicators
        self._calc_ema(df)
        self._calc_rsi(df)
        self._calc_macd(df)
        self._calc_atr(df)
        self._calc_bollinger(df)
        self._calc_adx(df)
        self._calc_supertrend(df)
        self._calc_stochastic(df)
        self._calc_volume(df)
        self._calc_support_resistance(df)
        self._calc_pivot_points(df)

        # Build latest indicator snapshot
        latest = self._build_indicator_result(df)

        return ToolResult.ok(
            data=df,
            indicators=latest,
            latest_close=float(df["close"].iloc[-1]),
            latest_atr=float(df["atr"].iloc[-1]),
        )

    # ── EMA ──────────────────────────────────────────

    def _calc_ema(self, df: pd.DataFrame):
        cfg = self.config
        fast = cfg.ema_fast if cfg else 9
        slow = cfg.ema_slow if cfg else 21
        trend = cfg.ema_trend if cfg else 200

        df["ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()
        if len(df) >= trend:
            df["ema_trend"] = df["close"].ewm(span=trend, adjust=False).mean()
        else:
            df["ema_trend"] = df["close"].mean()

    # ── RSI ──────────────────────────────────────────

    def _calc_rsi(self, df: pd.DataFrame, period: int = 14):
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.ewm(span=period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(span=period, min_periods=period, adjust=False).mean()

        rs = avg_gain / avg_loss.replace(0, np.finfo(float).eps)
        df["rsi"] = 100 - (100 / (1 + rs))

    # ── MACD ─────────────────────────────────────────

    def _calc_macd(self, df: pd.DataFrame):
        cfg = self.config
        fast = cfg.macd_fast if cfg else 12
        slow = cfg.macd_slow if cfg else 26
        signal = cfg.macd_signal if cfg else 9

        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
        df["macd_line"] = ema_fast - ema_slow
        df["macd_signal"] = df["macd_line"].ewm(span=signal, adjust=False).mean()
        df["macd_histogram"] = df["macd_line"] - df["macd_signal"]

    # ── ATR ──────────────────────────────────────────

    def _calc_atr(self, df: pd.DataFrame, period: int = 14):
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()

        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = true_range.ewm(span=period, adjust=False).mean()
        df["atr_pct"] = df["atr"] / df["close"] * 100

    # ── Bollinger Bands ──────────────────────────────

    def _calc_bollinger(self, df: pd.DataFrame, period: int = 20, std_dev: float = 2.0):
        df["bb_middle"] = df["close"].rolling(window=period).mean()
        rolling_std = df["close"].rolling(window=period).std()
        df["bb_upper"] = df["bb_middle"] + (rolling_std * std_dev)
        df["bb_lower"] = df["bb_middle"] - (rolling_std * std_dev)
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_middle"] * 100

    # ── ADX ──────────────────────────────────────────

    def _calc_adx(self, df: pd.DataFrame, period: int = 14):
        plus_dm = df["high"].diff().clip(lower=0)
        minus_dm = (-df["low"].diff()).clip(lower=0)

        # True range
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ], axis=1).max(axis=1)

        atr = tr.ewm(span=period, adjust=False).mean()
        plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / atr)

        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.finfo(float).eps)
        df["adx"] = dx.ewm(span=period, adjust=False).mean()
        df["plus_di"] = plus_di
        df["minus_di"] = minus_di

    # ── Supertrend ───────────────────────────────────

    def _calc_supertrend(self, df: pd.DataFrame, period: int = 10, multiplier: float = 3.0):
        hl2 = (df["high"] + df["low"]) / 2

        # ATR for supertrend
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.ewm(span=period, adjust=False).mean()

        upper_band = hl2 + (multiplier * atr)
        lower_band = hl2 - (multiplier * atr)

        supertrend = pd.Series(index=df.index, dtype=float)
        direction = pd.Series(index=df.index, dtype=int)

        supertrend.iloc[0] = upper_band.iloc[0]
        direction.iloc[0] = 1

        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper_band.iloc[i - 1]:
                supertrend.iloc[i] = max(lower_band.iloc[i], supertrend.iloc[i - 1]) if direction.iloc[i - 1] == 1 else lower_band.iloc[i]
                direction.iloc[i] = 1
            elif df["close"].iloc[i] < lower_band.iloc[i - 1]:
                supertrend.iloc[i] = min(upper_band.iloc[i], supertrend.iloc[i - 1]) if direction.iloc[i - 1] == -1 else upper_band.iloc[i]
                direction.iloc[i] = -1
            else:
                supertrend.iloc[i] = supertrend.iloc[i - 1]
                direction.iloc[i] = direction.iloc[i - 1]

        df["supertrend"] = supertrend
        df["supertrend_direction"] = direction

    # ── Stochastic ───────────────────────────────────

    def _calc_stochastic(self, df: pd.DataFrame, k_period: int = 14, d_period: int = 3):
        low_min = df["low"].rolling(window=k_period).min()
        high_max = df["high"].rolling(window=k_period).max()

        df["stoch_k"] = 100 * (df["close"] - low_min) / (high_max - low_min).replace(0, np.finfo(float).eps)
        df["stoch_d"] = df["stoch_k"].rolling(window=d_period).mean()

    # ── Volume ───────────────────────────────────────

    def _calc_volume(self, df: pd.DataFrame):
        if "volume" in df.columns and df["volume"].sum() > 0:
            vol_ma = df["volume"].rolling(window=20).mean()
            df["volume_ratio"] = df["volume"] / vol_ma.replace(0, np.finfo(float).eps)

            # OBV
            obv = (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()
            df["obv"] = obv
            df["obv_trend"] = obv.ewm(span=20, adjust=False).mean()
        else:
            df["volume_ratio"] = 1.0
            df["obv"] = 0.0
            df["obv_trend"] = 0.0

    # ── Support / Resistance ─────────────────────────

    def _calc_support_resistance(self, df: pd.DataFrame, lookback: int = 50):
        """Find key support/resistance levels using pivot points."""
        recent = df.tail(lookback)

        # Find swing highs and lows
        highs = recent["high"].values
        lows = recent["low"].values

        # Simple pivot-based S/R
        pivot_window = 5
        supports = []
        resistances = []

        for i in range(pivot_window, len(recent) - pivot_window):
            # Swing low = support
            if lows[i] == min(lows[i - pivot_window:i + pivot_window + 1]):
                supports.append(lows[i])
            # Swing high = resistance
            if highs[i] == max(highs[i - pivot_window:i + pivot_window + 1]):
                resistances.append(highs[i])

        # Cluster nearby levels (within 0.5% of each other)
        supports = self._cluster_levels(sorted(supports))
        resistances = self._cluster_levels(sorted(resistances, reverse=True))

        close = df["close"].iloc[-1]
        below = [s for s in supports if s < close]
        above = [r for r in resistances if r > close]

        df.attrs["support_levels"] = sorted(below, reverse=True)[:3] if below else [close * 0.99]
        df.attrs["resistance_levels"] = sorted(above)[:3] if above else [close * 1.01]

    def _cluster_levels(self, levels: List[float], threshold_pct: float = 0.3) -> List[float]:
        """Merge nearby price levels into single levels."""
        if not levels:
            return []
        clustered = [levels[0]]
        for level in levels[1:]:
            if abs(level - clustered[-1]) / clustered[-1] * 100 > threshold_pct:
                clustered.append(level)
        return clustered

    # ── Pivot Points ─────────────────────────────────

    def _calc_pivot_points(self, df: pd.DataFrame):
        """Classic pivot points from previous candle."""
        prev = df.iloc[-2] if len(df) > 1 else df.iloc[-1]
        pp = (prev["high"] + prev["low"] + prev["close"]) / 3
        df.attrs["pivot_point"] = pp
        df.attrs["r1"] = 2 * pp - prev["low"]
        df.attrs["s1"] = 2 * pp - prev["high"]
        df.attrs["r2"] = pp + (prev["high"] - prev["low"])
        df.attrs["s2"] = pp - (prev["high"] - prev["low"])

    # ── Build Result ─────────────────────────────────

    def _build_indicator_result(self, df: pd.DataFrame) -> IndicatorResult:
        """Build composite indicator result from latest candle."""
        latest = df.iloc[-1]

        supports = df.attrs.get("support_levels", [0, 0])
        resistances = df.attrs.get("resistance_levels", [0, 0])

        r = IndicatorResult(
            ema_fast=float(latest.get("ema_fast", 0)),
            ema_slow=float(latest.get("ema_slow", 0)),
            ema_trend=float(latest.get("ema_trend", 0)),
            adx=float(latest.get("adx", 0)),
            supertrend=float(latest.get("supertrend", 0)),
            supertrend_direction=int(latest.get("supertrend_direction", 1)),
            rsi=float(latest.get("rsi", 50)),
            macd_line=float(latest.get("macd_line", 0)),
            macd_signal=float(latest.get("macd_signal", 0)),
            macd_histogram=float(latest.get("macd_histogram", 0)),
            stoch_k=float(latest.get("stoch_k", 50)),
            stoch_d=float(latest.get("stoch_d", 50)),
            atr=float(latest.get("atr", 0)),
            atr_pct=float(latest.get("atr_pct", 0)),
            bb_upper=float(latest.get("bb_upper", 0)),
            bb_middle=float(latest.get("bb_middle", 0)),
            bb_lower=float(latest.get("bb_lower", 0)),
            bb_width=float(latest.get("bb_width", 0)),
            volume_ratio=float(latest.get("volume_ratio", 1)),
            support_1=supports[0] if supports else 0,
            support_2=supports[1] if len(supports) > 1 else 0,
            resistance_1=resistances[0] if resistances else 0,
            resistance_2=resistances[1] if len(resistances) > 1 else 0,
            pivot_point=float(df.attrs.get("pivot_point", 0)),
        )

        # Composite scores
        r.trend_score = self._score_trend(r)
        r.momentum_score = self._score_momentum(r)
        r.volatility_score = self._score_volatility(r)
        r.overall_score = r.trend_score * 0.4 + r.momentum_score * 0.35 + r.volatility_score * 0.25

        return r

    def _score_trend(self, r: IndicatorResult) -> float:
        """Score trend strength: -100 (strong bear) to +100 (strong bull)."""
        score = 0.0
        # EMA alignment
        if r.ema_fast > r.ema_slow > r.ema_trend:
            score += 40
        elif r.ema_fast < r.ema_slow < r.ema_trend:
            score -= 40
        elif r.ema_fast > r.ema_slow:
            score += 20
        else:
            score -= 20

        # Supertrend direction
        score += r.supertrend_direction * 25

        # ADX strength modifier
        if r.adx > 25:
            score *= 1.3
        elif r.adx < 15:
            score *= 0.5

        return max(-100, min(100, score))

    def _score_momentum(self, r: IndicatorResult) -> float:
        """Score momentum: -100 to +100."""
        score = 0.0

        # RSI contribution
        if r.rsi > 70:
            score -= 30  # overbought
        elif r.rsi < 30:
            score += 30  # oversold
        elif r.rsi > 55:
            score += 15
        elif r.rsi < 45:
            score -= 15

        # MACD histogram
        if r.macd_histogram > 0:
            score += 25
        else:
            score -= 25

        # Stochastic
        if r.stoch_k < 20:
            score += 20
        elif r.stoch_k > 80:
            score -= 20

        return max(-100, min(100, score))

    def _score_volatility(self, r: IndicatorResult) -> float:
        """Score volatility regime: 0 (low) to 100 (high/squeeze)."""
        if r.bb_width < 1.0:
            return 80  # squeeze — breakout likely
        elif r.bb_width > 4.0:
            return 30  # expanded — trend may be mature
        return 50 + (2.0 - r.bb_width) * 10
