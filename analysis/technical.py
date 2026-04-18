"""
analysis/technical.py - Professional Technical Analysis Engine
Calculates EMAs, RSI, MACD, ATR, Bollinger Bands, Stochastic, Pivots, Volume
"""
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class TechnicalAnalysis:
    """Full-featured TA engine. All methods operate in-place on a DataFrame."""

    # ─── Moving Averages ───────────────────────────────────────────────────────
    @staticmethod
    def add_ema(df: pd.DataFrame, periods: list[int] = [9, 21, 50, 200]) -> pd.DataFrame:
        for p in periods:
            df[f"ema_{p}"] = df["close"].ewm(span=p, adjust=False).mean()
        return df

    @staticmethod
    def add_sma(df: pd.DataFrame, periods: list[int] = [20, 50, 200]) -> pd.DataFrame:
        for p in periods:
            df[f"sma_{p}"] = df["close"].rolling(p).mean()
        return df

    # ─── RSI ───────────────────────────────────────────────────────────────────
    @staticmethod
    def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        delta = df["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
        avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df[f"rsi_{period}"] = 100 - (100 / (1 + rs))
        return df

    # ─── MACD ─────────────────────────────────────────────────────────────────
    @staticmethod
    def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
        df["macd"] = ema_fast - ema_slow
        df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]
        return df

    # ─── ATR ──────────────────────────────────────────────────────────────────
    @staticmethod
    def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift(1)).abs()
        lc = (df["low"] - df["close"].shift(1)).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        df[f"atr_{period}"] = tr.ewm(com=period - 1, adjust=False).mean()
        return df

    # ─── Bollinger Bands ──────────────────────────────────────────────────────
    @staticmethod
    def add_bollinger(df: pd.DataFrame, period: int = 20, std: float = 2.0) -> pd.DataFrame:
        df["bb_mid"] = df["close"].rolling(period).mean()
        rolling_std = df["close"].rolling(period).std()
        df["bb_upper"] = df["bb_mid"] + std * rolling_std
        df["bb_lower"] = df["bb_mid"] - std * rolling_std
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
        df["bb_pct"] = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])
        return df

    # ─── Stochastic ───────────────────────────────────────────────────────────
    @staticmethod
    def add_stochastic(df: pd.DataFrame, k_period: int = 14,
                        d_period: int = 3, smooth: int = 3) -> pd.DataFrame:
        low_min = df["low"].rolling(k_period).min()
        high_max = df["high"].rolling(k_period).max()
        raw_k = 100 * (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan)
        df["stoch_k"] = raw_k.rolling(smooth).mean()
        df["stoch_d"] = df["stoch_k"].rolling(d_period).mean()
        return df

    # ─── Volume ───────────────────────────────────────────────────────────────
    @staticmethod
    def add_volume_indicators(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        df["vol_sma"] = df["volume"].rolling(period).mean()
        df["vol_ratio"] = df["volume"] / df["vol_sma"].replace(0, np.nan)
        df["vol_spike"] = df["vol_ratio"] > 2.0
        # On-Balance Volume
        obv = [0.0]
        for i in range(1, len(df)):
            if df["close"].iloc[i] > df["close"].iloc[i - 1]:
                obv.append(obv[-1] + df["volume"].iloc[i])
            elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
                obv.append(obv[-1] - df["volume"].iloc[i])
            else:
                obv.append(obv[-1])
        df["obv"] = obv
        df["obv_ema"] = df["obv"].ewm(span=20, adjust=False).mean()
        return df

    # ─── Pivot Points (Traditional) ───────────────────────────────────────────
    @staticmethod
    def add_pivots(df: pd.DataFrame) -> pd.DataFrame:
        """Add daily pivot points to each bar."""
        prev_high = df["high"].shift(1)
        prev_low = df["low"].shift(1)
        prev_close = df["close"].shift(1)
        df["pivot"] = (prev_high + prev_low + prev_close) / 3
        df["r1"] = 2 * df["pivot"] - prev_low
        df["s1"] = 2 * df["pivot"] - prev_high
        df["r2"] = df["pivot"] + (prev_high - prev_low)
        df["s2"] = df["pivot"] - (prev_high - prev_low)
        df["r3"] = prev_high + 2 * (df["pivot"] - prev_low)
        df["s3"] = prev_low - 2 * (prev_high - df["pivot"])
        return df

    # ─── Swing Highs / Lows ───────────────────────────────────────────────────
    @staticmethod
    def add_swings(df: pd.DataFrame, lookback: int = 3) -> pd.DataFrame:
        """Mark swing highs and lows using rolling window."""
        swing_high = []
        swing_low = []
        n = len(df)
        for i in range(n):
            if i < lookback or i >= n - lookback:
                swing_high.append(False)
                swing_low.append(False)
                continue
            window_h = df["high"].iloc[i - lookback: i + lookback + 1]
            window_l = df["low"].iloc[i - lookback: i + lookback + 1]
            is_sh = df["high"].iloc[i] == window_h.max()
            is_sl = df["low"].iloc[i] == window_l.min()
            swing_high.append(is_sh)
            swing_low.append(is_sl)
        df["swing_high"] = swing_high
        df["swing_low"] = swing_low
        return df

    # ─── Trend Determination ─────────────────────────────────────────────────
    @staticmethod
    def determine_trend(df: pd.DataFrame) -> str:
        """Returns 'bullish', 'bearish', or 'ranging' based on EMAs."""
        if "ema_21" not in df.columns or "ema_50" not in df.columns:
            return "unknown"
        last = df.iloc[-1]
        close = last["close"]
        e21 = last.get("ema_21", np.nan)
        e50 = last.get("ema_50", np.nan)
        e200 = last.get("ema_200", np.nan)

        if pd.isna(e21) or pd.isna(e50):
            return "unknown"

        bull_score = 0
        bear_score = 0

        if close > e21:
            bull_score += 1
        else:
            bear_score += 1
        if close > e50:
            bull_score += 1
        else:
            bear_score += 1
        if not pd.isna(e200):
            if close > e200:
                bull_score += 2
            else:
                bear_score += 2
        if e21 > e50:
            bull_score += 1
        else:
            bear_score += 1

        total = bull_score + bear_score
        if total == 0:
            return "ranging"
        ratio = bull_score / total
        if ratio >= 0.65:
            return "bullish"
        elif ratio <= 0.35:
            return "bearish"
        else:
            return "ranging"

    # ─── RSI Divergence ───────────────────────────────────────────────────────
    @staticmethod
    def detect_rsi_divergence(df: pd.DataFrame, lookback: int = 20,
                               rsi_col: str = "rsi_14") -> str:
        """
        Detect RSI divergence over last `lookback` bars.
        Returns: 'bullish_div' | 'bearish_div' | 'hidden_bull_div' | 'hidden_bear_div' | 'none'
        """
        if rsi_col not in df.columns or len(df) < lookback:
            return "none"

        recent = df.tail(lookback)
        price_high_idx = recent["close"].idxmax()
        price_low_idx = recent["close"].idxmin()
        rsi_high_idx = recent[rsi_col].idxmax()
        rsi_low_idx = recent[rsi_col].idxmin()

        # Regular bearish divergence: price higher high, RSI lower high
        price_last = recent["close"].iloc[-1]
        price_prev_high = recent["close"].iloc[:-5].max() if len(recent) > 5 else np.nan
        rsi_last = recent[rsi_col].iloc[-1]
        rsi_prev_high = recent[rsi_col].iloc[:-5].max() if len(recent) > 5 else np.nan

        if not pd.isna(price_prev_high) and not pd.isna(rsi_prev_high):
            # Bearish divergence
            if price_last > price_prev_high and rsi_last < rsi_prev_high:
                return "bearish_div"
            # Bullish divergence
            price_prev_low = recent["close"].iloc[:-5].min() if len(recent) > 5 else np.nan
            rsi_prev_low = recent[rsi_col].iloc[:-5].min() if len(recent) > 5 else np.nan
            if not pd.isna(price_prev_low) and not pd.isna(rsi_prev_low):
                if price_last < price_prev_low and rsi_last > rsi_prev_low:
                    return "bullish_div"

        return "none"

    # ─── Add All Indicators ───────────────────────────────────────────────────
    @classmethod
    def add_all(cls, df: pd.DataFrame) -> pd.DataFrame:
        """One-call convenience: adds all indicators to the DataFrame."""
        df = cls.add_ema(df, [9, 21, 50, 200])
        df = cls.add_sma(df, [20, 50])
        df = cls.add_rsi(df, 14)
        df = cls.add_macd(df)
        df = cls.add_atr(df, 14)
        df = cls.add_bollinger(df, 20, 2.0)
        df = cls.add_stochastic(df)
        df = cls.add_volume_indicators(df)
        df = cls.add_pivots(df)
        df = cls.add_swings(df, lookback=3)
        return df

    # ─── Summary Row ─────────────────────────────────────────────────────────
    @classmethod
    def summary(cls, df: pd.DataFrame) -> dict:
        """Return a dict of key indicator values from the last bar."""
        if df is None or df.empty:
            return {}
        df = cls.add_all(df)
        last = df.iloc[-1]
        trend = cls.determine_trend(df)
        divergence = cls.detect_rsi_divergence(df)

        return {
            "close":      round(float(last["close"]), 4),
            "trend":      trend,
            "rsi":        round(float(last.get("rsi_14", 0)), 2),
            "macd":       round(float(last.get("macd", 0)), 4),
            "macd_hist":  round(float(last.get("macd_hist", 0)), 4),
            "atr":        round(float(last.get("atr_14", 0)), 4),
            "bb_pct":     round(float(last.get("bb_pct", 0.5)), 3),
            "bb_width":   round(float(last.get("bb_width", 0)), 4),
            "stoch_k":    round(float(last.get("stoch_k", 50)), 2),
            "stoch_d":    round(float(last.get("stoch_d", 50)), 2),
            "ema_21":     round(float(last.get("ema_21", 0)), 4),
            "ema_50":     round(float(last.get("ema_50", 0)), 4),
            "ema_200":    round(float(last.get("ema_200", 0)), 4),
            "vol_ratio":  round(float(last.get("vol_ratio", 1)), 2),
            "divergence": divergence,
            "pivot":      round(float(last.get("pivot", 0)), 4),
            "r1":         round(float(last.get("r1", 0)), 4),
            "s1":         round(float(last.get("s1", 0)), 4),
        }


ta = TechnicalAnalysis()
