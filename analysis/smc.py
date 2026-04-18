"""
analysis/smc.py - Smart Money Concepts (SMC) Analysis Engine
Detects: Order Blocks, Fair Value Gaps, Break of Structure, 
         Change of Character, Liquidity Sweeps, Equal Highs/Lows
"""
import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class OrderBlock:
    direction: str         # 'bullish' | 'bearish'
    high: float
    low: float
    open: float
    close: float
    index: int
    bar_time: any
    strength: float        # 0-1 score based on subsequent impulse
    tested: bool = False   # True if price has revisited the OB zone
    broken: bool = False   # True if price has broken through the OB


@dataclass
class FairValueGap:
    direction: str         # 'bullish' | 'bearish'
    upper: float           # top of the gap
    lower: float           # bottom of the gap
    index: int
    bar_time: any
    filled: bool = False


@dataclass
class StructureLevel:
    level_type: str        # 'BOS' | 'ChoCH'
    direction: str         # 'bullish' | 'bearish'
    price: float
    index: int
    bar_time: any


@dataclass
class LiquidityPool:
    """Represents a cluster of stop-losses / equal highs/lows where stops accumulate."""
    pool_type: str             # 'equal_highs' | 'equal_lows' | 'swing_cluster' | 'session_extreme'
    price: float               # the key price level
    strength: float            # 0-1 how many touches / confluence
    side: str                  # 'buy_side' (above price) | 'sell_side' (below price)
    touch_count: int = 1       # number of times price tested this level
    distance_atr: float = 0.0  # distance from current price in ATR units


@dataclass
class SMCContext:
    order_blocks: list = field(default_factory=list)
    fair_value_gaps: list = field(default_factory=list)
    structure_levels: list = field(default_factory=list)
    liquidity_levels: list = field(default_factory=list)
    liquidity_pools: list = field(default_factory=list)        # Tiger Hunter pools
    current_trend: str = "ranging"
    nearest_ob: Optional[OrderBlock] = None
    nearest_fvg: Optional[FairValueGap] = None
    recent_bos: Optional[StructureLevel] = None
    bias: str = "neutral"            # 'long' | 'short' | 'neutral'
    confidence: float = 0.0


class SMCAnalyzer:
    """
    Smart Money Concepts analyzer for institutional order flow.
    Implements ICT / SMC methodology.
    """

    def __init__(self, impulse_multiplier: float = 1.5,
                 ob_lookback: int = 5):
        self.impulse_multiplier = impulse_multiplier
        self.ob_lookback = ob_lookback

    # ─── Order Block Detection ────────────────────────────────────────────────
    def find_order_blocks(self, df: pd.DataFrame,
                          lookback: int = 50) -> list[OrderBlock]:
        """
        Find Order Blocks in the last `lookback` bars.
        A bullish OB = the last bearish candle before a strong bullish impulse.
        A bearish OB = the last bullish candle before a strong bearish impulse.
        """
        obs = []
        data = df.tail(lookback + self.ob_lookback).reset_index(drop=False)
        atr = df["close"].diff().abs().rolling(14).mean().iloc[-1]
        if pd.isna(atr) or atr == 0:
            atr = (df["high"] - df["low"]).mean()

        n = len(data)
        for i in range(1, n - self.ob_lookback):
            candle = data.iloc[i]
            c_open = float(candle["open"])
            c_close = float(candle["close"])
            c_high = float(candle["high"])
            c_low = float(candle["low"])

            # Look ahead for impulse
            forward = data.iloc[i + 1: i + 1 + self.ob_lookback]
            if forward.empty:
                continue

            forward_range = float(forward["high"].max() - forward["low"].min())

            # Bullish OB: bearish candle followed by bullish impulse
            if c_close < c_open:  # bearish candle
                fwd_move = float(forward["close"].iloc[-1]) - c_close
                if fwd_move > self.impulse_multiplier * atr:
                    strength = min(1.0, fwd_move / (self.impulse_multiplier * atr * 3))
                    current_price = float(df["close"].iloc[-1])
                    tested = c_low <= current_price <= c_high
                    obs.append(OrderBlock(
                        direction="bullish",
                        high=c_high,
                        low=c_low,
                        open=c_open,
                        close=c_close,
                        index=i,
                        bar_time=candle.get("timestamp", i),
                        strength=round(strength, 3),
                        tested=tested,
                        broken=current_price < c_low,
                    ))

            # Bearish OB: bullish candle followed by bearish impulse
            if c_close > c_open:  # bullish candle
                fwd_move = c_close - float(forward["close"].iloc[-1])
                if fwd_move > self.impulse_multiplier * atr:
                    strength = min(1.0, fwd_move / (self.impulse_multiplier * atr * 3))
                    current_price = float(df["close"].iloc[-1])
                    tested = c_low <= current_price <= c_high
                    obs.append(OrderBlock(
                        direction="bearish",
                        high=c_high,
                        low=c_low,
                        open=c_open,
                        close=c_close,
                        index=i,
                        bar_time=candle.get("timestamp", i),
                        strength=round(strength, 3),
                        tested=tested,
                        broken=current_price > c_high,
                    ))

        # Return non-broken OBs sorted by strength
        valid = [ob for ob in obs if not ob.broken]
        valid.sort(key=lambda x: x.strength, reverse=True)
        return valid[:10]

    # ─── Fair Value Gap Detection ─────────────────────────────────────────────
    def find_fvg(self, df: pd.DataFrame, lookback: int = 50) -> list[FairValueGap]:
        """
        Fair Value Gaps: 3-candle imbalance zones.
        Bullish FVG: candle[i-1].high < candle[i+1].low
        Bearish FVG: candle[i-1].low > candle[i+1].high
        """
        fvgs = []
        data = df.tail(lookback + 2).reset_index(drop=False)
        current_price = float(df["close"].iloc[-1])
        n = len(data)

        for i in range(1, n - 1):
            prev = data.iloc[i - 1]
            curr = data.iloc[i]
            nxt = data.iloc[i + 1]

            prev_high = float(prev["high"])
            prev_low = float(prev["low"])
            nxt_high = float(nxt["high"])
            nxt_low = float(nxt["low"])

            # Bullish FVG
            if prev_high < nxt_low:
                gap_size = nxt_low - prev_high
                filled = prev_high <= current_price <= nxt_low
                fvgs.append(FairValueGap(
                    direction="bullish",
                    upper=nxt_low,
                    lower=prev_high,
                    index=i,
                    bar_time=curr.get("timestamp", i),
                    filled=filled,
                ))

            # Bearish FVG
            elif prev_low > nxt_high:
                filled = nxt_high <= current_price <= prev_low
                fvgs.append(FairValueGap(
                    direction="bearish",
                    upper=prev_low,
                    lower=nxt_high,
                    index=i,
                    bar_time=curr.get("timestamp", i),
                    filled=filled,
                ))

        # Return unfilled FVGs
        unfilled = [fvg for fvg in fvgs if not fvg.filled]
        # Sort by recency (closest to current bar)
        unfilled.sort(key=lambda x: x.index, reverse=True)
        return unfilled[:8]

    # ─── Break of Structure ───────────────────────────────────────────────────
    def find_bos_choch(self, df: pd.DataFrame,
                       lookback: int = 50) -> list[StructureLevel]:
        """
        Detect Break of Structure (BOS) and Change of Character (ChoCH).
        BOS: continuation of trend structure.
        ChoCH: reversal of trend structure.
        """
        if "swing_high" not in df.columns or "swing_low" not in df.columns:
            return []

        levels = []
        data = df.tail(lookback).reset_index(drop=False)

        prev_trend = "unknown"
        prev_highs = []
        prev_lows = []

        for i in range(len(data)):
            row = data.iloc[i]
            close = float(row["close"])
            high = float(row["high"])
            low = float(row["low"])
            is_sh = bool(row.get("swing_high", False))
            is_sl = bool(row.get("swing_low", False))

            if is_sh:
                prev_highs.append(high)
            if is_sl:
                prev_lows.append(low)

            # Check for breaks
            if len(prev_highs) >= 2 and len(prev_lows) >= 2:
                last_high = prev_highs[-2]
                last_low = prev_lows[-2]

                # Break above previous high
                if close > last_high:
                    level_type = "BOS" if prev_trend == "bullish" else "ChoCH"
                    levels.append(StructureLevel(
                        level_type=level_type,
                        direction="bullish",
                        price=last_high,
                        index=i,
                        bar_time=row.get("timestamp", i),
                    ))
                    prev_trend = "bullish"

                # Break below previous low
                elif close < last_low:
                    level_type = "BOS" if prev_trend == "bearish" else "ChoCH"
                    levels.append(StructureLevel(
                        level_type=level_type,
                        direction="bearish",
                        price=last_low,
                        index=i,
                        bar_time=row.get("timestamp", i),
                    ))
                    prev_trend = "bearish"

        # Return most recent structures
        return levels[-5:] if levels else []

    # ─── Liquidity Levels ─────────────────────────────────────────────────────
    def find_liquidity_levels(self, df: pd.DataFrame,
                               lookback: int = 100) -> list[dict]:
        """
        Identify key liquidity zones:
        - Equal highs/lows (within 0.1% of each other)
        - Previous day/week H/L
        - Round number levels
        """
        levels = []
        data = df.tail(lookback)
        current_price = float(df["close"].iloc[-1])

        # Previous day H/L
        day_data = data.resample("1D") if data.index.freq is None else data
        try:
            prev_day_high = float(data["high"].tail(24).max())
            prev_day_low = float(data["low"].tail(24).min())
            levels.append({"type": "prev_day_high", "price": prev_day_high,
                           "distance_pct": abs(current_price - prev_day_high) / current_price * 100})
            levels.append({"type": "prev_day_low", "price": prev_day_low,
                           "distance_pct": abs(current_price - prev_day_low) / current_price * 100})
        except Exception:
            pass

        # Previous week H/L
        prev_week_high = float(data["high"].max())
        prev_week_low = float(data["low"].min())
        levels.append({"type": "prev_week_high", "price": prev_week_high,
                       "distance_pct": abs(current_price - prev_week_high) / current_price * 100})
        levels.append({"type": "prev_week_low", "price": prev_week_low,
                       "distance_pct": abs(current_price - prev_week_low) / current_price * 100})

        # Round numbers (psychological levels)
        magnitude = 10 ** (len(str(int(current_price))) - 2)
        nearest_round = round(current_price / magnitude) * magnitude
        for mult in [-2, -1, 0, 1, 2]:
            lvl = nearest_round + mult * magnitude
            levels.append({
                "type": "psychological",
                "price": lvl,
                "distance_pct": abs(current_price - lvl) / current_price * 100,
            })

        # Equal highs (sell-side liquidity above)
        swing_highs = data[data.get("swing_high", pd.Series(dtype=bool))]["high"].tolist() \
            if "swing_high" in data.columns else []
        for i, h1 in enumerate(swing_highs):
            for h2 in swing_highs[i + 1:]:
                if abs(h1 - h2) / h1 < 0.001:  # within 0.1%
                    levels.append({
                        "type": "equal_highs",
                        "price": (h1 + h2) / 2,
                        "distance_pct": abs(current_price - h1) / current_price * 100,
                    })
                    break

        levels.sort(key=lambda x: x["distance_pct"])
        return levels[:10]

    # ─── Tiger Hunter: Liquidity Pool Detection ──────────────────────────────
    def detect_liquidity_pools(self, df: pd.DataFrame,
                                lookback: int = 100) -> list[LiquidityPool]:
        """
        Tiger Hunter: Map where stop-losses accumulate.
        Detects:
        1. Equal highs (buy-side liquidity — shorts' SLs sit above)
        2. Equal lows (sell-side liquidity — longs' SLs sit below)
        3. Swing clusters (multiple swing points at similar levels)
        4. Session extremes (Asian range H/L etc.)
        """
        pools: list[LiquidityPool] = []
        data = df.tail(lookback)
        current_price = float(df["close"].iloc[-1])
        atr = float(df["close"].diff().abs().rolling(14).mean().iloc[-1])
        if pd.isna(atr) or atr <= 0:
            atr = float((df["high"] - df["low"]).mean())
        if atr <= 0:
            return pools

        # --- Equal Highs Detection (Buy-side liquidity above) ---
        highs = data["high"].values
        eq_high_tolerance = current_price * 0.0015  # 0.15% tolerance
        high_clusters: dict[float, int] = {}
        for i in range(len(highs)):
            h = float(highs[i])
            if not np.isfinite(h) or h <= 0:
                continue
            matched = False
            for key in list(high_clusters.keys()):
                if abs(h - key) <= eq_high_tolerance:
                    high_clusters[key] += 1
                    matched = True
                    break
            if not matched:
                high_clusters[h] = 1

        for level, count in high_clusters.items():
            if count >= 2:  # at least 2 touches = equal highs
                dist = abs(current_price - level)
                pools.append(LiquidityPool(
                    pool_type="equal_highs",
                    price=level,
                    strength=min(1.0, count / 5.0),
                    side="buy_side",
                    touch_count=count,
                    distance_atr=round(dist / atr, 2) if atr > 0 else 0.0,
                ))

        # --- Equal Lows Detection (Sell-side liquidity below) ---
        lows = data["low"].values
        eq_low_tolerance = current_price * 0.0015
        low_clusters: dict[float, int] = {}
        for i in range(len(lows)):
            lo = float(lows[i])
            if not np.isfinite(lo) or lo <= 0:
                continue
            matched = False
            for key in list(low_clusters.keys()):
                if abs(lo - key) <= eq_low_tolerance:
                    low_clusters[key] += 1
                    matched = True
                    break
            if not matched:
                low_clusters[lo] = 1

        for level, count in low_clusters.items():
            if count >= 2:
                dist = abs(current_price - level)
                pools.append(LiquidityPool(
                    pool_type="equal_lows",
                    price=level,
                    strength=min(1.0, count / 5.0),
                    side="sell_side",
                    touch_count=count,
                    distance_atr=round(dist / atr, 2) if atr > 0 else 0.0,
                ))

        # --- Swing Cluster Detection ---
        if "swing_high" in data.columns:
            swing_highs_df = data[data["swing_high"] == True]
            if len(swing_highs_df) >= 2:
                sh_vals = swing_highs_df["high"].values
                for i in range(len(sh_vals)):
                    cluster_count = sum(
                        1 for j in range(len(sh_vals))
                        if i != j and abs(float(sh_vals[i]) - float(sh_vals[j])) <= eq_high_tolerance
                    )
                    if cluster_count >= 1:
                        level = float(sh_vals[i])
                        dist = abs(current_price - level)
                        pools.append(LiquidityPool(
                            pool_type="swing_cluster",
                            price=level,
                            strength=min(1.0, (cluster_count + 1) / 4.0),
                            side="buy_side" if level > current_price else "sell_side",
                            touch_count=cluster_count + 1,
                            distance_atr=round(dist / atr, 2) if atr > 0 else 0.0,
                        ))

        if "swing_low" in data.columns:
            swing_lows_df = data[data["swing_low"] == True]
            if len(swing_lows_df) >= 2:
                sl_vals = swing_lows_df["low"].values
                for i in range(len(sl_vals)):
                    cluster_count = sum(
                        1 for j in range(len(sl_vals))
                        if i != j and abs(float(sl_vals[i]) - float(sl_vals[j])) <= eq_low_tolerance
                    )
                    if cluster_count >= 1:
                        level = float(sl_vals[i])
                        dist = abs(current_price - level)
                        pools.append(LiquidityPool(
                            pool_type="swing_cluster",
                            price=level,
                            strength=min(1.0, (cluster_count + 1) / 4.0),
                            side="buy_side" if level > current_price else "sell_side",
                            touch_count=cluster_count + 1,
                            distance_atr=round(dist / atr, 2) if atr > 0 else 0.0,
                        ))

        # Deduplicate pools that are very close to each other
        deduped: list[LiquidityPool] = []
        for pool in sorted(pools, key=lambda p: p.strength, reverse=True):
            is_dup = any(
                abs(pool.price - existing.price) <= eq_high_tolerance
                for existing in deduped
            )
            if not is_dup:
                deduped.append(pool)

        # Sort by proximity to current price
        deduped.sort(key=lambda p: p.distance_atr)
        return deduped[:15]

    # ─── Tiger Hunter: Anti-Sweep Stop Loss ──────────────────────────────────
    def anti_sweep_sl(
        self,
        entry: float,
        direction: str,
        liquidity_pools: list[LiquidityPool],
        atr: float,
        ob: Optional[OrderBlock] = None,
    ) -> tuple[float, str]:
        """
        Tiger Hunter: Place SL BEHIND liquidity pools, not AT obvious levels.

        For LONG: find sell-side liquidity below entry, place SL behind it.
        For SHORT: find buy-side liquidity above entry, place SL behind it.

        Returns (sl_price, sl_reason).
        """
        entry = float(entry)
        atr = float(atr)
        buffer = atr * 0.25  # buffer zone beyond liquidity

        # Default: 1.5× ATR (fallback)
        if direction == "long":
            default_sl = entry - 1.5 * atr
        else:
            default_sl = entry + 1.5 * atr

        if not liquidity_pools:
            # Try OB-based SL as fallback
            if ob and direction == "long" and ob.direction == "bullish":
                ob_sl = float(ob.low) - buffer
                return max(ob_sl, default_sl), "🛡️ SL behind bullish OB"
            elif ob and direction == "short" and ob.direction == "bearish":
                ob_sl = float(ob.high) + buffer
                return min(ob_sl, default_sl), "🛡️ SL behind bearish OB"
            return default_sl, "SL: 1.5× ATR (no liquidity mapped)"

        if direction == "long":
            # Find sell-side pools below entry (within reasonable range)
            sell_pools = [
                p for p in liquidity_pools
                if p.side == "sell_side"
                and p.price < entry
                and p.distance_atr <= 3.0  # within 3 ATR
            ]
            if sell_pools:
                # Pick the strongest nearby pool
                best_pool = max(sell_pools, key=lambda p: p.strength)
                sl = best_pool.price - buffer  # BEHIND the pool
                # Don't let SL be worse than default
                sl = max(sl, default_sl)
                # Minimum risk: at least 0.5 ATR
                if entry - sl < 0.5 * atr:
                    sl = entry - 0.5 * atr
                reason = (
                    f"🛡️ Anti-sweep SL behind {best_pool.pool_type} "
                    f"({best_pool.touch_count} touches)"
                )
                return sl, reason

            # Fallback: OB-based
            if ob and ob.direction == "bullish":
                ob_sl = float(ob.low) - buffer
                return max(ob_sl, default_sl), "🛡️ SL behind bullish OB"

        else:  # short
            buy_pools = [
                p for p in liquidity_pools
                if p.side == "buy_side"
                and p.price > entry
                and p.distance_atr <= 3.0
            ]
            if buy_pools:
                best_pool = max(buy_pools, key=lambda p: p.strength)
                sl = best_pool.price + buffer
                sl = min(sl, default_sl)
                if sl - entry < 0.5 * atr:
                    sl = entry + 0.5 * atr
                reason = (
                    f"🛡️ Anti-sweep SL behind {best_pool.pool_type} "
                    f"({best_pool.touch_count} touches)"
                )
                return sl, reason

            if ob and ob.direction == "bearish":
                ob_sl = float(ob.high) + buffer
                return min(ob_sl, default_sl), "🛡️ SL behind bearish OB"

        return default_sl, "SL: 1.5× ATR (no suitable pools)"

    # ─── Tiger Hunter: Liquidity TP Targets ──────────────────────────────────
    def liquidity_tp_targets(
        self,
        entry: float,
        direction: str,
        atr: float,
        liquidity_pools: list[LiquidityPool],
        fvgs: list[FairValueGap] = None,
    ) -> tuple[list[float], str]:
        """
        Tiger Hunter: Target TP at OPPOSING liquidity pools.

        For LONG: target buy-side liquidity above (where shorts' SLs sit).
        For SHORT: target sell-side liquidity below (where longs' SLs sit).

        Returns (tp_levels, tp_reason).
        """
        entry = float(entry)
        atr = float(atr)
        targets: list[tuple[float, str]] = []

        if direction == "long":
            # Target buy-side liquidity above entry
            buy_pools = sorted(
                [p for p in liquidity_pools
                 if p.side == "buy_side" and p.price > entry],
                key=lambda p: p.price,
            )
            for pool in buy_pools[:3]:
                targets.append((pool.price, f"Liquidity: {pool.pool_type}"))

            # Also target unfilled bearish FVGs above
            if fvgs:
                for fvg in fvgs:
                    if fvg.direction == "bearish" and float(fvg.lower) > entry:
                        targets.append((float(fvg.lower), "FVG fill"))

        else:  # short
            sell_pools = sorted(
                [p for p in liquidity_pools
                 if p.side == "sell_side" and p.price < entry],
                key=lambda p: p.price,
                reverse=True,
            )
            for pool in sell_pools[:3]:
                targets.append((pool.price, f"Liquidity: {pool.pool_type}"))

            if fvgs:
                for fvg in fvgs:
                    if fvg.direction == "bullish" and float(fvg.upper) < entry:
                        targets.append((float(fvg.upper), "FVG fill"))

        if not targets:
            # Fallback: mechanical R:R targets
            return [], "TP: mechanical R:R (no opposing liquidity)"

        # Sort by distance and pick up to 3
        if direction == "long":
            targets.sort(key=lambda t: t[0])
        else:
            targets.sort(key=lambda t: t[0], reverse=True)

        tp_levels = [t[0] for t in targets[:3]]
        tp_sources = [t[1] for t in targets[:3]]
        reason = f"⚡ TP at opposing liquidity: {', '.join(tp_sources[:2])}"
        return tp_levels, reason

    # ─── Full SMC Context ─────────────────────────────────────────────────────
    def analyze(self, df: pd.DataFrame) -> SMCContext:
        """Run full SMC analysis and return a structured context object."""
        ctx = SMCContext()

        try:
            ctx.order_blocks = self.find_order_blocks(df)
            ctx.fair_value_gaps = self.find_fvg(df)
            ctx.structure_levels = self.find_bos_choch(df)
            ctx.liquidity_levels = self.find_liquidity_levels(df)
            ctx.liquidity_pools = self.detect_liquidity_pools(df)

            current_price = float(df["close"].iloc[-1])

            # Find nearest OB above and below price
            obs_below = [ob for ob in ctx.order_blocks
                         if ob.direction == "bullish" and ob.high <= current_price * 1.005]
            obs_above = [ob for ob in ctx.order_blocks
                         if ob.direction == "bearish" and ob.low >= current_price * 0.995]

            if obs_below:
                ctx.nearest_ob = max(obs_below, key=lambda x: x.high)
            elif obs_above:
                ctx.nearest_ob = min(obs_above, key=lambda x: x.low)

            # Find nearest FVG
            fvgs_below = [fvg for fvg in ctx.fair_value_gaps
                          if fvg.upper <= current_price * 1.005]
            fvgs_above = [fvg for fvg in ctx.fair_value_gaps
                          if fvg.lower >= current_price * 0.995]
            if fvgs_below:
                ctx.nearest_fvg = max(fvgs_below, key=lambda x: x.upper)
            elif fvgs_above:
                ctx.nearest_fvg = min(fvgs_above, key=lambda x: x.lower)

            # Recent structure
            if ctx.structure_levels:
                ctx.recent_bos = ctx.structure_levels[-1]
                if ctx.recent_bos.direction == "bullish":
                    ctx.current_trend = "bullish"
                else:
                    ctx.current_trend = "bearish"

            # Determine bias
            bull_factors = 0
            bear_factors = 0

            if ctx.current_trend == "bullish":
                bull_factors += 2
            elif ctx.current_trend == "bearish":
                bear_factors += 2

            if ctx.nearest_ob and ctx.nearest_ob.direction == "bullish":
                bull_factors += 1
            if ctx.nearest_ob and ctx.nearest_ob.direction == "bearish":
                bear_factors += 1

            if ctx.nearest_fvg and ctx.nearest_fvg.direction == "bullish":
                bull_factors += 1
            if ctx.nearest_fvg and ctx.nearest_fvg.direction == "bearish":
                bear_factors += 1

            if bull_factors > bear_factors:
                ctx.bias = "long"
                ctx.confidence = round(bull_factors / max(bull_factors + bear_factors, 1), 2)
            elif bear_factors > bull_factors:
                ctx.bias = "short"
                ctx.confidence = round(bear_factors / max(bull_factors + bear_factors, 1), 2)
            else:
                ctx.bias = "neutral"
                ctx.confidence = 0.5

        except Exception as e:
            logger.error(f"SMCAnalyzer.analyze error: {e}")

        return ctx


smc = SMCAnalyzer()
