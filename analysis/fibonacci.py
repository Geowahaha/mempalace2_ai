"""
analysis/fibonacci.py - Fibonacci & Elliott Wave Analysis Engine

Detects swing points, computes Fibonacci retracement/extension levels,
identifies Golden Pocket zones, and evaluates Elliott Wave impulse context.

Design philosophy (from research):
  - Fibonacci alone has ~37% edge — NOT used as primary signal
  - Golden Pocket (0.618-0.65) + SMC confluence → success rate jumps to ~68%
  - Elliott Wave impulse context confirms the retracement direction
  - Order flow (DOM, delta) is required final gate before entry
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Fibonacci ratios ──────────────────────────────────────────────────────────
FIBO_RETRACEMENT_LEVELS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.65, 0.786, 1.0]
FIBO_EXTENSION_LEVELS   = [1.0, 1.272, 1.414, 1.618, 2.0, 2.618]

# Golden Pocket: institutional accumulation zone (ICT methodology)
GOLDEN_POCKET_LOW  = 0.618
GOLDEN_POCKET_HIGH = 0.65


@dataclass
class SwingPoint:
    index: int
    price: float
    bar_time: any
    swing_type: str  # 'high' | 'low'


@dataclass
class FibonacciLevels:
    direction: str           # 'bullish' (price retracing down) | 'bearish' (price retracing up)
    swing_start: float       # origin of the impulse move
    swing_end: float         # end of the impulse move (where retracement begins)
    swing_range: float       # abs(swing_end - swing_start)
    levels: dict             # ratio → price  e.g. {0.618: 2345.10, ...}
    extensions: dict         # ratio → price  e.g. {1.618: 2380.00, ...}
    golden_pocket_low: float
    golden_pocket_high: float
    impulse_strength: float  # 0-1 score of the impulse quality
    swing_start_idx: int = 0
    swing_end_idx: int = 0


@dataclass
class FiboSignalContext:
    """Summary of Fibonacci analysis for signal scoring."""
    fib_levels: Optional[FibonacciLevels] = None
    nearest_level_ratio: float = 0.0
    nearest_level_price: float = 0.0
    nearest_level_dist_pct: float = 0.0   # % distance from current price to level
    in_golden_pocket: bool = False
    in_382_zone: bool = False
    in_786_zone: bool = False
    retracement_depth: float = 0.0        # how far retraced (0-1)
    retracement_healthy: bool = False     # True if < 0.786 (structure still intact)
    elliott_wave_count: int = 0           # estimated wave count in current move
    impulse_confirmed: bool = False       # True if preceding move qualifies as impulse
    volume_diminishing: bool = False      # True if volume contracting on retracement
    fibo_confluence_score: float = 0.0   # 0-100 composite Fibonacci confidence
    reasons: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


class FibonacciAnalyzer:
    """
    Fibonacci retracement and Elliott Wave impulse analyzer.
    Designed to integrate with Dexter's SMC + order-flow pipeline.
    """

    def __init__(self, swing_lookback: int = 5, min_impulse_atr_mult: float = 1.2):
        self.swing_lookback = swing_lookback
        self.min_impulse_atr_mult = min_impulse_atr_mult

    # ── Swing Detection ───────────────────────────────────────────────────────

    def detect_swings(self, df: pd.DataFrame, left_bars: int = 5, right_bars: int = 3) -> list[SwingPoint]:
        """Detect significant swing highs and lows using fractal logic."""
        swings = []
        highs = df["high"].values
        lows  = df["low"].values
        idx   = df.index

        for i in range(left_bars, len(df) - right_bars):
            # Swing high: highest in left_bars+right_bars window
            left_high  = max(highs[i - left_bars:i])
            right_high = max(highs[i + 1:i + right_bars + 1])
            if highs[i] > left_high and highs[i] > right_high:
                swings.append(SwingPoint(
                    index=i, price=float(highs[i]),
                    bar_time=idx[i], swing_type="high"
                ))

            # Swing low: lowest in window
            left_low  = min(lows[i - left_bars:i])
            right_low = min(lows[i + 1:i + right_bars + 1])
            if lows[i] < left_low and lows[i] < right_low:
                swings.append(SwingPoint(
                    index=i, price=float(lows[i]),
                    bar_time=idx[i], swing_type="low"
                ))

        return sorted(swings, key=lambda s: s.index)

    # ── Impulse Quality ───────────────────────────────────────────────────────

    def _impulse_strength(self, df: pd.DataFrame, start_idx: int, end_idx: int,
                          atr: float) -> float:
        """
        Score the quality of an impulse move (0-1).
        High score = strong directional move with volume + momentum.
        """
        if end_idx <= start_idx or atr <= 0:
            return 0.0

        segment = df.iloc[start_idx:end_idx + 1]
        price_range = abs(float(df["close"].iloc[end_idx]) - float(df["close"].iloc[start_idx]))

        # Range vs ATR (institutional-grade move = > 2x ATR)
        range_score = min(price_range / (atr * 3.0), 1.0)

        # Directional consistency: most bars should close in impulse direction
        is_up = float(df["close"].iloc[end_idx]) > float(df["close"].iloc[start_idx])
        closes = segment["close"].values
        opens  = segment["open"].values
        aligned = sum(1 for c, o in zip(closes, opens) if (c > o) == is_up)
        direction_score = aligned / max(len(segment), 1)

        # Volume (if available): should be above average on impulse bars
        vol_score = 0.5
        if "volume" in df.columns:
            avg_vol = float(df["volume"].mean()) or 1.0
            seg_vol = float(segment["volume"].mean()) or 0.0
            vol_score = min(seg_vol / avg_vol, 2.0) / 2.0

        strength = (range_score * 0.5 + direction_score * 0.35 + vol_score * 0.15)
        return round(min(strength, 1.0), 3)

    # ── Fibonacci Levels ──────────────────────────────────────────────────────

    def compute_fibonacci_levels(self, swing_start: float, swing_end: float,
                                 swing_start_idx: int, swing_end_idx: int,
                                 impulse_strength: float) -> FibonacciLevels:
        """
        Compute retracement and extension levels.
        For a bullish impulse (low → high): retracement = price pulling back down.
        For a bearish impulse (high → low): retracement = price pulling back up.
        """
        is_bullish = swing_end > swing_start
        direction  = "bullish" if is_bullish else "bearish"
        rng        = abs(swing_end - swing_start)

        levels = {}
        for ratio in FIBO_RETRACEMENT_LEVELS:
            if is_bullish:
                levels[ratio] = swing_end - ratio * rng
            else:
                levels[ratio] = swing_end + ratio * rng

        extensions = {}
        for ratio in FIBO_EXTENSION_LEVELS:
            if is_bullish:
                extensions[ratio] = swing_start + ratio * rng
            else:
                extensions[ratio] = swing_start - ratio * rng

        return FibonacciLevels(
            direction=direction,
            swing_start=swing_start,
            swing_end=swing_end,
            swing_range=rng,
            levels=levels,
            extensions=extensions,
            golden_pocket_low=levels[GOLDEN_POCKET_LOW],
            golden_pocket_high=levels[GOLDEN_POCKET_HIGH],
            impulse_strength=impulse_strength,
            swing_start_idx=swing_start_idx,
            swing_end_idx=swing_end_idx,
        )

    # ── Nearest Level Detection ───────────────────────────────────────────────

    def nearest_retracement_level(self, current_price: float,
                                  fib: FibonacciLevels) -> tuple[float, float, float]:
        """
        Returns (ratio, level_price, distance_pct) for the nearest retracement level.
        """
        best_ratio = 0.0
        best_price = 0.0
        best_dist  = float("inf")

        for ratio, price in fib.levels.items():
            dist = abs(current_price - price)
            if dist < best_dist:
                best_dist  = dist
                best_ratio = ratio
                best_price = price

        dist_pct = (best_dist / max(current_price, 1.0)) * 100.0
        return best_ratio, best_price, round(dist_pct, 4)

    # ── Elliott Wave Context (validated) ─────────────────────────────────────

    def estimate_wave_count(self, swings: list[SwingPoint], direction: str) -> int:
        """
        Elliott Wave count with structural validation rules.

        Rules enforced (institution-grade):
          1. Wave 3 cannot be the shortest impulse wave
          2. Wave 2 cannot retrace beyond the start of Wave 1
          3. Wave 4 cannot overlap Wave 1 price territory (in trending markets)

        Returns the validated wave number (1-5 for impulse, 0 if invalid/uncertain).
        """
        if len(swings) < 3:
            return 0

        relevant = list(swings[-12:])
        if len(relevant) < 3:
            return 0

        # Build alternating wave segments
        waves: list[dict] = []
        for i in range(1, len(relevant)):
            if relevant[i].swing_type != relevant[i - 1].swing_type:
                waves.append({
                    "start_price": relevant[i - 1].price,
                    "end_price": relevant[i].price,
                    "start_idx": relevant[i - 1].index,
                    "end_idx": relevant[i].index,
                    "magnitude": abs(relevant[i].price - relevant[i - 1].price),
                    "is_impulse": (relevant[i].price > relevant[i - 1].price) == (direction == "bullish"),
                })

        if len(waves) < 2:
            return 0

        # Map alternating moves to Elliott waves
        impulse_waves = [w for w in waves if w["is_impulse"]]
        corrective_waves = [w for w in waves if not w["is_impulse"]]

        wave_count = min(len(waves), 5)

        # ── Validation: Wave 3 cannot be shortest impulse ─────────────
        if len(impulse_waves) >= 3:
            magnitudes = [w["magnitude"] for w in impulse_waves[:3]]
            if magnitudes[1] == min(magnitudes):
                wave_count = min(wave_count, 2)

        # ── Validation: Wave 2 must not retrace past Wave 1 start ─────
        if len(waves) >= 2 and not waves[1]["is_impulse"]:
            w1_start = waves[0]["start_price"]
            w2_end = waves[1]["end_price"]
            if direction == "bullish" and w2_end < w1_start:
                wave_count = 0
            elif direction == "bearish" and w2_end > w1_start:
                wave_count = 0

        # ── Validation: Wave 4 must not overlap Wave 1 territory ──────
        if len(waves) >= 4 and not waves[3]["is_impulse"]:
            w1_end = waves[0]["end_price"]
            w4_end = waves[3]["end_price"]
            if direction == "bullish" and w4_end < w1_end:
                wave_count = min(wave_count, 3)
            elif direction == "bearish" and w4_end > w1_end:
                wave_count = min(wave_count, 3)

        return wave_count

    # ── Volume Trend on Retracement ───────────────────────────────────────────

    def _volume_diminishing(self, df: pd.DataFrame, retracement_start_idx: int) -> bool:
        """
        Healthy retracement = volume contracts as price retraces.
        Compare last N bars' volume to prior N bars.
        """
        if "volume" not in df.columns:
            return True  # assume healthy if no volume data

        lookback = min(5, retracement_start_idx)
        if retracement_start_idx < lookback:
            return True

        impulse_vol    = float(df["volume"].iloc[retracement_start_idx - lookback:retracement_start_idx].mean())
        retracement_vol = float(df["volume"].iloc[retracement_start_idx:].mean())

        if impulse_vol <= 0:
            return True

        return retracement_vol < impulse_vol * 0.85  # volume < 85% of impulse = contracting

    # ── Main Analysis Entry Point ─────────────────────────────────────────────

    def analyze(self, df_structure: pd.DataFrame, df_entry: pd.DataFrame,
                current_price: float, atr: float,
                smc_context=None) -> FiboSignalContext:
        """
        Full Fibonacci analysis.

        Args:
            df_structure: H4 bars for swing/impulse detection
            df_entry:     H1 bars for entry-level Fibonacci precision
            current_price: live price
            atr:           ATR from entry timeframe
            smc_context:   SMCContext from smc.py (optional, for confluence)

        Returns:
            FiboSignalContext with score and metadata
        """
        ctx = FiboSignalContext()

        try:
            if df_structure is None or df_structure.empty or len(df_structure) < 20:
                ctx.warnings.append("insufficient_structure_data")
                return ctx

            # ── 1. Detect swings on structure TF (H4) ─────────────────────
            swings = self.detect_swings(df_structure, left_bars=5, right_bars=3)
            if len(swings) < 4:
                ctx.warnings.append("insufficient_swings")
                return ctx

            # ── 2. Identify the most recent completed impulse ─────────────
            # Look at the last 4 swings: find a high-low or low-high pair
            # where the move qualifies as an impulse
            last_swings = swings[-6:]
            fib_levels: Optional[FibonacciLevels] = None
            impulse_start_idx = 0
            impulse_end_idx   = 0

            for i in range(len(last_swings) - 1, 0, -1):
                s_end   = last_swings[i]
                s_start = last_swings[i - 1]

                if s_start.swing_type == s_end.swing_type:
                    continue  # need alternating high/low

                rng = abs(s_end.price - s_start.price)
                if atr > 0 and rng < atr * self.min_impulse_atr_mult:
                    continue  # too small to be impulse

                strength = self._impulse_strength(
                    df_structure, s_start.index, s_end.index, atr
                )
                if strength < 0.35:
                    continue  # weak move, skip

                fib_levels = self.compute_fibonacci_levels(
                    swing_start=s_start.price,
                    swing_end=s_end.price,
                    swing_start_idx=s_start.index,
                    swing_end_idx=s_end.index,
                    impulse_strength=strength,
                )
                impulse_start_idx = s_start.index
                impulse_end_idx   = s_end.index
                ctx.impulse_confirmed = strength >= 0.5
                break

            if fib_levels is None:
                ctx.warnings.append("no_valid_impulse_found")
                return ctx

            ctx.fib_levels = fib_levels

            # ── 3. Nearest level + retracement depth ─────────────────────
            ratio, level_price, dist_pct = self.nearest_retracement_level(current_price, fib_levels)
            ctx.nearest_level_ratio  = ratio
            ctx.nearest_level_price  = level_price
            ctx.nearest_level_dist_pct = dist_pct

            rng = fib_levels.swing_range
            if rng > 0:
                if fib_levels.direction == "bullish":
                    ctx.retracement_depth = (fib_levels.swing_end - current_price) / rng
                else:
                    ctx.retracement_depth = (current_price - fib_levels.swing_end) / rng
                ctx.retracement_depth = max(0.0, min(ctx.retracement_depth, 1.2))

            ctx.retracement_healthy = ctx.retracement_depth <= 0.786

            # ── 4. Zone classification ────────────────────────────────────
            gp_lo = fib_levels.golden_pocket_low
            gp_hi = fib_levels.golden_pocket_high
            zone_tol = atr * 0.3  # tolerance around each level

            ctx.in_golden_pocket = (
                min(gp_lo, gp_hi) - zone_tol <= current_price <= max(gp_lo, gp_hi) + zone_tol
            )
            lvl_382 = fib_levels.levels.get(0.382, 0.0)
            lvl_786 = fib_levels.levels.get(0.786, 0.0)
            ctx.in_382_zone = abs(current_price - lvl_382) <= zone_tol
            ctx.in_786_zone = abs(current_price - lvl_786) <= zone_tol

            # ── 5. Elliott Wave estimate ──────────────────────────────────
            ctx.elliott_wave_count = self.estimate_wave_count(swings, fib_levels.direction)

            # ── 6. Volume check ───────────────────────────────────────────
            ctx.volume_diminishing = self._volume_diminishing(df_structure, impulse_end_idx)

            # ── 7. Confluence scoring ─────────────────────────────────────
            score   = 0.0
            reasons = []
            warnings = []

            if ctx.in_golden_pocket:
                score += 30.0
                reasons.append("golden_pocket_0618_065")
            elif ctx.in_382_zone:
                score += 18.0
                reasons.append("fib_382_zone")
            elif ctx.in_786_zone:
                score += 8.0
                warnings.append("deep_retracement_786_risky")
            elif 0.45 < ctx.nearest_level_ratio <= 0.55:
                score += 12.0
                reasons.append("fib_50pct_zone")
            elif dist_pct < 0.15:
                score += 5.0
                reasons.append(f"near_fib_{ctx.nearest_level_ratio:.3f}")

            if ctx.impulse_confirmed:
                score += 15.0
                reasons.append(f"impulse_confirmed_strength_{fib_levels.impulse_strength:.2f}")
            else:
                warnings.append("impulse_weak")

            if ctx.retracement_healthy:
                score += 8.0
                reasons.append(f"retracement_healthy_{ctx.retracement_depth:.2f}")
            else:
                score -= 15.0
                warnings.append("retracement_deep_structure_at_risk")

            if ctx.volume_diminishing:
                score += 8.0
                reasons.append("retracement_volume_contracting")

            if ctx.elliott_wave_count in (2, 4):
                score += 10.0
                reasons.append(f"ew_wave_{ctx.elliott_wave_count}_retracement")
            elif ctx.elliott_wave_count == 3:
                score += 5.0
                reasons.append("ew_wave_3_continuation")

            # SMC confluence
            if smc_context is not None:
                ob = smc_context.nearest_ob
                fvg = smc_context.nearest_fvg
                if ob and not ob.broken:
                    ob_mid = (ob.high + ob.low) / 2
                    if abs(ob_mid - current_price) <= atr * 0.5:
                        score += 12.0
                        reasons.append("ob_at_fib_level")
                if fvg and not fvg.filled:
                    fvg_mid = (fvg.upper + fvg.lower) / 2
                    if abs(fvg_mid - current_price) <= atr * 0.6:
                        score += 8.0
                        reasons.append("fvg_at_fib_level")
                if smc_context.recent_bos:
                    score += 5.0
                    reasons.append("bos_confirmed")

            ctx.fibo_confluence_score = round(min(score, 100.0), 2)
            ctx.reasons  = reasons
            ctx.warnings = warnings

        except Exception as e:
            logger.warning("[FiboAnalyzer] analyze error: %s", e)
            ctx.warnings.append(f"analyzer_error:{e}")

        return ctx
