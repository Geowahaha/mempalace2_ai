"""
analysis/signals.py - Signal generation engine
Combines technical analysis + SMC to produce high-probability trade signals
"""
import logging
from dataclasses import dataclass, field
from typing import Optional
import numpy as np
import pandas as pd

from analysis.technical import TechnicalAnalysis
from analysis.smc import SMCAnalyzer, SMCContext, LiquidityPool

logger = logging.getLogger(__name__)
ta = TechnicalAnalysis()
smc = SMCAnalyzer()


@dataclass
class TradeSignal:
    symbol: str
    direction: str              # 'long' | 'short'
    confidence: float           # 0-100
    entry: float
    stop_loss: float
    take_profit_1: float        # first TP (1:1)
    take_profit_2: float        # second TP (1:2)
    take_profit_3: float        # third TP (1:3+)
    risk_reward: float
    timeframe: str
    session: str
    trend: str
    rsi: float
    atr: float
    pattern: str                # e.g. "OB Bounce + BOS Confirm"
    reasons: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    smc_context: Optional[SMCContext] = None
    raw_scores: dict = field(default_factory=dict)
    # ── Tiger Hunter fields ──
    entry_type: str = "market"          # "market" | "limit" | "patience" | "buy_stop" | "sell_stop" | "stop"
    sl_type: str = "atr"                # "atr" | "anti_sweep" | "structure"
    sl_reason: str = ""                 # human-readable SL placement reason
    tp_type: str = "rr"                 # "rr" | "liquidity" | "structure"
    tp_reason: str = ""                 # human-readable TP targeting reason
    sl_liquidity_mapped: bool = False   # True if SL used anti-sweep logic
    liquidity_pools_count: int = 0      # number of pools detected nearby

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "confidence": self.confidence,
            "entry": self.entry,
            "stop_loss": self.stop_loss,
            "take_profit_1": self.take_profit_1,
            "take_profit_2": self.take_profit_2,
            "take_profit_3": self.take_profit_3,
            "risk_reward": self.risk_reward,
            "timeframe": self.timeframe,
            "session": self.session,
            "trend": self.trend,
            "rsi": self.rsi,
            "atr": self.atr,
            "pattern": self.pattern,
            "reasons": self.reasons,
            "warnings": self.warnings,
            # Tiger Hunter metadata
            "entry_type": self.entry_type,
            "sl_type": self.sl_type,
            "sl_reason": self.sl_reason,
            "tp_type": self.tp_type,
            "tp_reason": self.tp_reason,
            "sl_liquidity_mapped": self.sl_liquidity_mapped,
        }

    def emoji_direction(self) -> str:
        return "📈" if self.direction == "long" else "📉"

    def confidence_emoji(self) -> str:
        if self.confidence >= 85:
            return "🔥🔥🔥"
        elif self.confidence >= 75:
            return "🔥🔥"
        elif self.confidence >= 65:
            return "🔥"
        else:
            return "⚡"


class SignalGenerator:
    """
    Generates trade signals using multi-factor scoring:
    1. Trend alignment (multi-timeframe)
    2. RSI momentum / divergence
    3. MACD signal
    4. Bollinger Band position
    5. Volume confirmation
    6. SMC: Order Block, FVG, BOS/ChoCH
    7. Session timing
    """

    def __init__(self, min_confidence: float = 60.0):
        self.min_confidence = min_confidence

    @staticmethod
    def _price_decimals(value: float) -> int:
        """Adaptive decimals so micro-priced assets (e.g., PEPE) don't round to zero."""
        v = abs(float(value))
        if v >= 1000:
            return 2
        if v >= 100:
            return 3
        if v >= 1:
            return 4
        if v >= 0.1:
            return 5
        if v >= 0.01:
            return 6
        if v >= 0.001:
            return 7
        if v >= 0.0001:
            return 8
        if v >= 0.00001:
            return 9
        return 10

    def _round_price(self, value: float) -> float:
        return round(float(value), self._price_decimals(float(value)))

    def _fmt_price(self, value: float) -> str:
        d = self._price_decimals(float(value))
        return f"{float(value):.{d}f}"

    @staticmethod
    def _clamp(value: float, lo: float, hi: float) -> float:
        return max(float(lo), min(float(hi), float(value)))

    def _select_advantaged_entry(
        self,
        *,
        direction: str,
        close: float,
        atr: float,
        smc_ctx: Optional[SMCContext],
    ) -> tuple[float, Optional[str]]:
        """
        Prefer nearby SMC pullback/retest levels (OB/FVG/BOS) instead of always
        entering at the latest close. Falls back to close when no safe advantage exists.
        """
        close = float(close)
        atr = float(atr)
        if smc_ctx is None or not np.isfinite(close) or close <= 0 or not np.isfinite(atr) or atr <= 0:
            return close, None
        if direction not in {"long", "short"}:
            return close, None

        max_retest_distance = max(close * 0.0005, atr * 1.10)
        min_improvement = max(close * 0.00005, atr * 0.08)
        confluence_band = max(close * 0.0002, atr * 0.20)
        candidates: list[dict] = []

        def _distance_from_close(px: float) -> float:
            return (close - px) if direction == "long" else (px - close)

        def _add_candidate(px: float, label: str, base_score: float):
            try:
                px = float(px)
            except Exception:
                return
            if not np.isfinite(px) or px <= 0:
                return
            if direction == "long":
                if px > close:
                    return
            else:
                if px < close:
                    return
            improvement = _distance_from_close(px)
            if not np.isfinite(improvement) or improvement < min_improvement:
                return
            if improvement > max_retest_distance:
                return
            progress = self._clamp(improvement / max_retest_distance, 0.0, 1.0)
            # Reward both proximity (higher fill odds) and price improvement.
            score = float(base_score) + (0.55 * progress) + (0.45 * (1.0 - progress))
            candidates.append({
                "price": px,
                "label": str(label),
                "score": score,
                "improvement": improvement,
            })

        def _zone_ok(lo: float, hi: float) -> bool:
            return np.isfinite(lo) and np.isfinite(hi) and lo > 0 and hi > 0 and hi >= lo

        def _clamp_advantaged(target: float, lo: float, hi: float) -> float:
            # Keep the level inside the zone and never worse than current close.
            px = self._clamp(float(target), float(lo), float(hi))
            if direction == "long":
                px = min(px, close)
            else:
                px = max(px, close)
            return float(px)

        ob = getattr(smc_ctx, "nearest_ob", None)
        if ob is not None:
            try:
                ob_low = float(ob.low)
                ob_high = float(ob.high)
                ob_strength = float(getattr(ob, "strength", 0.0) or 0.0)
            except Exception:
                ob_low = ob_high = ob_strength = 0.0
            if _zone_ok(ob_low, ob_high):
                if direction == "long" and str(getattr(ob, "direction", "")) == "bullish":
                    # Favor a shallow mitigation entry for better fill probability.
                    target = ob_high - 0.35 * (ob_high - ob_low)
                    _add_candidate(
                        _clamp_advantaged(target, ob_low, ob_high),
                        "Bullish OB retest",
                        3.10 + (0.90 * self._clamp(ob_strength, 0.0, 1.0)) - (0.25 if bool(getattr(ob, "tested", False)) else 0.0),
                    )
                elif direction == "short" and str(getattr(ob, "direction", "")) == "bearish":
                    target = ob_low + 0.35 * (ob_high - ob_low)
                    _add_candidate(
                        _clamp_advantaged(target, ob_low, ob_high),
                        "Bearish OB retest",
                        3.10 + (0.90 * self._clamp(ob_strength, 0.0, 1.0)) - (0.25 if bool(getattr(ob, "tested", False)) else 0.0),
                    )

        fvg = getattr(smc_ctx, "nearest_fvg", None)
        if fvg is not None:
            try:
                fvg_low = float(fvg.lower)
                fvg_high = float(fvg.upper)
            except Exception:
                fvg_low = fvg_high = 0.0
            if _zone_ok(fvg_low, fvg_high):
                if direction == "long" and str(getattr(fvg, "direction", "")) == "bullish":
                    _add_candidate(
                        _clamp_advantaged((fvg_low + fvg_high) / 2.0, fvg_low, fvg_high),
                        "Bullish FVG fill",
                        2.55,
                    )
                elif direction == "short" and str(getattr(fvg, "direction", "")) == "bearish":
                    _add_candidate(
                        _clamp_advantaged((fvg_low + fvg_high) / 2.0, fvg_low, fvg_high),
                        "Bearish FVG fill",
                        2.55,
                    )

        bos = getattr(smc_ctx, "recent_bos", None)
        if bos is not None:
            try:
                bos_px = float(bos.price)
            except Exception:
                bos_px = 0.0
            if np.isfinite(bos_px) and bos_px > 0:
                bos_dir = str(getattr(bos, "direction", ""))
                bos_type = str(getattr(bos, "level_type", "BOS"))
                if direction == "long" and bos_dir == "bullish":
                    _add_candidate(bos_px, f"{bos_type} retest", 2.35 if bos_type == "ChoCH" else 2.05)
                elif direction == "short" and bos_dir == "bearish":
                    _add_candidate(bos_px, f"{bos_type} retest", 2.35 if bos_type == "ChoCH" else 2.05)

        if not candidates:
            return close, None

        # Confluence bonus when multiple candidates cluster within a small ATR band.
        for cand in candidates:
            neighbors = sum(
                1
                for other in candidates
                if other is not cand and abs(float(other["price"]) - float(cand["price"])) <= confluence_band
            )
            cand["score"] = float(cand["score"]) + (0.45 * neighbors)

        def _sort_key(c: dict) -> tuple[float, float]:
            price = float(c["price"])
            advantage = _distance_from_close(price)
            # Tie-break: prefer better price after quality score.
            return (float(c["score"]), float(advantage))

        best = max(candidates, key=_sort_key)
        entry = float(best["price"])
        improvement = _distance_from_close(entry)
        improvement_atr = (improvement / atr) if atr > 0 else 0.0
        if improvement <= 0:
            return close, None
        verb = "discount" if direction == "long" else "premium"
        note = (
            f"🎯 {best['label']} entry @ {self._fmt_price(entry)} "
            f"({improvement_atr:.2f} ATR {verb} vs close)"
        )
        return entry, note

    def score_signal(
        self,
        df_entry: pd.DataFrame,
        df_trend: Optional[pd.DataFrame],
        symbol: str,
        timeframe: str,
        session_info: dict,
    ) -> Optional[TradeSignal]:
        """
        Full scoring pipeline. Returns a TradeSignal or None if below threshold.
        """
        try:
            # Add indicators
            df_entry = ta.add_all(df_entry)
            if df_trend is not None:
                df_trend = ta.add_all(df_trend)

            last = df_entry.iloc[-1]
            atr = float(last.get("atr_14", 0)) or (float(last["high"]) - float(last["low"]))
            close = float(last["close"])
            if not np.isfinite(close) or close <= 0:
                return None
            if not np.isfinite(atr) or atr <= 0:
                return None

            # SMC analysis on entry TF
            smc_ctx = smc.analyze(df_entry)

            scores: dict[str, float] = {}
            reason_items: list[tuple[str, str]] = []
            warnings: list[str] = []
            long_score = 0.0
            short_score = 0.0

            def add_reason(side: str, text: str):
                """side: 'long' | 'short' | 'both'."""
                reason_items.append((side, text))

            # ── 1. Trend Alignment ────────────────────────────────────────────
            entry_trend = ta.determine_trend(df_entry)
            trend_label = entry_trend
            if df_trend is not None:
                higher_trend = ta.determine_trend(df_trend)
                if higher_trend == "bullish":
                    long_score += 20
                    add_reason("long", "✅ Higher-TF trend: BULLISH")
                elif higher_trend == "bearish":
                    short_score += 20
                    add_reason("short", "✅ Higher-TF trend: BEARISH")
                else:
                    warnings.append("⚠️ Higher-TF trend: RANGING (lower conviction)")
                trend_label = higher_trend

            if entry_trend == "bullish":
                long_score += 10
            elif entry_trend == "bearish":
                short_score += 10
            scores["trend"] = long_score - short_score

            # ── 2. RSI ────────────────────────────────────────────────────────
            rsi = float(last.get("rsi_14", 50))
            if rsi < 30:
                long_score += 15
                add_reason("long", f"✅ RSI oversold ({rsi:.1f})")
            elif rsi < 45:
                long_score += 8
                add_reason("long", f"✅ RSI low momentum ({rsi:.1f})")
            elif rsi > 70:
                short_score += 15
                add_reason("short", f"✅ RSI overbought ({rsi:.1f})")
            elif rsi > 55:
                short_score += 8
                add_reason("short", f"✅ RSI high momentum ({rsi:.1f})")
            else:
                warnings.append(f"⚠️ RSI neutral ({rsi:.1f})")

            # RSI divergence bonus
            divergence = ta.detect_rsi_divergence(df_entry)
            if divergence == "bullish_div":
                long_score += 15
                add_reason("long", "✅ Bullish RSI divergence detected")
            elif divergence == "bearish_div":
                short_score += 15
                add_reason("short", "✅ Bearish RSI divergence detected")
            scores["rsi"] = rsi

            # ── 3. MACD ───────────────────────────────────────────────────────
            macd = float(last.get("macd", 0))
            macd_sig = float(last.get("macd_signal", 0))
            macd_hist = float(last.get("macd_hist", 0))
            prev_hist = float(df_entry["macd_hist"].iloc[-2]) if len(df_entry) > 1 else 0

            if macd > macd_sig and macd_hist > 0 and prev_hist < 0:
                long_score += 12
                add_reason("long", "✅ MACD bullish crossover")
            elif macd_hist > 0 and macd_hist > prev_hist:
                long_score += 6
                add_reason("long", "✅ MACD histogram increasing")
            elif macd < macd_sig and macd_hist < 0 and prev_hist > 0:
                short_score += 12
                add_reason("short", "✅ MACD bearish crossover")
            elif macd_hist < 0 and macd_hist < prev_hist:
                short_score += 6
                add_reason("short", "✅ MACD histogram decreasing")
            scores["macd"] = macd_hist

            # ── 4. Bollinger Bands ────────────────────────────────────────────
            bb_pct = float(last.get("bb_pct", 0.5))
            bb_width = float(last.get("bb_width", 0))
            bb_squeeze = bb_width < float(df_entry["bb_width"].quantile(0.2)) if "bb_width" in df_entry else False

            if bb_pct <= 0.1:
                long_score += 10
                add_reason("long", f"✅ Price at lower BB band ({bb_pct:.2f})")
            elif bb_pct >= 0.9:
                short_score += 10
                add_reason("short", f"✅ Price at upper BB band ({bb_pct:.2f})")

            if bb_squeeze:
                add_reason("both", "✅ BB Squeeze: volatility compression → explosive move pending")
                long_score += 5
                short_score += 5
            scores["bb"] = bb_pct

            # ── 5. Volume ─────────────────────────────────────────────────────
            vol_ratio = float(last.get("vol_ratio", 1))
            if vol_ratio > 2.0:
                if close > float(df_entry["open"].iloc[-1]):
                    long_score += 10
                    add_reason("long", f"✅ High volume bullish bar (vol ratio: {vol_ratio:.1f}x)")
                else:
                    short_score += 10
                    add_reason("short", f"✅ High volume bearish bar (vol ratio: {vol_ratio:.1f}x)")
            elif vol_ratio < 0.5:
                warnings.append(f"⚠️ Low volume ({vol_ratio:.1f}x avg) - weak conviction")
            scores["volume"] = vol_ratio

            # ── 6. SMC Factors ────────────────────────────────────────────────
            smc_pattern_parts = []
            if smc_ctx.bias == "long":
                long_score += int(smc_ctx.confidence * 20)
                add_reason("long", f"✅ SMC bias: LONG (conf: {smc_ctx.confidence:.0%})")
            elif smc_ctx.bias == "short":
                short_score += int(smc_ctx.confidence * 20)
                add_reason("short", f"✅ SMC bias: SHORT (conf: {smc_ctx.confidence:.0%})")

            if smc_ctx.nearest_ob:
                ob = smc_ctx.nearest_ob
                if ob.direction == "bullish" and ob.high >= close * 0.99 and not ob.tested:
                    long_score += 12
                    smc_pattern_parts.append("Bullish OB")
                    add_reason("long", f"✅ Bullish Order Block at {self._fmt_price(ob.low)}-{self._fmt_price(ob.high)}")
                elif ob.direction == "bearish" and ob.low <= close * 1.01 and not ob.tested:
                    short_score += 12
                    smc_pattern_parts.append("Bearish OB")
                    add_reason("short", f"✅ Bearish Order Block at {self._fmt_price(ob.low)}-{self._fmt_price(ob.high)}")

            if smc_ctx.nearest_fvg:
                fvg = smc_ctx.nearest_fvg
                if fvg.direction == "bullish":
                    long_score += 8
                    smc_pattern_parts.append("Bullish FVG")
                    add_reason("long", f"✅ Bullish FVG: {self._fmt_price(fvg.lower)}-{self._fmt_price(fvg.upper)}")
                else:
                    short_score += 8
                    smc_pattern_parts.append("Bearish FVG")
                    add_reason("short", f"✅ Bearish FVG: {self._fmt_price(fvg.lower)}-{self._fmt_price(fvg.upper)}")

            if smc_ctx.recent_bos:
                bos = smc_ctx.recent_bos
                if bos.level_type == "ChoCH":
                    smc_pattern_parts.append("ChoCH")
                    add_reason(
                        "long" if bos.direction == "bullish" else "short",
                        f"✅ Change of Character ({bos.direction}) at {self._fmt_price(bos.price)}"
                    )
                    if bos.direction == "bullish":
                        long_score += 15
                    else:
                        short_score += 15
                elif bos.level_type == "BOS":
                    smc_pattern_parts.append("BOS")
                    if bos.direction == "bullish":
                        long_score += 8
                    else:
                        short_score += 8

            # ── 7. Session Timing ─────────────────────────────────────────────
            active_sessions = session_info.get("active_sessions", [])
            if "overlap" in active_sessions:
                long_score *= 1.1
                short_score *= 1.1
                add_reason("both", "✅ London/NY Overlap - peak volatility window")
            elif "london" in active_sessions or "new_york" in active_sessions:
                long_score *= 1.05
                short_score *= 1.05

            # ── Determine direction ───────────────────────────────────────────
            if long_score == short_score:
                return None

            direction = "long" if long_score > short_score else "short"
            raw_confidence = max(long_score, short_score)
            score_edge = abs(long_score - short_score)

            # Require a minimum directional edge to avoid weak/ambiguous setups.
            if score_edge < 12:
                return None

            opposing_score = short_score if direction == "long" else long_score
            if opposing_score >= raw_confidence * 0.75:
                warnings.append("⚠️ Strong counter-signals detected; size risk conservatively")

            # ── Normalize confidence to 0-100 ─────────────────────────────────
            # Blend absolute score strength with directional edge quality.
            # This avoids over-clustering at 100% while preserving relative ranking.
            REALISTIC_MAX = 120.0
            EDGE_MAX = 80.0
            strength_ratio = min(1.0, raw_confidence / REALISTIC_MAX)
            edge_ratio = min(1.0, score_edge / EDGE_MAX)
            confidence = round((0.75 * strength_ratio + 0.25 * edge_ratio) * 100, 1)

            if confidence < self.min_confidence:
                return None

            # ── Build entry, SL, TP ── Tiger Hunter Pipeline ─────────────────
            entry, entry_note = self._select_advantaged_entry(
                direction=direction,
                close=close,
                atr=atr,
                smc_ctx=smc_ctx,
            )
            entry_type = "limit" if entry_note else "market"
            if entry_note:
                add_reason(direction, entry_note)

            # ── Tiger Hunter: Anti-Sweep SL ─────────────────────────────────
            pools = getattr(smc_ctx, "liquidity_pools", []) or []
            stop_loss, sl_reason = smc.anti_sweep_sl(
                entry=entry,
                direction=direction,
                liquidity_pools=pools,
                atr=atr,
                ob=smc_ctx.nearest_ob,
            )
            sl_liquidity_mapped = "Anti-sweep" in sl_reason or "behind" in sl_reason.lower()
            sl_type = "anti_sweep" if sl_liquidity_mapped else "atr"
            if sl_reason:
                add_reason(direction, sl_reason)

            risk = abs(entry - stop_loss)
            if not np.isfinite(risk) or risk <= 0:
                return None

            # ── Tiger Hunter: Liquidity TP Targets ──────────────────────────
            liq_tp_levels, tp_reason = smc.liquidity_tp_targets(
                entry=entry,
                direction=direction,
                atr=atr,
                liquidity_pools=pools,
                fvgs=smc_ctx.fair_value_gaps,
            )

            if liq_tp_levels and len(liq_tp_levels) >= 1:
                # Use liquidity-based TP targets
                tp_type = "liquidity"
                tp1 = liq_tp_levels[0]
                tp2 = liq_tp_levels[1] if len(liq_tp_levels) >= 2 else (
                    entry + 2 * risk if direction == "long" else entry - 2 * risk
                )
                tp3 = liq_tp_levels[2] if len(liq_tp_levels) >= 3 else (
                    entry + 3 * risk if direction == "long" else entry - 3 * risk
                )
                # Validate TP direction (tp must be in profit direction)
                if direction == "long":
                    tp1 = max(tp1, entry + 0.5 * risk)  # minimum 0.5R
                    tp2 = max(tp2, tp1 + 0.3 * risk)
                    tp3 = max(tp3, tp2 + 0.3 * risk)
                else:
                    tp1 = min(tp1, entry - 0.5 * risk)
                    tp2 = min(tp2, tp1 - 0.3 * risk)
                    tp3 = min(tp3, tp2 - 0.3 * risk)
                if tp_reason:
                    add_reason(direction, tp_reason)
            else:
                # Fallback: mechanical R:R targets
                tp_type = "rr"
                tp_reason = "TP: mechanical R:R"
                if direction == "long":
                    tp1 = entry + risk
                    tp2 = entry + 2 * risk
                    tp3 = entry + 3 * risk
                else:
                    tp1 = entry - risk
                    tp2 = entry - 2 * risk
                    tp3 = entry - 3 * risk

            if not all(np.isfinite(v) for v in (entry, stop_loss, tp1, tp2, tp3, risk)):
                return None

            rr = round(abs(tp2 - entry) / abs(entry - stop_loss), 2)

            filtered_pattern_parts = smc_pattern_parts
            if direction == "long":
                filtered_pattern_parts = [p for p in smc_pattern_parts if "Bearish" not in p]
            else:
                filtered_pattern_parts = [p for p in smc_pattern_parts if "Bullish" not in p]
            pattern = " + ".join(filtered_pattern_parts) if filtered_pattern_parts else "Multi-Factor Confluence"

            reasons = [text for side, text in reason_items if side in (direction, "both")]

            return TradeSignal(
                symbol=symbol,
                direction=direction,
                confidence=confidence,
                entry=self._round_price(entry),
                stop_loss=self._round_price(stop_loss),
                take_profit_1=self._round_price(tp1),
                take_profit_2=self._round_price(tp2),
                take_profit_3=self._round_price(tp3),
                risk_reward=rr,
                timeframe=timeframe,
                session=", ".join(active_sessions) or "off_hours",
                trend=trend_label,
                rsi=round(rsi, 2),
                atr=self._round_price(atr),
                pattern=pattern,
                reasons=reasons,
                warnings=warnings,
                smc_context=smc_ctx,
                raw_scores={"long": long_score, "short": short_score, "edge": score_edge},
                # Tiger Hunter metadata
                entry_type=entry_type,
                sl_type=sl_type,
                sl_reason=sl_reason,
                tp_type=tp_type,
                tp_reason=tp_reason,
                sl_liquidity_mapped=sl_liquidity_mapped,
                liquidity_pools_count=len(pools),
            )

        except Exception as e:
            logger.error(f"SignalGenerator.score_signal({symbol}): {e}", exc_info=True)
            return None


signal_gen = SignalGenerator()
