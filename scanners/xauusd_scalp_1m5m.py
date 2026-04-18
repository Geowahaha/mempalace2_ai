"""
scanners/xauusd_scalp_1m5m.py - XAUUSD Advanced Behavior-Based Scalping Scanner
===================================================================================
Strategy: Forget classical TA names — read what price actually does.

Core Logic (3 phases of a big move):
  1. LIQUIDITY BUILD    — price gravitates to sweep zone ($X00/$X50, PDH/PDL, session H/L)
  2. LIQUIDITY SWEEP    — price spikes through zone, wicks back (≥45% wick ratio)
  3. FVG / IMBALANCE    — 3-bar gap left behind → price returns to fill → ENTRY

Entry: at FVG zone or sweep candle edge (NOT on breakout — wait for retest)
TP  : next liquidity pool (PDH/PDL, $X00/$X50, session H/L)
SL  : beyond sweep wick + small buffer (thesis invalidation point)

Timeframes:
  M5 — setup detection (FVG, sweep, structure)
  M1 — entry trigger confirmation (rejection candle, EMA alignment)

Kill zones active: London (14:00-17:00 BKK), NY Open (20:30-22:30 BKK)
Macro shield: DXY ±0.18% / TNX ±2bps in 15m → block adverse entries
"""
from __future__ import annotations

import logging
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional, NamedTuple

import pandas as pd
import yfinance as yf

from analysis.technical import TechnicalAnalysis
from config import config
from market.data_fetcher import xauusd_provider

logger = logging.getLogger(__name__)
ta = TechnicalAnalysis()


# ─── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class FVGZone:
    direction: str      # bullish / bearish
    lower: float
    upper: float
    formed_idx: int     # bar index when formed
    filled: bool = False

    @property
    def mid(self) -> float:
        return (self.lower + self.upper) / 2.0

    @property
    def size(self) -> float:
        return self.upper - self.lower


@dataclass
class SweepEvent:
    side: str           # bullish_sweep (swept below then recovered) / bearish_sweep
    sweep_price: float  # the extreme wick price
    close_price: float  # close back on other side
    wick_ratio: float
    bars_ago: int
    ref_level: str      # what was swept (pdh, pdl, swing_high, swing_low, round_50, round_100)
    ref_price: float


@dataclass
class ScalpSetup:
    """Full scalping setup — only emitted when ALL conditions are met."""
    symbol: str = "XAUUSD"
    direction: str = ""             # long / short
    entry: float = 0.0
    stop_loss: float = 0.0
    take_profit_1: float = 0.0
    take_profit_2: float = 0.0
    take_profit_3: float = 0.0
    risk_reward: float = 0.0
    confidence: float = 0.0
    session: str = ""
    kill_zone: str = ""
    pattern: str = ""
    sweep: Optional[SweepEvent] = None
    fvg: Optional[FVGZone] = None
    macro_shock: str = "neutral"
    atr_m5: float = 0.0
    setup_detail: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)


# ─── Scanner ──────────────────────────────────────────────────────────────────

class XAUUSDScalp1M5MScanner:
    """
    Behavior-based scalping scanner.
    Does NOT use classical indicator names — reads liquidity and price behavior.
    """

    KILL_ZONES_UTC = {
        "london_kill_zone":     ("07:00", "10:00"),   # 14:00-17:00 BKK
        "ny_open_drive":        ("13:30", "15:30"),   # 20:30-22:30 BKK
        "ny_kill_zone":         ("12:00", "15:00"),   # 19:00-22:00 BKK
    }

    def __init__(self):
        self._macro_cache: dict = {}

    # ─── Kill Zone ────────────────────────────────────────────────────────────

    def _current_kill_zone(self) -> str:
        now_utc = datetime.now(timezone.utc)
        hhmm = now_utc.strftime("%H:%M")
        for name, (start, end) in self.KILL_ZONES_UTC.items():
            if start <= hhmm <= end:
                return name
        return "off_kill_zone"

    # ─── Macro Shock Filter ───────────────────────────────────────────────────

    def _macro_shock(self) -> dict:
        """Check DXY and TNX for 15m shock. Cache 60s."""
        cache_key = "macro_15m"
        now_ts = datetime.now(timezone.utc).timestamp()
        if cache_key in self._macro_cache:
            cached = self._macro_cache[cache_key]
            if now_ts - cached.get("ts", 0) < 60:
                return cached
        result = {"available": False, "dxy_ret_15m": None, "tnx_chg_15m_bps": None,
                  "adverse_long": False, "adverse_short": False, "summary": "unavailable", "ts": now_ts}
        try:
            dxy = yf.download("DX-Y.NYB", period="1d", interval="5m", auto_adjust=True, progress=False, timeout=8)
            tnx = yf.download("^TNX", period="1d", interval="5m", auto_adjust=True, progress=False, timeout=8)

            def _close_series(df):
                if df is None or df.empty:
                    return None
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df.columns = [str(c).lower() for c in df.columns]
                return pd.to_numeric(df.get("close", pd.Series(dtype=float)), errors="coerce").dropna() if "close" in df.columns else None

            dxy_s = _close_series(dxy)
            tnx_s = _close_series(tnx)

            dxy_ret = None
            tnx_bps = None

            if dxy_s is not None and len(dxy_s) > 3:
                dxy_ret = (float(dxy_s.iloc[-1]) / float(dxy_s.iloc[-4]) - 1.0) * 100.0
                result["dxy_ret_15m"] = round(dxy_ret, 3)
            if tnx_s is not None and len(tnx_s) > 3:
                tnx_bps = (float(tnx_s.iloc[-1]) - float(tnx_s.iloc[-4])) * 10.0
                result["tnx_chg_15m_bps"] = round(tnx_bps, 2)

            dxy_thr = float(getattr(config, "XAUUSD_TRAP_DXY_SHOCK_PCT_15M", 0.18))
            tnx_thr = float(getattr(config, "XAUUSD_TRAP_TNX_SHOCK_BPS_15M", 2.0))
            result["available"] = dxy_ret is not None or tnx_bps is not None
            result["adverse_long"] = bool(
                (dxy_ret is not None and dxy_ret >= dxy_thr) or
                (tnx_bps is not None and tnx_bps >= tnx_thr)
            )
            result["adverse_short"] = bool(
                (dxy_ret is not None and dxy_ret <= -dxy_thr) or
                (tnx_bps is not None and tnx_bps <= -tnx_thr)
            )
            if result["adverse_long"] and result["adverse_short"]:
                result["summary"] = "macro_cross_shock"
            elif result["adverse_long"]:
                result["summary"] = "dxy_tnx_rising"
            elif result["adverse_short"]:
                result["summary"] = "dxy_tnx_falling"
            else:
                result["summary"] = "neutral"
        except Exception as e:
            logger.debug("[XAUScalp1M5M] Macro fetch error: %s", e)
        self._macro_cache[cache_key] = result
        return result

    # ─── FVG Detection ────────────────────────────────────────────────────────

    def _find_fvgs(self, df: pd.DataFrame, lookback: int = 20) -> list[FVGZone]:
        """
        Detect Fair Value Gaps (imbalance zones) in last `lookback` bars.

        Bullish FVG: candle[i].high < candle[i+2].low  → gap between i and i+2
        Bearish FVG: candle[i].low  > candle[i+2].high → gap between i and i+2
        """
        fvgs = []
        n = len(df)
        start = max(0, n - lookback - 2)
        for i in range(start, n - 2):
            try:
                c0_high = float(df.iloc[i]["high"])
                c0_low  = float(df.iloc[i]["low"])
                c2_high = float(df.iloc[i + 2]["high"])
                c2_low  = float(df.iloc[i + 2]["low"])

                # Bullish FVG: c0.high < c2.low → gap exists above c0, below c2
                if c0_high < c2_low:
                    fvgs.append(FVGZone(
                        direction="bullish",
                        lower=c0_high,
                        upper=c2_low,
                        formed_idx=i,
                    ))
                # Bearish FVG: c0.low > c2.high → gap exists below c0, above c2
                elif c0_low > c2_high:
                    fvgs.append(FVGZone(
                        direction="bearish",
                        lower=c2_high,
                        upper=c0_low,
                        formed_idx=i,
                    ))
            except Exception:
                continue
        return fvgs

    def _nearest_unfilled_fvg(
        self, fvgs: list[FVGZone], current_price: float, direction: str
    ) -> Optional[FVGZone]:
        """
        Find the most relevant unfilled FVG for the given trade direction.
        For long: bullish FVG below current price (price returning to fill)
        For short: bearish FVG above current price
        """
        candidates = []
        for fvg in fvgs:
            if fvg.filled:
                continue
            if direction == "long" and fvg.direction == "bullish":
                # FVG should be at or just below current price (within 2 ATR equivalent)
                if fvg.upper <= current_price * 1.003:
                    dist = abs(current_price - fvg.mid)
                    candidates.append((dist, fvg))
            elif direction == "short" and fvg.direction == "bearish":
                if fvg.lower >= current_price * 0.997:
                    dist = abs(current_price - fvg.mid)
                    candidates.append((dist, fvg))

        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    # ─── Liquidity Sweep Detection ────────────────────────────────────────────

    def _detect_sweep(
        self, df_m5: pd.DataFrame, df_h1: Optional[pd.DataFrame] = None
    ) -> Optional[SweepEvent]:
        """
        Detect a liquidity sweep in M5:
        - Price spikes through a key level (PDH, PDL, swing H/L, round numbers)
        - Wick ≥ 45% of candle range
        - Close back on the other side (rejection)
        Look in last 6 bars (30 min on M5).
        """
        if df_m5 is None or len(df_m5) < 20:
            return None

        n = len(df_m5)
        lookback = 5  # last 5 completed bars
        wick_min = float(getattr(config, "XAUUSD_TRAP_REJECTION_WICK_RATIO", 0.45))

        # Build reference levels
        levels = {}

        # Previous swing high/low (last 20 bars before sweep window)
        ref_window = df_m5.iloc[max(0, n - lookback - 20): n - lookback]
        if not ref_window.empty:
            levels["swing_high"] = float(ref_window["high"].max())
            levels["swing_low"] = float(ref_window["low"].min())

        # Round numbers
        if n > 0:
            cp = float(df_m5.iloc[-1]["close"])
            levels["round_50"] = round(cp / 50) * 50.0
            levels["round_100"] = round(cp / 100) * 100.0

        # Previous day high/low from H1
        if df_h1 is not None and not df_h1.empty:
            try:
                now_utc = datetime.now(timezone.utc)
                today_floor = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                prev_day = df_h1[df_h1.index < today_floor]
                if not prev_day.empty:
                    levels["pdh"] = float(prev_day["high"].max())
                    levels["pdl"] = float(prev_day["low"].min())
            except Exception:
                pass

        # Check recent bars for sweep + rejection
        for bar_offset in range(1, lookback + 1):
            i = n - bar_offset
            if i < 1:
                continue
            bar = df_m5.iloc[i]
            hi = float(bar["high"])
            lo = float(bar["low"])
            op = float(bar["open"])
            cl = float(bar["close"])
            rng = max(1e-9, hi - lo)

            upper_wick = hi - max(op, cl)
            lower_wick = min(op, cl) - lo

            for lev_name, lev_price in levels.items():
                # Bearish sweep: wick above level, close below it
                if (
                    hi >= lev_price
                    and cl < lev_price
                    and (upper_wick / rng) >= wick_min
                    and cl < op  # bearish close
                ):
                    return SweepEvent(
                        side="bearish_sweep",
                        sweep_price=hi,
                        close_price=cl,
                        wick_ratio=round(upper_wick / rng, 3),
                        bars_ago=bar_offset,
                        ref_level=lev_name,
                        ref_price=lev_price,
                    )
                # Bullish sweep: wick below level, close above it
                if (
                    lo <= lev_price
                    and cl > lev_price
                    and (lower_wick / rng) >= wick_min
                    and cl > op  # bullish close
                ):
                    return SweepEvent(
                        side="bullish_sweep",
                        sweep_price=lo,
                        close_price=cl,
                        wick_ratio=round(lower_wick / rng, 3),
                        bars_ago=bar_offset,
                        ref_level=lev_name,
                        ref_price=lev_price,
                    )
        return None

    # ─── M1 Entry Trigger ─────────────────────────────────────────────────────

    def _m1_entry_trigger(self, df_m1: pd.DataFrame, direction: str, current_price: float) -> dict:
        """
        Confirm entry on M1:
        - Rejection candle (wick ≥ 35% pointing against direction)
        - EMA9 and EMA21 aligned with direction
        - RSI not extreme in wrong direction
        Returns dict with ok, reason, and details.
        """
        out = {"ok": False, "reason": "unknown", "close": current_price}
        if df_m1 is None or len(df_m1) < 15:
            out["reason"] = "m1_data_insufficient"
            return out

        try:
            d = ta.add_all(df_m1.tail(60).copy())
        except Exception as e:
            out["reason"] = f"m1_ta_error:{e}"
            return out

        last = d.iloc[-1]
        cl = float(last.get("close", current_price) or current_price)
        ema9 = float(last.get("ema_9", cl) or cl)
        ema21 = float(last.get("ema_21", cl) or cl)
        rsi14 = float(last.get("rsi_14", 50) or 50)
        atr = abs(float(last.get("atr_14", 0) or 0))
        if atr <= 0:
            atr = abs(cl) * 0.001

        # Rejection candle check
        hi = float(last.get("high", cl) or cl)
        lo = float(last.get("low", cl) or cl)
        op = float(last.get("open", cl) or cl)
        rng = max(1e-9, hi - lo)
        upper_wick = hi - max(op, cl)
        lower_wick = min(op, cl) - lo
        buf = atr * 0.15

        out.update({"close": cl, "ema9": round(ema9, 2), "ema21": round(ema21, 2), "rsi": round(rsi14, 1)})

        if direction == "long":
            ema_ok = ema9 >= ema21 - buf
            rsi_ok = 45 <= rsi14 <= 75
            rejection_ok = (lower_wick / rng) >= 0.35 or cl > op  # bullish rejection or bullish close
            ok = ema_ok and rsi_ok and rejection_ok
            out["reason"] = "m1_long_confirmed" if ok else (
                "m1_ema_against" if not ema_ok else
                "m1_rsi_overbought" if rsi14 > 75 else
                "m1_no_rejection"
            )
            out["ok"] = bool(ok)
            return out

        if direction == "short":
            ema_ok = ema9 <= ema21 + buf
            rsi_ok = 25 <= rsi14 <= 55
            rejection_ok = (upper_wick / rng) >= 0.35 or cl < op  # bearish rejection or bearish close
            ok = ema_ok and rsi_ok and rejection_ok
            out["reason"] = "m1_short_confirmed" if ok else (
                "m1_ema_against" if not ema_ok else
                "m1_rsi_oversold" if rsi14 < 25 else
                "m1_no_rejection"
            )
            out["ok"] = bool(ok)
            return out

        out["reason"] = "invalid_direction"
        return out

    # ─── TP/SL Builder ────────────────────────────────────────────────────────

    def _build_tp_sl(
        self,
        direction: str,
        current_price: float,
        sweep: Optional[SweepEvent],
        fvg: Optional[FVGZone],
        df_h1: Optional[pd.DataFrame],
        atr_m5: float,
    ) -> dict:
        """
        SL: beyond sweep wick (thesis invalidation)
        TP1: 1.5R
        TP2: next liquidity pool (PDH/PDL, $X00/$X50)
        TP3: 3R or session extreme
        """
        result = {"entry": current_price, "stop_loss": 0.0, "tp1": 0.0, "tp2": 0.0, "tp3": 0.0, "rr": 0.0}

        # Entry = current price or FVG mid if price is returning to it
        entry = current_price
        if fvg is not None:
            if direction == "long" and fvg.upper < current_price:
                entry = fvg.mid
            elif direction == "short" and fvg.lower > current_price:
                entry = fvg.mid
        result["entry"] = round(entry, 2)

        # SL: beyond sweep wick + buffer
        sl_buffer = max(atr_m5 * 0.3, 1.5)  # min $1.50 buffer on gold
        if sweep is not None:
            if direction == "long":
                sl = sweep.sweep_price - sl_buffer  # below the wick low
            else:
                sl = sweep.sweep_price + sl_buffer  # above the wick high
        else:
            # No sweep detected — use ATR-based SL
            sl = entry - atr_m5 * 1.2 if direction == "long" else entry + atr_m5 * 1.2
        result["stop_loss"] = round(sl, 2)

        risk = abs(entry - sl)
        if risk <= 0:
            risk = atr_m5

        # TP levels
        if direction == "long":
            tp1 = entry + risk * 1.5
            tp2 = entry + risk * 2.5
            tp3 = entry + risk * 4.0
        else:
            tp1 = entry - risk * 1.5
            tp2 = entry - risk * 2.5
            tp3 = entry - risk * 4.0

        # Snap TP2 to nearest round level if closer than 0.5 ATR
        round_50 = round(entry / 50) * 50.0
        round_100 = round(entry / 100) * 100.0
        if direction == "long":
            # Nearest round ABOVE entry
            candidates = [r for r in [round_50, round_100, round_50 + 50, round_100 + 100] if r > entry]
        else:
            candidates = [r for r in [round_50, round_100, round_50 - 50, round_100 - 100] if r < entry]

        if candidates:
            nearest_round = min(candidates, key=lambda r: abs(tp2 - r))
            if abs(nearest_round - tp2) < atr_m5 * 0.5:
                tp2 = nearest_round

        # PDH/PDL snap for TP3
        if df_h1 is not None and not df_h1.empty:
            try:
                now_utc = datetime.now(timezone.utc)
                today_floor = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                prev_day = df_h1[df_h1.index < today_floor]
                if not prev_day.empty:
                    pdh = float(prev_day["high"].max())
                    pdl = float(prev_day["low"].min())
                    if direction == "long" and pdh > entry and abs(pdh - tp3) < atr_m5:
                        tp3 = pdh
                    elif direction == "short" and pdl < entry and abs(pdl - tp3) < atr_m5:
                        tp3 = pdl
            except Exception:
                pass

        result["tp1"] = round(tp1, 2)
        result["tp2"] = round(tp2, 2)
        result["tp3"] = round(tp3, 2)
        result["rr"] = round(abs(tp2 - entry) / max(risk, 1e-9), 2)
        return result

    # ─── Confidence Score ─────────────────────────────────────────────────────

    def _score_confidence(
        self,
        kill_zone: str,
        sweep: Optional[SweepEvent],
        fvg: Optional[FVGZone],
        macro: dict,
        direction: str,
        m1_trigger: dict,
    ) -> tuple[float, list[str]]:
        """Score confidence 0-100 based on behavior signals."""
        score = 50.0
        reasons = []

        # Kill zone bonus
        if kill_zone == "ny_open_drive":
            score += 15; reasons.append("✅ NY Open Drive kill zone")
        elif kill_zone == "london_kill_zone":
            score += 10; reasons.append("✅ London Kill Zone")
        elif kill_zone == "ny_kill_zone":
            score += 8; reasons.append("✅ NY Kill Zone")
        else:
            score -= 10; reasons.append("⚠️ Off kill zone")

        # Sweep bonus
        if sweep is not None:
            wr = sweep.wick_ratio
            if wr >= 0.7:
                score += 18; reasons.append(f"✅ Strong sweep rejection (wick {wr:.0%})")
            elif wr >= 0.55:
                score += 12; reasons.append(f"✅ Sweep rejection (wick {wr:.0%})")
            else:
                score += 6; reasons.append(f"⚠️ Weak sweep (wick {wr:.0%})")

            if sweep.bars_ago <= 2:
                score += 5; reasons.append("✅ Fresh sweep (≤2 bars ago)")
        else:
            score -= 15; reasons.append("⚠️ No sweep detected")

        # FVG bonus
        if fvg is not None:
            fvg_size = fvg.size
            score += 10; reasons.append(f"✅ FVG zone {fvg.lower:.1f}-{fvg.upper:.1f} (${fvg_size:.1f})")
        else:
            score -= 5; reasons.append("⚠️ No FVG found")

        # Macro shield
        if macro.get("available"):
            adverse = macro.get("adverse_long") if direction == "long" else macro.get("adverse_short")
            if adverse:
                score -= 20; reasons.append(f"⛔ Macro adverse: {macro.get('summary')}")
            else:
                score += 5; reasons.append("✅ Macro aligned")

        # M1 trigger
        if m1_trigger.get("ok"):
            score += 8; reasons.append(f"✅ M1 trigger confirmed: {m1_trigger.get('reason', '')}")
        else:
            score -= 8; reasons.append(f"⚠️ M1 trigger: {m1_trigger.get('reason', 'not confirmed')}")

        return max(0.0, min(100.0, score)), reasons

    # ─── Main Scan ────────────────────────────────────────────────────────────

    def scan(self) -> Optional[ScalpSetup]:
        """
        Run the behavior-based scalping scan.
        Returns ScalpSetup if conditions are met, None otherwise.
        """
        # Fetch data
        df_m5 = xauusd_provider.fetch("5m", bars=80)
        df_m1 = xauusd_provider.fetch("1m", bars=120)
        df_h1 = xauusd_provider.fetch("1h", bars=48)

        if df_m5 is None or df_m5.empty or len(df_m5) < 30:
            logger.debug("[XAUScalp1M5M] M5 data unavailable")
            return None

        # Current price
        live_price = xauusd_provider.get_current_price()
        current_price = float(live_price) if live_price else float(df_m5.iloc[-1]["close"])

        # Kill zone check
        kill_zone = self._current_kill_zone()
        kz_enabled = bool(getattr(config, "XAUUSD_SCALP_REQUIRE_KILL_ZONE", True))
        if kz_enabled and kill_zone == "off_kill_zone":
            logger.debug("[XAUScalp1M5M] Off kill zone — skip")
            return None

        # Get ATR
        try:
            m5_enriched = ta.add_all(df_m5.copy())
            atr_m5 = float(m5_enriched.iloc[-1].get("atr_14", 0) or 0)
        except Exception:
            atr_m5 = 0.0
        if atr_m5 <= 0:
            atr_m5 = max(3.0, current_price * 0.002)

        # Detect sweep
        sweep = self._detect_sweep(df_m5, df_h1)
        if sweep is None:
            logger.debug("[XAUScalp1M5M] No sweep detected")
            return None

        # Determine direction from sweep
        direction = "long" if sweep.side == "bullish_sweep" else "short"

        # Detect FVGs
        fvgs = self._find_fvgs(df_m5, lookback=25)
        fvg = self._nearest_unfilled_fvg(fvgs, current_price, direction)

        # Macro shield
        macro = self._macro_shock()
        if macro.get("available"):
            if direction == "long" and macro.get("adverse_long"):
                logger.debug("[XAUScalp1M5M] Macro adverse for long: %s", macro.get("summary"))
            if direction == "short" and macro.get("adverse_short"):
                logger.debug("[XAUScalp1M5M] Macro adverse for short: %s", macro.get("summary"))

        # M1 trigger
        m1_trigger = self._m1_entry_trigger(df_m1, direction, current_price)

        # Minimum: M1 must confirm, or sweep wick ≥ 0.6 AND FVG present
        min_m1 = bool(getattr(config, "XAUUSD_SCALP_REQUIRE_M1_TRIGGER", True))
        if min_m1 and not m1_trigger["ok"] and not (
            sweep.wick_ratio >= 0.60 and fvg is not None
        ):
            logger.debug("[XAUScalp1M5M] M1 trigger failed and no strong sweep+FVG: %s", m1_trigger["reason"])
            return None

        # Score confidence
        confidence, reasons = self._score_confidence(kill_zone, sweep, fvg, macro, direction, m1_trigger)

        min_conf = float(getattr(config, "XAUUSD_SCALP_MIN_CONFIDENCE", 58.0))
        if confidence < min_conf:
            logger.debug("[XAUScalp1M5M] Confidence %.1f < min %.1f", confidence, min_conf)
            return None

        # Build TP/SL
        tp_sl = self._build_tp_sl(direction, current_price, sweep, fvg, df_h1, atr_m5)

        # Validate RR
        if tp_sl["rr"] < 1.5:
            logger.debug("[XAUScalp1M5M] RR %.2f < 1.5 — skip", tp_sl["rr"])
            return None

        # Session label
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour
        if 7 <= hour < 12:
            session = "london"
        elif 12 <= hour < 21:
            session = "new_york"
        elif 0 <= hour < 8:
            session = "asian"
        else:
            session = "off_session"

        # Pattern description (behavior-based, no classic names)
        parts = []
        if sweep:
            parts.append(f"sweep_{sweep.ref_level}")
        if fvg:
            parts.append("fvg_retest")
        if m1_trigger["ok"]:
            parts.append("m1_rejection_confirmed")
        pattern = "+".join(parts) if parts else "behavior_scalp"

        setup_detail = {
            "kill_zone": kill_zone,
            "sweep": {
                "side": sweep.side,
                "sweep_price": sweep.sweep_price,
                "wick_ratio": sweep.wick_ratio,
                "bars_ago": sweep.bars_ago,
                "ref_level": sweep.ref_level,
                "ref_price": sweep.ref_price,
            } if sweep else None,
            "fvg": {
                "direction": fvg.direction,
                "lower": fvg.lower,
                "upper": fvg.upper,
                "size": round(fvg.size, 2),
            } if fvg else None,
            "macro": macro.get("summary", "neutral"),
            "dxy_ret_15m": macro.get("dxy_ret_15m"),
            "tnx_bps_15m": macro.get("tnx_chg_15m_bps"),
            "m1_trigger": m1_trigger.get("reason"),
            "atr_m5": round(atr_m5, 2),
            "current_price": round(current_price, 2),
        }

        logger.info("[XAUScalp1M5M] ✅ Setup: %s %s @ %.2f | SL=%.2f | TP=%.2f/%.2f/%.2f | conf=%.1f%%",
                    direction.upper(), "XAUUSD", tp_sl["entry"],
                    tp_sl["stop_loss"], tp_sl["tp1"], tp_sl["tp2"], tp_sl["tp3"], confidence)

        return ScalpSetup(
            symbol="XAUUSD",
            direction=direction,
            entry=tp_sl["entry"],
            stop_loss=tp_sl["stop_loss"],
            take_profit_1=tp_sl["tp1"],
            take_profit_2=tp_sl["tp2"],
            take_profit_3=tp_sl["tp3"],
            risk_reward=tp_sl["rr"],
            confidence=round(confidence, 1),
            session=session,
            kill_zone=kill_zone,
            pattern=pattern,
            sweep=sweep,
            fvg=fvg,
            macro_shock=macro.get("summary", "neutral"),
            atr_m5=round(atr_m5, 2),
            setup_detail=setup_detail,
            warnings=[r for r in reasons if r.startswith("⚠️") or r.startswith("⛔")],
        )


# Singleton
xauusd_scalp_1m5m = XAUUSDScalp1M5MScanner()
