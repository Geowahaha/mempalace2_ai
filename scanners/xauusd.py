"""
scanners/xauusd.py - Professional XAUUSD (Gold) Scanner
Multi-timeframe analysis: D1 trend → H4 structure → H1/M15 entry
Incorporates SMC, session timing, key level detection
"""
import logging
from typing import Optional
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

from market.data_fetcher import xauusd_provider, session_manager
from analysis.technical import TechnicalAnalysis
from analysis.smc import SMCAnalyzer
from analysis.signals import SignalGenerator, TradeSignal
from config import config
from learning.live_profile_autopilot import LiveProfileAutopilot
autopilot = LiveProfileAutopilot()

logger = logging.getLogger(__name__)
ta = TechnicalAnalysis()
smc = SMCAnalyzer()
sig = SignalGenerator(min_confidence=config.MIN_SIGNAL_CONFIDENCE)


class XAUUSDScanner:
    """
    Professional XAUUSD scanner.
    Uses 3 timeframes:
      - D1 for overall trend bias
      - H4 for structure and key levels
      - H1 for precise entry timing
    Also tracks Asian range, London breakout, NY continuation.
    """

    def _check_microstructure_alignment(self, signal: TradeSignal, current_price: float) -> tuple[bool, str]:
        """
        Final check against the TickBarEngine data via the autopilot.
        Validates that order-flow delta and depth imbalance support the direction.
        """
        try:
            snapshot = autopilot.latest_capture_feature_snapshot(
                symbol="XAUUSD", 
                direction=signal.direction, 
                confidence=signal.confidence
            )
            if not bool((snapshot or {}).get("ok")):
                status = str((snapshot or {}).get("status") or "capture_unavailable").strip().lower() or "capture_unavailable"
                return True, f"micro_capture_unavailable:{status}"
            features = snapshot.get("features", {}) if snapshot else {}
            if not features:
                return True, "micro_capture_unavailable:no_features"
            spots_count = int(features.get("spots_count", 0) or 0)
            depth_count = int(features.get("depth_count", 0) or 0)
            if spots_count < 3 and depth_count < 3:
                return True, f"micro_capture_insufficient:{spots_count}s_{depth_count}d"
            delta = float(features.get("delta_proxy", 0.0))
            imbalance = float(features.get("depth_imbalance", 0.0))
            tick_velocity = float(features.get("bar_volume_proxy", 0.0))
            
            delta_thr = float(getattr(config, "MRD_DELTA_BIAS_THRESHOLD", 0.15) or 0.15)
            imb_thr = float(getattr(config, "MRD_DEPTH_IMBALANCE_THRESHOLD", 0.25) or 0.25)
            vel_thr = float(getattr(config, "MRD_TICK_VELOCITY_MIN", 0.10) or 0.10)
            high_conf = float(getattr(config, "MRD_HIGH_CONF_THRESHOLD", 75.0) or 75.0)
            high_conf_relax = float(getattr(config, "MRD_HIGH_CONF_DELTA_RELAX", 0.10) or 0.10)

            effective_delta_thr = delta_thr + high_conf_relax if signal.confidence >= high_conf else delta_thr

            if signal.direction == "long":
                if delta < -effective_delta_thr:
                    return False, f"negative_delta_bias:{delta:.3f}"
                if imbalance < -imb_thr:
                    return False, f"negative_depth_imbalance:{imbalance:.3f}"
            else:
                if delta > effective_delta_thr:
                    return False, f"positive_delta_bias:{delta:.3f}"
                if imbalance > imb_thr:
                    return False, f"positive_depth_imbalance:{imbalance:.3f}"
            
            if tick_velocity < vel_thr:
                if tick_velocity == 0.0 and delta == 0.0 and imbalance == 0.0:
                    return True, "micro_stale_data_passthrough:all_zero"
                return False, f"low_tick_velocity:{tick_velocity:.3f}"
                
            return True, "micro_aligned"
        except Exception as e:
            logger.debug("[XAUUSD] microstructure alignment error: %s", e)
            return True, "micro_error_default_on"

    def __init__(self):
        self.last_signal: Optional[TradeSignal] = None
        self.scan_count = 0
        self.signal_count = 0
        self._macro_ref_cache_ts: float = 0.0
        self._macro_ref_cache: dict = {}
        self._last_scan_diagnostics: dict = {}

    def get_last_scan_diagnostics(self) -> dict:
        return dict(self._last_scan_diagnostics or {})

    def _set_last_scan_diagnostics(self, **kwargs) -> None:
        try:
            self._last_scan_diagnostics = dict(kwargs or {})
        except Exception:
            self._last_scan_diagnostics = {}

    def get_asian_range(self) -> Optional[dict]:
        """Calculate the Asian session high/low range on H1."""
        df = xauusd_provider.fetch("1h", bars=24)
        if df is None or df.empty:
            return None

        now_utc = datetime.now(timezone.utc)
        try:
            # Asian session: 00:00-08:00 UTC
            asian_bars = df[df.index.hour.isin(range(0, 8))]
            if asian_bars.empty:
                return None
            return {
                "high": round(float(asian_bars["high"].max()), 2),
                "low": round(float(asian_bars["low"].min()), 2),
                "range_size": round(float(asian_bars["high"].max() - asian_bars["low"].min()), 2),
                "mid": round(float((asian_bars["high"].max() + asian_bars["low"].min()) / 2), 2),
            }
        except Exception as e:
            logger.error(f"Asian range error: {e}")
            return None

    def analyze_key_levels(self, current_price: float) -> dict:
        """Get key support/resistance levels for gold."""
        levels = {}

        # Psychological round numbers for gold (every $50, $100)
        mag = 50
        nearest_50_above = (int(current_price / mag) + 1) * mag
        nearest_50_below = int(current_price / mag) * mag

        levels["nearest_resistance"] = float(nearest_50_above)
        levels["nearest_support"] = float(nearest_50_below)
        levels["distance_to_res"] = round(nearest_50_above - current_price, 2)
        levels["distance_to_sup"] = round(current_price - nearest_50_below, 2)

        # Asian range
        asian = self.get_asian_range()
        if asian:
            levels["asian_high"] = asian["high"]
            levels["asian_low"] = asian["low"]
            levels["asian_range"] = asian["range_size"]
            levels["asian_mid"] = asian["mid"]

        return levels

    @staticmethod
    def _tf_label(tf: str) -> str:
        token = str(tf or "").strip().lower()
        table = {
            "1m": "M1",
            "3m": "M3",
            "5m": "M5",
            "15m": "M15",
            "30m": "M30",
            "1h": "H1",
            "2h": "H2",
            "4h": "H4",
            "1d": "D1",
            "1w": "W1",
        }
        return table.get(token, str(tf or "").upper() or "?")


    @staticmethod
    def _coerce_utc_index(df):
        if df is None or getattr(df, "empty", True):
            return df
        d = df.copy()
        try:
            idx = d.index
            if getattr(idx, "tz", None) is None:
                d.index = pd.to_datetime(idx, utc=True)
            else:
                d.index = idx.tz_convert(timezone.utc)
        except Exception:
            try:
                d.index = pd.to_datetime(d.index, utc=True)
            except Exception:
                return d
        return d.sort_index()

    @staticmethod
    def _slice_time_window(df, start_dt: datetime, end_dt: datetime):
        if df is None or getattr(df, "empty", True):
            return df
        return df[(df.index >= start_dt) & (df.index < end_dt)]

    @staticmethod
    def _hilo_summary(df) -> Optional[dict]:
        if df is None or getattr(df, "empty", True):
            return None
        try:
            return {"high": round(float(df["high"].max()), 2), "low": round(float(df["low"].min()), 2)}
        except Exception:
            return None

    @staticmethod
    def _session_vwap(df) -> Optional[float]:
        if df is None or getattr(df, "empty", True):
            return None
        try:
            vol = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0.0)
            if float(vol.sum()) <= 0:
                return None
            tp = (pd.to_numeric(df["high"], errors="coerce") + pd.to_numeric(df["low"], errors="coerce") + pd.to_numeric(df["close"], errors="coerce")) / 3.0
            return float((tp * vol).sum() / vol.sum())
        except Exception:
            return None

    def _news_freeze_context(self) -> dict:
        out = {
            "enabled": bool(getattr(config, "XAUUSD_NEWS_FREEZE_ENABLED", True)),
            "active": False,
            "nearest_min": -1,
            "window_min": int(getattr(config, "XAUUSD_NEWS_FREEZE_WINDOW_MIN", 20)),
            "events": [],
        }
        if not out["enabled"]:
            return out
        try:
            hits, nearest = self._nearby_usd_event_risk()
            out["nearest_min"] = int(nearest) if nearest is not None else -1
            out["events"] = [str(getattr(ev, "title", "")) for ev in hits[:3]]
            out["active"] = bool(hits) and nearest >= 0 and nearest <= out["window_min"]
        except Exception:
            pass
        return out

    def _macro_ref_series(self, ticker: str, period: str = "5d", interval: str = "5m") -> Optional[pd.Series]:
        try:
            cache_key = f"{ticker}|{period}|{interval}"
            now_ts = datetime.now(timezone.utc).timestamp()
            cached = self._macro_ref_cache.get(cache_key) if isinstance(self._macro_ref_cache, dict) else None
            if cached and (now_ts - float(cached.get("ts", 0))) < 60:
                return cached.get("series")
            raw = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False, timeout=10)
            if raw is None or raw.empty:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.columns = [str(c).strip().lower() for c in raw.columns]
            if "close" not in raw.columns:
                return None
            s = pd.to_numeric(raw["close"], errors="coerce").dropna()
            if s.empty:
                return None
            try:
                if getattr(s.index, "tz", None) is None:
                    s.index = pd.to_datetime(s.index, utc=True)
                else:
                    s.index = s.index.tz_convert(timezone.utc)
            except Exception:
                s.index = pd.to_datetime(s.index, utc=True)
            self._macro_ref_cache[cache_key] = {"ts": now_ts, "series": s}
            return s
        except Exception as e:
            logger.debug("[XAUUSD] macro ref fetch %s failed: %s", ticker, e)
            return None

    def _macro_shock_context(self, df_m5) -> dict:
        out = {
            "available": False,
            "dxy": {},
            "tnx": {},
            "xau": {},
            "adverse_for_long": False,
            "adverse_for_short": False,
            "summary": "unavailable",
        }
        try:
            dxy = self._macro_ref_series("DX-Y.NYB")
            tnx = self._macro_ref_series("^TNX")
            xau = None
            if df_m5 is not None and not getattr(df_m5, "empty", True):
                d = self._coerce_utc_index(df_m5)
                xau = pd.to_numeric(d.get("close"), errors="coerce").dropna() if "close" in d.columns else None

            def pct_ret(series, bars_back: int) -> Optional[float]:
                if series is None or len(series) <= bars_back:
                    return None
                prev = float(series.iloc[-(bars_back + 1)])
                last = float(series.iloc[-1])
                if prev == 0:
                    return None
                return (last / prev - 1.0) * 100.0

            dxy_15 = pct_ret(dxy, 3)
            dxy_60 = pct_ret(dxy, 12)
            tnx_15_bps = None
            tnx_60_bps = None
            if tnx is not None and len(tnx) > 12:
                tnx_15_bps = (float(tnx.iloc[-1]) - float(tnx.iloc[-4])) * 10.0
                tnx_60_bps = (float(tnx.iloc[-1]) - float(tnx.iloc[-13])) * 10.0
            xau_15 = pct_ret(xau, 3)
            xau_60 = pct_ret(xau, 12)
            out["dxy"] = {"ret_15m_pct": None if dxy_15 is None else round(dxy_15, 3), "ret_60m_pct": None if dxy_60 is None else round(dxy_60, 3)}
            out["tnx"] = {"chg_15m_bps": None if tnx_15_bps is None else round(tnx_15_bps, 2), "chg_60m_bps": None if tnx_60_bps is None else round(tnx_60_bps, 2)}
            out["xau"] = {"ret_15m_pct": None if xau_15 is None else round(xau_15, 3), "ret_60m_pct": None if xau_60 is None else round(xau_60, 3)}
            out["available"] = any(v is not None for v in (dxy_15, tnx_15_bps))
            dxy_thr = float(getattr(config, "XAUUSD_TRAP_DXY_SHOCK_PCT_15M", 0.18))
            tnx_thr = float(getattr(config, "XAUUSD_TRAP_TNX_SHOCK_BPS_15M", 2.0))
            dxy_up = dxy_15 is not None and dxy_15 >= dxy_thr
            dxy_down = dxy_15 is not None and dxy_15 <= -dxy_thr
            tnx_up = tnx_15_bps is not None and tnx_15_bps >= tnx_thr
            tnx_down = tnx_15_bps is not None and tnx_15_bps <= -tnx_thr
            out["adverse_for_long"] = bool(dxy_up or tnx_up)
            out["adverse_for_short"] = bool(dxy_down or tnx_down)
            if out["adverse_for_long"] and out["adverse_for_short"]:
                out["summary"] = "cross-asset shock mixed"
            elif out["adverse_for_long"]:
                out["summary"] = "DXY/TNX shock against XAU longs"
            elif out["adverse_for_short"]:
                out["summary"] = "DXY/TNX drop shock against XAU shorts"
            else:
                out["summary"] = "macro shock neutral"
        except Exception as e:
            logger.debug("[XAUUSD] macro shock context error: %s", e)
        return out

    def _volume_profile_proxy(self, df_h1, current_price: float) -> dict:
        out = {"hvn": [], "lvn": []}
        if df_h1 is None or getattr(df_h1, "empty", True):
            return out
        try:
            lookback = max(40, int(getattr(config, "XAUUSD_LIQUIDITY_VP_LOOKBACK_H1", 120)))
            bins_n = max(8, int(getattr(config, "XAUUSD_LIQUIDITY_VP_BINS", 24)))
            d = self._coerce_utc_index(df_h1).tail(lookback).copy()
            if d.empty:
                return out
            tp = (pd.to_numeric(d["high"], errors="coerce") + pd.to_numeric(d["low"], errors="coerce") + pd.to_numeric(d["close"], errors="coerce")) / 3.0
            vol = pd.to_numeric(d.get("volume"), errors="coerce").fillna(0.0)
            tp = tp.dropna()
            if tp.empty:
                return out
            pmin = float(tp.min()); pmax = float(tp.max())
            if pmax <= pmin:
                return out
            cats = pd.cut(tp, bins=bins_n, include_lowest=True, duplicates="drop")
            bucket_vol = vol.groupby(cats, observed=False).sum()
            rows = []
            for interval, vv in bucket_vol.items():
                if pd.isna(vv) or float(vv) <= 0:
                    continue
                mid = float((interval.left + interval.right) / 2.0)
                rows.append((mid, float(vv), abs(mid - float(current_price))))
            if not rows:
                return out
            rows.sort(key=lambda x: x[1], reverse=True)
            out["hvn"] = [round(r[0], 2) for r in rows[:2]]
            low_rows = sorted(rows, key=lambda x: (x[1], x[2]))
            out["lvn"] = [round(r[0], 2) for r in low_rows[:2]]
        except Exception as e:
            logger.debug("[XAUUSD] volume profile proxy error: %s", e)
        return out

    def _build_liquidity_map(self, current_price: float, df_h1, df_m5, h1_ta=None) -> dict:
        out = {
            "enabled": bool(getattr(config, "XAUUSD_LIQUIDITY_MAP_ENABLED", True)),
            "kill_zone": {"label": "off_kill_zone", "active": False},
            "levels": {},
            "sessions": {},
            "comex": {},
            "volume_profile": {},
            "imbalance": {},
            "sweep_probability": {"score": 0, "label": "low", "reasons": []},
        }
        if not out["enabled"]:
            return out
        try:
            now = datetime.now(timezone.utc)
            d_h1 = self._coerce_utc_index(df_h1) if df_h1 is not None else None
            d_m5 = self._coerce_utc_index(df_m5) if df_m5 is not None else None
            atr_h1 = None
            if h1_ta is not None and not getattr(h1_ta, "empty", True):
                atr_h1 = float(h1_ta.iloc[-1].get("atr_14", 0) or 0)
            elif d_h1 is not None and not d_h1.empty:
                try:
                    atr_h1 = float(ta.add_all(d_h1.copy()).iloc[-1].get("atr_14", 0) or 0)
                except Exception:
                    atr_h1 = None
            if not atr_h1 or atr_h1 <= 0:
                atr_h1 = max(5.0, abs(float(current_price)) * 0.002)

            if d_h1 is not None and not d_h1.empty:
                daily = d_h1.resample("1D").agg({"high": "max", "low": "min", "close": "last"}).dropna()
                today_floor = now.replace(hour=0, minute=0, second=0, microsecond=0)
                completed_days = daily[daily.index < today_floor]
                if not completed_days.empty:
                    pd_bar = completed_days.iloc[-1]
                    out["levels"].update({"pdh": round(float(pd_bar["high"]), 2), "pdl": round(float(pd_bar["low"]), 2)})
                    week_days = completed_days.tail(5)
                    if not week_days.empty:
                        out["levels"].update({"pwh": round(float(week_days["high"].max()), 2), "pwl": round(float(week_days["low"].min()), 2)})
                out["volume_profile"] = self._volume_profile_proxy(d_h1, current_price)
                try:
                    h1_enriched = h1_ta if h1_ta is not None else ta.add_all(d_h1.copy())
                    smc_ctx = smc.analyze(h1_enriched)
                    bull_fvgs = [f for f in (smc_ctx.fair_value_gaps or []) if getattr(f, "direction", "") == "bullish"]
                    bear_fvgs = [f for f in (smc_ctx.fair_value_gaps or []) if getattr(f, "direction", "") == "bearish"]
                    if bull_fvgs:
                        f = min(bull_fvgs, key=lambda x: min(abs(float(current_price) - float(x.lower)), abs(float(current_price) - float(x.upper))))
                        out["imbalance"]["nearest_bull_fvg"] = [round(float(f.lower), 2), round(float(f.upper), 2)]
                    if bear_fvgs:
                        f = min(bear_fvgs, key=lambda x: min(abs(float(current_price) - float(x.lower)), abs(float(current_price) - float(x.upper))))
                        out["imbalance"]["nearest_bear_fvg"] = [round(float(f.lower), 2), round(float(f.upper), 2)]
                except Exception as e:
                    logger.debug("[XAUUSD] imbalance map error: %s", e)

            if d_m5 is not None and not d_m5.empty:
                day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
                day1 = day0 + timedelta(days=1)
                day_m5 = self._slice_time_window(d_m5, day0, day1)
                windows = {"asia": (0, 0, 8, 0), "london": (7, 0, 13, 30), "new_york": (13, 30, 21, 0)}
                for name, (sh, smn, eh, em) in windows.items():
                    s = day0 + timedelta(hours=sh, minutes=smn)
                    e = day0 + timedelta(hours=eh, minutes=em)
                    out["sessions"][name] = self._hilo_summary(self._slice_time_window(day_m5, s, e)) or {}
                comex_start = day0 + timedelta(hours=13, minutes=30)
                comex_or_end = comex_start + timedelta(minutes=30)
                comex_end = day0 + timedelta(hours=20, minutes=0)
                comex_or = self._slice_time_window(day_m5, comex_start, comex_or_end)
                comex_run = self._slice_time_window(day_m5, comex_start, min(comex_end, now + timedelta(minutes=5)))
                out["comex"] = {"or_30m": self._hilo_summary(comex_or) or {}, "session_vwap": None}
                vwap = self._session_vwap(comex_run)
                if vwap is not None:
                    out["comex"]["session_vwap"] = round(float(vwap), 2)
                hhmm = now.strftime("%H:%M")
                if "13:30" <= hhmm <= "15:30":
                    out["kill_zone"] = {"label": "new_york_open_drive", "active": True}
                elif "12:00" <= hhmm <= "15:00":
                    out["kill_zone"] = {"label": "new_york_kill_zone", "active": True}
                elif "07:00" <= hhmm <= "10:00":
                    out["kill_zone"] = {"label": "london_kill_zone", "active": True}

            score = 0
            reasons = []
            rounds = self._nearest_round_levels(current_price)
            nearest_round_dist = min(abs(float(current_price) - float(rounds["nearest_50"])), abs(float(current_price) - float(rounds["nearest_100"])))
            if nearest_round_dist <= 0.30 * atr_h1:
                score += 22; reasons.append("near_round_number")
            for k in ("pdh", "pdl", "pwh", "pwl"):
                lv = out.get("levels", {}).get(k)
                if lv is not None and abs(float(current_price) - float(lv)) <= 0.40 * atr_h1:
                    score += 18; reasons.append(f"near_{k}")
            if out.get("kill_zone", {}).get("active"):
                score += 10; reasons.append(str(out["kill_zone"].get("label")))
            sweep = self._recent_liquidity_sweep(d_m5, current_price)
            if sweep.get("detected"):
                score += 28; reasons.append(str(sweep.get("reason") or "recent_sweep")); out["recent_m5_sweep"] = sweep
            if h1_ta is not None and not getattr(h1_ta, "empty", True):
                last = h1_ta.iloc[-1]
                ema21 = float(last.get("ema_21", current_price) or current_price)
                bb_pct = float(last.get("bb_pct", 0.5) or 0.5)
                ext_atr = abs(float(current_price) - ema21) / max(1e-9, atr_h1)
                out["extension"] = {"h1_ema21_atr": round(ext_atr, 2), "bb_pct": round(bb_pct, 3)}
                if ext_atr >= 1.0:
                    score += 12; reasons.append("h1_extension")
                if bb_pct >= 0.92 or bb_pct <= 0.08:
                    score += 8; reasons.append("bb_extreme")
            label = "low"
            if score >= 75:
                label = "extreme"
            elif score >= 55:
                label = "high"
            elif score >= 30:
                label = "medium"
            out["round_levels"] = {"r50": round(float(rounds["nearest_50"]), 2), "r100": round(float(rounds["nearest_100"]), 2)}
            out["atr_h1"] = round(float(atr_h1), 2)
            out["sweep_probability"] = {"score": int(score), "label": label, "reasons": reasons[:6]}
        except Exception as e:
            logger.debug("[XAUUSD] liquidity map build error: %s", e)
        return out


    @staticmethod
    def _nearest_round_levels(current_price: float) -> dict:
        p = float(current_price)
        return {
            "nearest_50": float(round(p / 50.0) * 50.0),
            "nearest_100": float(round(p / 100.0) * 100.0),
        }

    def _nearby_usd_event_risk(self) -> tuple[list, int]:
        try:
            from market.economic_calendar import economic_calendar
            now = datetime.now(timezone.utc)
            window_min = max(5, int(getattr(config, "XAUUSD_TRAP_EVENT_WINDOW_MIN", 30)))
            hits = []
            for ev in economic_calendar.fetch_events():
                if str(getattr(ev, "currency", "")).upper() != "USD":
                    continue
                if str(getattr(ev, "impact", "")).lower() != "high":
                    continue
                delta_min = int(abs((ev.time_utc - now).total_seconds()) // 60)
                if delta_min <= window_min:
                    hits.append((ev, delta_min))
            hits.sort(key=lambda x: x[1])
            return [e for e, _ in hits[:3]], (hits[0][1] if hits else -1)
        except Exception:
            return [], -1

    def _recent_liquidity_sweep(self, df_m5, current_price: float) -> dict:
        out = {
            "detected": False,
            "side": None,
            "bars_ago": None,
            "wick_ratio": None,
            "vol_ratio": None,
            "reason": "none",
            "trigger_level": None,
            "sweep_high": None,
            "sweep_low": None,
            "sweep_open": None,
            "sweep_close": None,
            "sweep_time": None,
        }
        if df_m5 is None or getattr(df_m5, "empty", True) or len(df_m5) < 40:
            return out
        try:
            d = ta.add_all(df_m5.copy())
            n = len(d)
            lookback = max(10, int(getattr(config, "XAUUSD_TRAP_REJECTION_M5_LOOKBACK", 36)))
            recent_bars = max(2, int(getattr(config, "XAUUSD_TRAP_REJECTION_RECENT_BARS", 4)))
            wick_min = float(getattr(config, "XAUUSD_TRAP_REJECTION_WICK_RATIO", 0.45))
            end_i = n - 1  # exclude latest bar (may still be forming)
            start_i = max(lookback + 2, end_i - recent_bars)
            for i in range(start_i, end_i):
                bar = d.iloc[i]
                prev = d.iloc[max(0, i - lookback):i]
                if len(prev) < 5:
                    continue
                hi = float(bar["high"]); lo = float(bar["low"]); op = float(bar["open"]); cl = float(bar["close"])
                rng = max(1e-9, hi - lo)
                upper_wick = hi - max(op, cl)
                lower_wick = min(op, cl) - lo
                prev_hi = float(prev["high"].max())
                prev_lo = float(prev["low"].min())
                vol_ratio = float(bar.get("vol_ratio", 1.0) or 1.0)

                if hi > prev_hi and cl < prev_hi and cl < op and (upper_wick / rng) >= wick_min:
                    out.update({
                        "detected": True,
                        "side": "bearish_rejection",
                        "bars_ago": int((n - 2) - i),
                        "wick_ratio": round(upper_wick / rng, 3),
                        "vol_ratio": round(vol_ratio, 2),
                        "reason": "m5_sweep_above_high_then_reject",
                        "trigger_level": round(prev_hi, 4),
                        "sweep_high": round(hi, 4),
                        "sweep_low": round(lo, 4),
                        "sweep_open": round(op, 4),
                        "sweep_close": round(cl, 4),
                        "sweep_time": str(d.index[i]),
                    })
                    return out
                if lo < prev_lo and cl > prev_lo and cl > op and (lower_wick / rng) >= wick_min:
                    out.update({
                        "detected": True,
                        "side": "bullish_rejection",
                        "bars_ago": int((n - 2) - i),
                        "wick_ratio": round(lower_wick / rng, 3),
                        "vol_ratio": round(vol_ratio, 2),
                        "reason": "m5_sweep_below_low_then_reject",
                        "trigger_level": round(prev_lo, 4),
                        "sweep_high": round(hi, 4),
                        "sweep_low": round(lo, 4),
                        "sweep_open": round(op, 4),
                        "sweep_close": round(cl, 4),
                        "sweep_time": str(d.index[i]),
                    })
                    return out
        except Exception as e:
            logger.debug("[XAUUSD] liquidity sweep analysis error: %s", e)
        return out

    def _apply_trade_location_guard(self, signal: TradeSignal, current_price: float, key_levels: dict, df_h1, df_m5) -> tuple[Optional[TradeSignal], dict]:
        guard = {
            "enabled": bool(getattr(config, "XAUUSD_SMART_TRAP_GUARD_ENABLED", True)),
            "penalty": 0.0,
            "blocked": False,
            "near_round": False,
            "no_chase": False,
            "event_risk": False,
            "news_freeze": {},
            "macro_shock": {},
            "liq_map": {},
            "sweep": {},
            "warnings": [],
        }
        if not guard["enabled"]:
            return signal, guard
        try:
            h1_ta = ta.add_all(df_h1.copy()) if df_h1 is not None and not df_h1.empty else None
            h1_last = h1_ta.iloc[-1] if h1_ta is not None and not h1_ta.empty else None
            atr_h1 = float(getattr(signal, "atr", 0) or 0)
            if (not atr_h1) and h1_last is not None:
                atr_h1 = float(h1_last.get("atr_14", 0) or 0)
            atr_h1 = atr_h1 if atr_h1 > 0 else max(5.0, abs(float(current_price)) * 0.002)

            near_round_atr = float(getattr(config, "XAUUSD_TRAP_NEAR_ROUND_ATR", 0.35))
            no_chase_ema21_atr = float(getattr(config, "XAUUSD_TRAP_NO_CHASE_EMA21_ATR", 1.0))
            no_chase_bb_pct = float(getattr(config, "XAUUSD_TRAP_NO_CHASE_BB_PCT", 0.92))

            rr = self._nearest_round_levels(current_price)
            nearest50 = float(rr["nearest_50"])
            nearest100 = float(rr["nearest_100"])
            dist_to_50_atr = abs(float(current_price) - nearest50) / max(1e-9, atr_h1)
            dist_to_100_atr = abs(float(current_price) - nearest100) / max(1e-9, atr_h1)
            guard["round_levels"] = {"50": nearest50, "100": nearest100}

            liq_map = self._build_liquidity_map(current_price, df_h1, df_m5, h1_ta=h1_ta)
            guard["liq_map"] = liq_map
            macro_ctx = self._macro_shock_context(df_m5)
            guard["macro_shock"] = macro_ctx
            news_freeze = self._news_freeze_context()
            guard["news_freeze"] = news_freeze

            if signal.direction == "long":
                dist_res = float(key_levels.get("distance_to_res", 9999) or 9999)
                dist_res_atr = dist_res / max(1e-9, atr_h1)
                if dist_res_atr <= near_round_atr or min(dist_to_50_atr, dist_to_100_atr) <= near_round_atr:
                    guard["near_round"] = True
                    guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_ROUND_RES", 8))
                    guard["warnings"].append(
                        f"⚠️ Round-number liquidity zone overhead: limited room to resistance (${key_levels.get('nearest_resistance', 0):.0f})"
                    )
            elif signal.direction == "short":
                dist_sup = float(key_levels.get("distance_to_sup", 9999) or 9999)
                dist_sup_atr = dist_sup / max(1e-9, atr_h1)
                if dist_sup_atr <= near_round_atr or min(dist_to_50_atr, dist_to_100_atr) <= near_round_atr:
                    guard["near_round"] = True
                    guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_ROUND_RES", 8))
                    guard["warnings"].append(
                        f"⚠️ Round-number liquidity zone below: limited room to support (${key_levels.get('nearest_support', 0):.0f})"
                    )

            if h1_last is not None:
                entry_tf_lbl = self._tf_label(str(getattr(config, "XAUUSD_ENTRY_TF", "1h")))
                ema21 = float(h1_last.get("ema_21", current_price) or current_price)
                bb_pct = float(h1_last.get("bb_pct", 0.5) or 0.5)
                if signal.direction == "long":
                    ext_atr = (float(current_price) - ema21) / max(1e-9, atr_h1)
                    if ext_atr >= no_chase_ema21_atr and bb_pct >= no_chase_bb_pct:
                        guard["no_chase"] = True
                        guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_NO_CHASE", 10))
                        guard["warnings"].append(
                            f"⚠️ No-chase: price stretched {ext_atr:.2f} ATR above {entry_tf_lbl} EMA21 (BB% {bb_pct:.2f})"
                        )
                else:
                    ext_atr = (ema21 - float(current_price)) / max(1e-9, atr_h1)
                    if ext_atr >= no_chase_ema21_atr and bb_pct <= (1.0 - no_chase_bb_pct):
                        guard["no_chase"] = True
                        guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_NO_CHASE", 10))
                        guard["warnings"].append(
                            f"⚠️ No-chase: price stretched {ext_atr:.2f} ATR below {entry_tf_lbl} EMA21 (BB% {bb_pct:.2f})"
                        )
                guard["ext_atr"] = round(ext_atr, 3)
                guard["bb_pct"] = round(bb_pct, 3)

            sweep = self._recent_liquidity_sweep(df_m5, current_price)
            guard["sweep"] = sweep
            if sweep.get("detected"):
                bad_for_long = signal.direction == "long" and sweep.get("side") == "bearish_rejection"
                bad_for_short = signal.direction == "short" and sweep.get("side") == "bullish_rejection"
                if bad_for_long or bad_for_short:
                    guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_SWEEP", 18))
                    guard["warnings"].append(
                        f"⚠️ M5 liquidity sweep/rejection detected ({sweep.get('reason')}, {sweep.get('bars_ago')} bars ago, wick {sweep.get('wick_ratio')}, vol {sweep.get('vol_ratio')}x)"
                    )
                    if bool(getattr(config, "XAUUSD_TRAP_BLOCK_ON_SWEEP", True)) and (guard["near_round"] or guard["no_chase"]):
                        guard["blocked"] = True
                        guard["warnings"].append("⛔ Trap guard: sweep + round-level/chase confluence")

            evs, nearest_ev_min = self._nearby_usd_event_risk()
            if evs:
                guard["event_risk"] = True
                guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_EVENT", 12))
                guard["warnings"].append(f"⚠️ High-impact USD event proximity ({nearest_ev_min}m): stop-sweep risk elevated")

            sweep_prob = int((liq_map.get("sweep_probability") or {}).get("score", 0) or 0)
            if sweep_prob >= int(getattr(config, "XAUUSD_TRAP_SWEEP_PROB_BLOCK_SCORE", 78)) and (guard["near_round"] or guard["no_chase"]):
                guard["blocked"] = True
                guard["warnings"].append(f"⛔ Liquidity map trap block: sweep probability {sweep_prob} with chase/round confluence")
            elif sweep_prob >= 55:
                guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_SWEEP_PROB", 8))
                guard["warnings"].append(f"⚠️ Liquidity map sweep probability elevated ({sweep_prob}/100)")

            if macro_ctx.get("available"):
                macro_bad = bool(macro_ctx.get("adverse_for_long")) if signal.direction == "long" else bool(macro_ctx.get("adverse_for_short"))
                if macro_bad:
                    guard["penalty"] += float(getattr(config, "XAUUSD_TRAP_PENALTY_MACRO_SHOCK", 10))
                    guard["warnings"].append(f"⚠️ Macro shock filter: {macro_ctx.get('summary', 'adverse cross-asset move')}")

            if news_freeze.get("active"):
                nearest = int(news_freeze.get("nearest_min", -1))
                ev_name = str((news_freeze.get("events") or ["USD high-impact event"])[0])[:64]
                guard["warnings"].append(f"⚠️ News freeze window active ({nearest}m): {ev_name}")
                if bool(getattr(config, "XAUUSD_TRAP_BLOCK_ON_NEWS_FREEZE", True)):
                    guard["blocked"] = True
                    guard["warnings"].append("⛔ XAU news freeze: no fresh entries into high-impact USD window")

            signal.raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            signal.raw_scores.update({
                "xau_guard_penalty": round(float(guard["penalty"]), 2),
                "xau_guard_blocked": bool(guard["blocked"]),
                "xau_guard_near_round": bool(guard["near_round"]),
                "xau_guard_no_chase": bool(guard["no_chase"]),
                "xau_guard_event_risk": bool(guard["event_risk"]),
                "xau_guard_sweep": bool((guard.get("sweep") or {}).get("detected")),
                "xau_guard_news_freeze": bool((guard.get("news_freeze") or {}).get("active")),
                "xau_guard_macro_shock": bool((guard.get("macro_shock") or {}).get("adverse_for_long") or (guard.get("macro_shock") or {}).get("adverse_for_short")),
                "xau_liq_sweep_prob": int(((guard.get("liq_map") or {}).get("sweep_probability") or {}).get("score", 0) or 0),
            })

            liq_prob = (guard.get("liq_map") or {}).get("sweep_probability") or {}
            if liq_prob:
                kz_label = str((guard.get("liq_map") or {}).get("kill_zone", {}).get("label", "off_kill_zone")).replace("_", " ")
                signal.reasons.append(f"🧭 Liquidity map: sweep risk {str(liq_prob.get('label','low')).upper()} ({liq_prob.get('score',0)}/100) in {kz_label}")
            if guard["near_round"]:
                signal.reasons.append(f"🧭 Trap map: round-number liquidity zone near {nearest50:.0f}/{nearest100:.0f}; prefer pullback/retest entry")
            for w in guard["warnings"]:
                if w not in signal.warnings:
                    signal.warnings.append(w)
            if guard["penalty"] > 0:
                before = float(signal.confidence)
                signal.confidence = max(0.0, before - float(guard["penalty"]))
                signal.warnings.append(f"⚠️ XAU trap-risk penalty applied: -{guard['penalty']:.1f} conf ({before:.1f}% → {signal.confidence:.1f}%)")

            if guard["blocked"]:
                logger.info(
                    "[XAUUSD] Trap guard blocked %s @ %.2f | penalty=%.1f near_round=%s no_chase=%s sweep=%s event=%s",
                    str(signal.direction).upper(), float(current_price), float(guard["penalty"]),
                    bool(guard["near_round"]), bool(guard["no_chase"]), bool((guard.get("sweep") or {}).get("detected")), bool(guard["event_risk"]),
                )
                return None, guard
            if guard["penalty"] > 0:
                logger.info(
                    "[XAUUSD] Trap guard penalty %s @ %.2f | penalty=%.1f conf=%.1f near_round=%s no_chase=%s sweep=%s event=%s",
                    str(signal.direction).upper(), float(current_price), float(guard["penalty"]), float(signal.confidence),
                    bool(guard["near_round"]), bool(guard["no_chase"]), bool((guard.get("sweep") or {}).get("detected")), bool(guard["event_risk"]),
                )
        except Exception as e:
            logger.warning("[XAUUSD] Trap guard error: %s", e)
        return signal, guard

    @staticmethod
    def _as_float(value, default: float = 0.0) -> float:
        try:
            v = float(value)
            return v if np.isfinite(v) else float(default)
        except Exception:
            return float(default)

    def _collect_behavior_targets(
        self,
        *,
        direction: str,
        entry: float,
        key_levels: dict,
        liq_map: dict,
        atr_m5: float,
    ) -> list[float]:
        levels: list[float] = []

        def _add(px):
            try:
                p = float(px)
            except Exception:
                return
            if not np.isfinite(p) or p <= 0:
                return
            levels.append(p)

        km = key_levels or {}
        lm = liq_map or {}
        lvl = lm.get("levels") or {}
        round_levels = lm.get("round_levels") or {}
        vp = lm.get("volume_profile") or {}

        if direction == "long":
            _add(km.get("nearest_resistance"))
            _add(km.get("asian_high"))
            _add(lvl.get("pdh"))
            _add(lvl.get("pwh"))
            for px in (vp.get("hvn") or []):
                _add(px)
            for px in round_levels.values():
                _add(px)
        else:
            _add(km.get("nearest_support"))
            _add(km.get("asian_low"))
            _add(lvl.get("pdl"))
            _add(lvl.get("pwl"))
            for px in (vp.get("hvn") or []):
                _add(px)
            for px in round_levels.values():
                _add(px)

        min_sep = max(0.15 * max(1e-9, float(atr_m5)), abs(float(entry)) * 0.0002)
        if direction == "long":
            candidates = sorted([p for p in levels if p > (entry + 0.25 * atr_m5)])
        else:
            candidates = sorted([p for p in levels if p < (entry - 0.25 * atr_m5)], reverse=True)

        deduped: list[float] = []
        for px in candidates:
            if not deduped or abs(px - deduped[-1]) >= min_sep:
                deduped.append(px)
        return deduped

    def _behavioral_fallback_signal(
        self,
        *,
        current_price: float,
        df_h1,
        df_m5,
        df_h4=None,
        df_d1=None,
        key_levels: Optional[dict] = None,
        session_info: Optional[dict] = None,
        trend_tf_label: str = "1d",
        structure_tf_label: str = "4h",
        entry_tf_label: str = "1h",
    ) -> tuple[Optional[TradeSignal], dict]:
        diag = {
            "engine": "behavioral_fallback_v2",
            "enabled": bool(getattr(config, "XAUUSD_BEHAVIORAL_FALLBACK_ENABLED", True)),
            "status": "no_signal",
            "reason": "unmet",
            "selected_direction": None,
            "long": {},
            "short": {},
            "timeframes": {
                "trend": self._tf_label(trend_tf_label),
                "structure": self._tf_label(structure_tf_label),
                "entry": self._tf_label(entry_tf_label),
            },
        }
        if not diag["enabled"]:
            diag["reason"] = "fallback_disabled"
            return None, diag
        if df_h1 is None or getattr(df_h1, "empty", True) or df_m5 is None or getattr(df_m5, "empty", True):
            diag["reason"] = "missing_h1_or_m5_data"
            return None, diag

        try:
            h1_ta = ta.add_all(df_h1.copy())
            m5_ta = ta.add_all(df_m5.copy())
            if m5_ta is None or m5_ta.empty or len(m5_ta) < 40:
                diag["reason"] = "insufficient_m5_data"
                return None, diag

            h1_last = h1_ta.iloc[-1]
            m5_last = m5_ta.iloc[-1]
            atr_h1 = self._as_float(h1_last.get("atr_14"), max(5.0, abs(float(current_price)) * 0.002))
            atr_m5 = self._as_float(m5_last.get("atr_14"), max(1.0, abs(float(current_price)) * 0.0008))
            if atr_h1 <= 0 or atr_m5 <= 0:
                diag["reason"] = "invalid_atr"
                return None, diag

            km = dict(key_levels or {})
            if not km:
                km = self.analyze_key_levels(float(current_price))
            sessions = dict(session_info or {})

            liq_map = self._build_liquidity_map(float(current_price), df_h1, df_m5, h1_ta=h1_ta)
            macro_ctx = self._macro_shock_context(df_m5)
            news_ctx = self._news_freeze_context()
            sweep = self._recent_liquidity_sweep(df_m5, float(current_price))
            kill_zone = str((liq_map.get("kill_zone") or {}).get("label", "off_kill_zone"))
            kill_zone_active = bool((liq_map.get("kill_zone") or {}).get("active"))

            bb_width_series = pd.to_numeric(m5_ta.get("bb_width"), errors="coerce").dropna()
            bb_width_rank = 1.0
            if len(bb_width_series) >= 80:
                bb_width_rank = float(bb_width_series.tail(192).rank(pct=True).iloc[-1])
            tr_last = max(
                self._as_float(m5_last.get("high")) - self._as_float(m5_last.get("low")),
                abs(self._as_float(m5_last.get("high")) - self._as_float(m5_ta["close"].iloc[-2], float(current_price))),
                abs(self._as_float(m5_last.get("low")) - self._as_float(m5_ta["close"].iloc[-2], float(current_price))),
            ) if len(m5_ta) > 1 else self._as_float(m5_last.get("high")) - self._as_float(m5_last.get("low"))
            tr_atr = tr_last / max(1e-9, atr_m5)
            compress = bool(
                bb_width_rank <= float(getattr(config, "XAUUSD_BEHAVIORAL_TRIGGER_BB_PCTL_MAX", 0.25))
                and tr_atr <= float(getattr(config, "XAUUSD_BEHAVIORAL_TRIGGER_TR_ATR_MAX", 0.85))
            )

            rr = self._nearest_round_levels(float(current_price))
            dist_round_atr = min(
                abs(float(current_price) - float(rr["nearest_50"])) / max(1e-9, atr_h1),
                abs(float(current_price) - float(rr["nearest_100"])) / max(1e-9, atr_h1),
            )
            near_round = dist_round_atr <= float(getattr(config, "XAUUSD_TRAP_NEAR_ROUND_ATR", 0.35))

            trend_entry = ta.determine_trend(h1_ta)
            trend_structure = ta.determine_trend(ta.add_all(df_h4.copy())) if df_h4 is not None and not getattr(df_h4, "empty", True) else "ranging"
            trend_regime = ta.determine_trend(ta.add_all(df_d1.copy())) if df_d1 is not None and not getattr(df_d1, "empty", True) else "ranging"
            bb_h1 = self._as_float(h1_last.get("bb_pct"), 0.5)
            ema21_h1 = self._as_float(h1_last.get("ema_21"), float(current_price))
            ext_long = (float(current_price) - ema21_h1) / max(1e-9, atr_h1)
            ext_short = (ema21_h1 - float(current_price)) / max(1e-9, atr_h1)
            close_m5 = self._as_float(m5_last.get("close"), float(current_price))
            ema9_m5 = self._as_float(m5_last.get("ema_9"), close_m5)
            rsi_m5 = self._as_float(m5_last.get("rsi_14"), 50.0)

            trend_lbl = self._tf_label(trend_tf_label)
            structure_lbl = self._tf_label(structure_tf_label)
            entry_lbl = self._tf_label(entry_tf_label)

            imbalance = (liq_map.get("imbalance") or {})
            bull_fvg = imbalance.get("nearest_bull_fvg")
            bear_fvg = imbalance.get("nearest_bear_fvg")

            def _zone_dist(zone):
                if not zone or not isinstance(zone, (list, tuple)) or len(zone) < 2:
                    return None
                z0 = self._as_float(zone[0], np.nan)
                z1 = self._as_float(zone[1], np.nan)
                if not np.isfinite(z0) or not np.isfinite(z1):
                    return None
                lo = min(z0, z1)
                hi = max(z0, z1)
                if lo <= float(current_price) <= hi:
                    return 0.0
                return min(abs(float(current_price) - lo), abs(float(current_price) - hi))

            bull_fvg_dist = _zone_dist(bull_fvg)
            bear_fvg_dist = _zone_dist(bear_fvg)
            no_chase_ema21_atr = float(getattr(config, "XAUUSD_TRAP_NO_CHASE_EMA21_ATR", 1.0))
            no_chase_bb_pct = float(getattr(config, "XAUUSD_TRAP_NO_CHASE_BB_PCT", 0.92))

            candidates = {
                "long": {"score": 0.0, "reasons": [], "warnings": [], "trigger": False, "entry_hint": None},
                "short": {"score": 0.0, "reasons": [], "warnings": [], "trigger": False, "entry_hint": None},
            }
            trend_votes = {"long": 0, "short": 0}

            def _add(direction: str, score: float, reason: str):
                candidates[direction]["score"] = float(candidates[direction]["score"]) + float(score)
                if reason:
                    candidates[direction]["reasons"].append(str(reason))

            if compress:
                _add("long", 6, f"✅ Compression: BB width pct {bb_width_rank:.2f}, TR/ATR {tr_atr:.2f}")
                _add("short", 6, f"✅ Compression: BB width pct {bb_width_rank:.2f}, TR/ATR {tr_atr:.2f}")
            if near_round and compress:
                _add("long", 4, "✅ Compression near round-number liquidity magnet")
                _add("short", 4, "✅ Compression near round-number liquidity magnet")
            if kill_zone_active:
                _add("long", 6, f"✅ Kill-zone active: {kill_zone.replace('_', ' ')}")
                _add("short", 6, f"✅ Kill-zone active: {kill_zone.replace('_', ' ')}")

            if trend_regime == "bullish":
                _add("long", 6, f"✅ {trend_lbl} regime supports longs")
                trend_votes["long"] += 1
            elif trend_regime == "bearish":
                _add("short", 6, f"✅ {trend_lbl} regime supports shorts")
                trend_votes["short"] += 1
            if trend_structure == "bullish":
                _add("long", 5, f"✅ {structure_lbl} structure tilts bullish")
                trend_votes["long"] += 1
            elif trend_structure == "bearish":
                _add("short", 5, f"✅ {structure_lbl} structure tilts bearish")
                trend_votes["short"] += 1
            if trend_entry == "bullish":
                _add("long", 3, f"✅ {entry_lbl} momentum supportive")
                trend_votes["long"] += 1
            elif trend_entry == "bearish":
                _add("short", 3, f"✅ {entry_lbl} momentum supportive")
                trend_votes["short"] += 1

            if sweep.get("detected"):
                if sweep.get("side") == "bullish_rejection":
                    _add("long", 30, f"✅ M5 sweep/reject up trigger ({sweep.get('reason')})")
                    candidates["long"]["trigger"] = True
                elif sweep.get("side") == "bearish_rejection":
                    _add("short", 30, f"✅ M5 sweep/reject down trigger ({sweep.get('reason')})")
                    candidates["short"]["trigger"] = True

            if bull_fvg_dist is not None and bull_fvg_dist <= (1.2 * atr_m5):
                _add("long", 10, f"✅ Bullish FVG retest proximity ({bull_fvg_dist / max(1e-9, atr_m5):.2f} ATR)")
                try:
                    lo = min(float(bull_fvg[0]), float(bull_fvg[1]))
                    hi = max(float(bull_fvg[0]), float(bull_fvg[1]))
                    candidates["long"]["entry_hint"] = min(float(current_price), hi - 0.20 * (hi - lo))
                except Exception:
                    pass
            if bear_fvg_dist is not None and bear_fvg_dist <= (1.2 * atr_m5):
                _add("short", 10, f"✅ Bearish FVG retest proximity ({bear_fvg_dist / max(1e-9, atr_m5):.2f} ATR)")
                try:
                    lo = min(float(bear_fvg[0]), float(bear_fvg[1]))
                    hi = max(float(bear_fvg[0]), float(bear_fvg[1]))
                    candidates["short"]["entry_hint"] = max(float(current_price), lo + 0.20 * (hi - lo))
                except Exception:
                    pass

            if macro_ctx.get("available"):
                if bool(macro_ctx.get("adverse_for_long")):
                    candidates["long"]["score"] -= 14.0
                    candidates["long"]["warnings"].append(f"⚠️ Macro shock adverse to long ({macro_ctx.get('summary')})")
                if bool(macro_ctx.get("adverse_for_short")):
                    candidates["short"]["score"] -= 14.0
                    candidates["short"]["warnings"].append(f"⚠️ Macro shock adverse to short ({macro_ctx.get('summary')})")

            if ext_long >= no_chase_ema21_atr and bb_h1 >= no_chase_bb_pct:
                candidates["long"]["score"] -= 10.0
                candidates["long"]["warnings"].append(f"⚠️ No-chase long: +{ext_long:.2f} ATR above {entry_lbl} EMA21 (BB% {bb_h1:.2f})")
            if ext_short >= no_chase_ema21_atr and bb_h1 <= (1.0 - no_chase_bb_pct):
                candidates["short"]["score"] -= 10.0
                candidates["short"]["warnings"].append(f"⚠️ No-chase short: +{ext_short:.2f} ATR below {entry_lbl} EMA21 (BB% {bb_h1:.2f})")

            dist_res_atr = self._as_float((km or {}).get("distance_to_res"), 9999.0) / max(1e-9, atr_h1)
            dist_sup_atr = self._as_float((km or {}).get("distance_to_sup"), 9999.0) / max(1e-9, atr_h1)
            near_round_atr = float(getattr(config, "XAUUSD_TRAP_NEAR_ROUND_ATR", 0.35))
            if dist_res_atr <= near_round_atr:
                candidates["long"]["score"] -= 8.0
                candidates["long"]["warnings"].append("⚠️ Overhead liquidity too close for long")
            if dist_sup_atr <= near_round_atr:
                candidates["short"]["score"] -= 8.0
                candidates["short"]["warnings"].append("⚠️ Downside liquidity too close for short")

            require_sweep = bool(getattr(config, "XAUUSD_BEHAVIORAL_REQUIRE_SWEEP_TRIGGER", True))
            secondary_long = bool(compress and kill_zone_active and bull_fvg_dist is not None and bull_fvg_dist <= 0.8 * atr_m5)
            secondary_short = bool(compress and kill_zone_active and bear_fvg_dist is not None and bear_fvg_dist <= 0.8 * atr_m5)
            if not require_sweep:
                candidates["long"]["trigger"] = bool(candidates["long"]["trigger"] or secondary_long)
                candidates["short"]["trigger"] = bool(candidates["short"]["trigger"] or secondary_short)

            # Reversal trigger to capture strong pullback opportunities in dominant trend
            # without blindly catching a falling knife.
            reversal_enabled = bool(getattr(config, "XAUUSD_BEHAVIORAL_REVERSAL_TRIGGER_ENABLED", True))
            rev_ext_min = float(getattr(config, "XAUUSD_BEHAVIORAL_REVERSAL_MIN_EXTENSION_ATR", 0.45))
            rev_rsi_long_min = float(getattr(config, "XAUUSD_BEHAVIORAL_REVERSAL_RSI_LONG_MIN", 49.5))
            rev_rsi_short_max = float(getattr(config, "XAUUSD_BEHAVIORAL_REVERSAL_RSI_SHORT_MAX", 50.5))
            if reversal_enabled:
                pullback_long = bool(ext_short >= rev_ext_min and bull_fvg_dist is not None and bull_fvg_dist <= 1.0 * atr_m5)
                pullback_short = bool(ext_long >= rev_ext_min and bear_fvg_dist is not None and bear_fvg_dist <= 1.0 * atr_m5)
                confirm_long = bool(
                    sweep.get("side") == "bullish_rejection"
                    and close_m5 >= (ema9_m5 - 0.10 * atr_m5)
                    and rsi_m5 >= rev_rsi_long_min
                )
                confirm_short = bool(
                    sweep.get("side") == "bearish_rejection"
                    and close_m5 <= (ema9_m5 + 0.10 * atr_m5)
                    and rsi_m5 <= rev_rsi_short_max
                )
                if pullback_long and confirm_long:
                    _add("long", 12, f"✅ Pullback reversal confirmed ({entry_lbl} reclaim + sweep) aligned with {trend_lbl}/{structure_lbl}")
                    candidates["long"]["trigger"] = True
                elif pullback_long:
                    candidates["long"]["warnings"].append(
                        f"⚠️ Pullback long watch: wait for bullish sweep + {entry_lbl} reclaim to avoid knife catch"
                    )
                if pullback_short and confirm_short:
                    _add("short", 12, f"✅ Pullback reversal confirmed ({entry_lbl} reject + sweep) aligned with {trend_lbl}/{structure_lbl}")
                    candidates["short"]["trigger"] = True
                elif pullback_short:
                    candidates["short"]["warnings"].append(
                        f"⚠️ Pullback short watch: wait for bearish sweep + {entry_lbl} reject to avoid knife catch"
                    )

            regime_guard_enabled = bool(getattr(config, "XAUUSD_REGIME_GUARD_ENABLED", True))
            require_struct_align = bool(getattr(config, "XAUUSD_REGIME_GUARD_REQUIRE_STRUCTURE_ALIGN", True))
            regime_votes = {"long": 0, "short": 0}
            if trend_regime == "bullish":
                regime_votes["long"] += 1
            elif trend_regime == "bearish":
                regime_votes["short"] += 1
            if require_struct_align:
                if trend_structure == "bullish":
                    regime_votes["long"] += 1
                elif trend_structure == "bearish":
                    regime_votes["short"] += 1
            dominant_side = None
            if regime_votes["long"] >= 2 and regime_votes["short"] == 0:
                dominant_side = "long"
            elif regime_votes["short"] >= 2 and regime_votes["long"] == 0:
                dominant_side = "short"
            for side in ("long", "short"):
                candidates[side]["regime_blocked"] = False
                candidates[side]["countertrend_confirmed"] = False
            if regime_guard_enabled and dominant_side in {"long", "short"}:
                counter = "short" if dominant_side == "long" else "long"
                counter_confirmed = False
                if counter == "long":
                    counter_confirmed = bool(
                        reversal_enabled
                        and sweep.get("side") == "bullish_rejection"
                        and ext_short >= rev_ext_min
                        and close_m5 >= (ema9_m5 - 0.10 * atr_m5)
                        and rsi_m5 >= rev_rsi_long_min
                    )
                else:
                    counter_confirmed = bool(
                        reversal_enabled
                        and sweep.get("side") == "bearish_rejection"
                        and ext_long >= rev_ext_min
                        and close_m5 <= (ema9_m5 + 0.10 * atr_m5)
                        and rsi_m5 <= rev_rsi_short_max
                    )
                if counter_confirmed:
                    candidates[counter]["countertrend_confirmed"] = True
                    _add(counter, 3, f"✅ Counter-trend reversal confirmed vs dominant {dominant_side.upper()} regime")
                else:
                    candidates[counter]["regime_blocked"] = True
                    candidates[counter]["trigger"] = False
                    candidates[counter]["warnings"].append(
                        f"⚠️ Regime guard blocked {counter.upper()} against dominant {trend_lbl}/{structure_lbl} trend"
                    )

            for side in ("long", "short"):
                score = float(candidates[side]["score"])
                conf = max(0.0, min(95.0, 46.0 + score))
                candidates[side]["confidence"] = round(conf, 1)
                candidates[side]["trigger"] = bool(candidates[side]["trigger"])

            edge_triggered = False
            if bool(getattr(config, "XAUUSD_BEHAVIORAL_EDGE_TRIGGER_ENABLED", True)):
                long_conf = float(candidates["long"]["confidence"])
                short_conf = float(candidates["short"]["confidence"])
                dom_side = "long" if long_conf >= short_conf else "short"
                dom_conf = max(long_conf, short_conf)
                conf_edge = abs(long_conf - short_conf)
                min_dom_conf = float(getattr(config, "XAUUSD_BEHAVIORAL_EDGE_TRIGGER_CONFIDENCE", 68.0))
                min_edge = float(getattr(config, "XAUUSD_BEHAVIORAL_EDGE_TRIGGER_MIN_EDGE", 5.0))
                min_votes = max(1, int(getattr(config, "XAUUSD_BEHAVIORAL_EDGE_TRIGGER_MIN_TREND_VOTES", 2)))
                supporting_context = bool(
                    compress
                    or kill_zone_active
                    or int(trend_votes.get(dom_side, 0)) >= min_votes
                )
                if (not bool(candidates[dom_side]["trigger"])) and dom_conf >= min_dom_conf and conf_edge >= min_edge and supporting_context:
                    candidates[dom_side]["trigger"] = True
                    edge_triggered = True
                    candidates[dom_side]["reasons"].append(
                        f"✅ Dominant-direction edge trigger ({dom_conf:.1f}% confidence, edge {conf_edge:.1f})"
                    )

            min_conf = float(getattr(config, "XAUUSD_BEHAVIORAL_MIN_CONFIDENCE", 62.0))
            min_conf_long = float(getattr(config, "XAUUSD_BEHAVIORAL_MIN_CONFIDENCE_LONG", min_conf))
            min_conf_short = float(getattr(config, "XAUUSD_BEHAVIORAL_MIN_CONFIDENCE_SHORT", min_conf))
            if bool(getattr(config, "XAUUSD_BEHAVIORAL_BALANCE_SIDE_THRESHOLDS", True)):
                balanced = max(float(min_conf_long), float(min_conf_short))
                min_conf_long = balanced
                min_conf_short = balanced
            side_min_conf = {
                "long": max(0.0, min(100.0, float(min_conf_long))),
                "short": max(0.0, min(100.0, float(min_conf_short))),
            }
            valid = [
                side for side in ("long", "short")
                if candidates[side]["trigger"] and float(candidates[side]["confidence"]) >= float(side_min_conf.get(side, min_conf))
            ]

            diag.update({
                "long": {
                    "score": round(float(candidates["long"]["score"]), 2),
                    "confidence": float(candidates["long"]["confidence"]),
                    "trigger": bool(candidates["long"]["trigger"]),
                    "regime_blocked": bool(candidates["long"].get("regime_blocked", False)),
                    "countertrend_confirmed": bool(candidates["long"].get("countertrend_confirmed", False)),
                    "min_confidence": float(side_min_conf["long"]),
                    "passed": bool(
                        bool(candidates["long"]["trigger"])
                        and (not bool(candidates["long"].get("regime_blocked", False)))
                        and float(candidates["long"]["confidence"]) >= float(side_min_conf["long"])
                    ),
                    "reasons": list(candidates["long"]["reasons"][:6]),
                    "warnings": list(candidates["long"]["warnings"][:4]),
                },
                "short": {
                    "score": round(float(candidates["short"]["score"]), 2),
                    "confidence": float(candidates["short"]["confidence"]),
                    "trigger": bool(candidates["short"]["trigger"]),
                    "regime_blocked": bool(candidates["short"].get("regime_blocked", False)),
                    "countertrend_confirmed": bool(candidates["short"].get("countertrend_confirmed", False)),
                    "min_confidence": float(side_min_conf["short"]),
                    "passed": bool(
                        bool(candidates["short"]["trigger"])
                        and (not bool(candidates["short"].get("regime_blocked", False)))
                        and float(candidates["short"]["confidence"]) >= float(side_min_conf["short"])
                    ),
                    "reasons": list(candidates["short"]["reasons"][:6]),
                    "warnings": list(candidates["short"]["warnings"][:4]),
                },
                "compress": bool(compress),
                "near_round": bool(near_round),
                "sweep": dict(sweep or {}),
                "kill_zone": kill_zone,
                "trend_votes": dict(trend_votes),
                "regime_votes": dict(regime_votes),
                "dominant_side": dominant_side,
                "edge_triggered": bool(edge_triggered),
                "macro_summary": str(macro_ctx.get("summary", "unknown")),
            })

            if news_ctx.get("active") and bool(getattr(config, "XAUUSD_TRAP_BLOCK_ON_NEWS_FREEZE", True)):
                diag["reason"] = "news_freeze_active"
                return None, diag
            if not valid:
                diag["gating"] = {
                    "long": {
                        "trigger": bool(candidates["long"]["trigger"]),
                        "regime_blocked": bool(candidates["long"].get("regime_blocked", False)),
                        "confidence": float(candidates["long"]["confidence"]),
                        "min_confidence": float(side_min_conf["long"]),
                        "passed": bool(
                            bool(candidates["long"]["trigger"])
                            and (not bool(candidates["long"].get("regime_blocked", False)))
                            and float(candidates["long"]["confidence"]) >= float(side_min_conf["long"])
                        ),
                    },
                    "short": {
                        "trigger": bool(candidates["short"]["trigger"]),
                        "regime_blocked": bool(candidates["short"].get("regime_blocked", False)),
                        "confidence": float(candidates["short"]["confidence"]),
                        "min_confidence": float(side_min_conf["short"]),
                        "passed": bool(
                            bool(candidates["short"]["trigger"])
                            and (not bool(candidates["short"].get("regime_blocked", False)))
                            and float(candidates["short"]["confidence"]) >= float(side_min_conf["short"])
                        ),
                    },
                }
                diag["reason"] = "no_direction_passed_threshold"
                return None, diag

            if len(valid) > 1:
                edge_min = float(getattr(config, "XAUUSD_BEHAVIORAL_MIN_EDGE", 4.0))
                edge = abs(float(candidates["long"]["confidence"]) - float(candidates["short"]["confidence"]))
                if edge < edge_min:
                    diag["reason"] = "ambiguous_bidirectional_setup"
                    return None, diag

            direction = max(valid, key=lambda s: (float(candidates[s]["confidence"]), float(candidates[s]["score"])))
            chosen = candidates[direction]
            diag["selected_direction"] = direction

            entry = float(chosen.get("entry_hint") or float(current_price))
            entry_band = max(0.35 * atr_m5, 0.05 * atr_h1)
            entry_buffer = float(getattr(config, "XAUUSD_BEHAVIORAL_ENTRY_BUFFER_ATR_M5", 0.10)) * atr_m5
            rr_min = float(getattr(config, "XAUUSD_BEHAVIORAL_MIN_RR", 2.0))

            if direction == "long":
                if sweep.get("side") == "bullish_rejection" and np.isfinite(self._as_float(sweep.get("trigger_level"), np.nan)):
                    entry = min(entry, self._as_float(sweep.get("trigger_level"), entry) + 0.20 * atr_m5)
                entry = max(entry, float(current_price) - entry_band)
                entry = min(entry, float(current_price) + 0.20 * atr_m5)
                sweep_low = self._as_float(sweep.get("sweep_low"), entry - 0.9 * atr_m5)
                stop_loss = min(entry - 0.75 * atr_m5, sweep_low - entry_buffer)
                if (entry - stop_loss) < (0.45 * atr_m5):
                    stop_loss = entry - 0.45 * atr_m5
            else:
                if sweep.get("side") == "bearish_rejection" and np.isfinite(self._as_float(sweep.get("trigger_level"), np.nan)):
                    entry = max(entry, self._as_float(sweep.get("trigger_level"), entry) - 0.20 * atr_m5)
                entry = min(entry, float(current_price) + entry_band)
                entry = max(entry, float(current_price) - 0.20 * atr_m5)
                sweep_high = self._as_float(sweep.get("sweep_high"), entry + 0.9 * atr_m5)
                stop_loss = max(entry + 0.75 * atr_m5, sweep_high + entry_buffer)
                if (stop_loss - entry) < (0.45 * atr_m5):
                    stop_loss = entry + 0.45 * atr_m5

            risk = abs(entry - stop_loss)
            if not np.isfinite(risk) or risk <= 0:
                diag["reason"] = "invalid_risk_after_levels"
                return None, diag

            targets = self._collect_behavior_targets(
                direction=direction,
                entry=float(entry),
                key_levels=km,
                liq_map=liq_map,
                atr_m5=float(atr_m5),
            )

            if direction == "long":
                def _pick(min_px: float):
                    for px in targets:
                        if px >= min_px:
                            return px
                    return None
                tp1 = _pick(entry + 1.0 * risk) or (entry + 1.0 * risk)
                tp2 = _pick(entry + rr_min * risk) or (entry + rr_min * risk)
                tp3 = _pick(entry + max(2.8, rr_min + 0.8) * risk) or (entry + max(2.8, rr_min + 0.8) * risk)
                tp2 = max(tp2, tp1 + 0.3 * risk)
                tp3 = max(tp3, tp2 + 0.3 * risk)
            else:
                def _pick(max_px: float):
                    for px in targets:
                        if px <= max_px:
                            return px
                    return None
                tp1 = _pick(entry - 1.0 * risk) or (entry - 1.0 * risk)
                tp2 = _pick(entry - rr_min * risk) or (entry - rr_min * risk)
                tp3 = _pick(entry - max(2.8, rr_min + 0.8) * risk) or (entry - max(2.8, rr_min + 0.8) * risk)
                tp2 = min(tp2, tp1 - 0.3 * risk)
                tp3 = min(tp3, tp2 - 0.3 * risk)

            rr = abs(tp2 - entry) / max(1e-9, risk)
            if rr < rr_min:
                diag["reason"] = f"rr_below_min_{rr_min:.2f}"
                return None, diag

            session_list = list((sessions or {}).get("active_sessions", []) or [])
            reasons = list(chosen.get("reasons") or [])
            if sweep.get("detected"):
                reasons.append(f"🎯 Trigger candle: {sweep.get('sweep_time', 'recent')} | wick={sweep.get('wick_ratio')} | vol={sweep.get('vol_ratio')}x")
            if targets:
                reasons.append(f"🎯 Liquidity TP ladder derived from {min(3, len(targets))} nearby pools/levels")
            reasons.append(f"📍 Entry style: retest (no breakout chase), kill-zone={kill_zone}")

            warnings = list(chosen.get("warnings") or [])
            if news_ctx.get("active"):
                warnings.append(f"⚠️ News proximity: {news_ctx.get('nearest_min')}m")

            signal = TradeSignal(
                symbol="XAUUSD",
                direction=direction,
                confidence=round(float(chosen.get("confidence") or 0.0), 1),
                entry=round(float(entry), 4),
                stop_loss=round(float(stop_loss), 4),
                take_profit_1=round(float(tp1), 4),
                take_profit_2=round(float(tp2), 4),
                take_profit_3=round(float(tp3), 4),
                risk_reward=round(float(rr), 2),
                timeframe=f"{str(getattr(config, 'XAUUSD_ENTRY_TF', '1h'))}+5m",
                session=", ".join(session_list) if session_list else "off_hours",
                trend=str(trend_regime if trend_regime != "ranging" else trend_entry),
                rsi=round(self._as_float(h1_last.get("rsi_14"), 50.0), 2),
                atr=round(float(atr_h1), 4),
                pattern="Behavioral Sweep-Retest + Liquidity Continuation",
                reasons=reasons[:12],
                warnings=warnings[:8],
                smc_context=None,
                raw_scores={
                    "engine": "behavioral_fallback_v2",
                    "long_score": round(float(candidates["long"]["score"]), 2),
                    "short_score": round(float(candidates["short"]["score"]), 2),
                    "long_confidence": round(float(candidates["long"]["confidence"]), 2),
                    "short_confidence": round(float(candidates["short"]["confidence"]), 2),
                    "selected_score": round(float(chosen.get("score") or 0.0), 2),
                    "compression": bool(compress),
                    "near_round": bool(near_round),
                    "kill_zone": kill_zone,
                    "trend_tf": trend_lbl,
                    "structure_tf": structure_lbl,
                    "entry_tf": entry_lbl,
                    "regime_votes": dict(regime_votes),
                    "dominant_side": dominant_side,
                    "countertrend_confirmed": bool(chosen.get("countertrend_confirmed", False)),
                    "macro_summary": str(macro_ctx.get("summary", "")),
                },
                entry_type="limit" if abs(float(entry) - float(current_price)) > (0.05 * atr_m5) else "market",
                sl_type="anti_sweep" if bool(sweep.get("detected")) else "atr",
                sl_reason="SL beyond sweep candle invalidation + ATR buffer",
                tp_type="liquidity",
                tp_reason="TP ladder mapped to next liquidity pools/round/session levels",
                sl_liquidity_mapped=bool(sweep.get("detected")),
                liquidity_pools_count=len((liq_map.get("volume_profile") or {}).get("hvn", []) or []),
            )

            diag["status"] = "signal_generated"
            diag["reason"] = "behavioral_trigger_passed"
            diag["selected_direction"] = direction
            return signal, diag
        except Exception as e:
            logger.warning("[XAUUSD] behavioral fallback error: %s", e)
            diag["reason"] = f"fallback_error:{e}"
            return None, diag

    def scan(self) -> Optional[TradeSignal]:
        """
        Full XAUUSD scan. Returns a TradeSignal if opportunity found.
        """
        self.scan_count += 1
        session_info = session_manager.get_session_info()
        self._set_last_scan_diagnostics(
            status="scan_started",
            utc_time=str(session_info.get("utc_time", "-")),
            active_sessions=list(session_info.get("active_sessions", []) or []),
            unmet=[],
            notes=[],
        )
        logger.info(f"[XAUUSD] Scan #{self.scan_count} | {session_info['utc_time']} | "
                    f"Sessions: {session_info['active_sessions']}")
        if not bool(session_info.get("xauusd_market_open", True)):
            self._set_last_scan_diagnostics(
                status="market_closed",
                utc_time=str(session_info.get("utc_time", "-")),
                active_sessions=list(session_info.get("active_sessions", []) or []),
                unmet=["market_closed"],
                notes=["xauusd_market_closed_weekend_window"],
            )
            logger.info("[XAUUSD] Market closed; skip signal evaluation")
            return None

        # Fetch all timeframes
        df_d1 = xauusd_provider.fetch(config.XAUUSD_TREND_TF, bars=100)
        df_h4 = xauusd_provider.fetch(config.XAUUSD_STRUCTURE_TF, bars=150)
        df_h1 = xauusd_provider.fetch(config.XAUUSD_ENTRY_TF, bars=200)
        df_m5 = xauusd_provider.fetch("5m", bars=180)

        if df_h1 is None or df_h1.empty:
            self._set_last_scan_diagnostics(
                status="no_h1_data",
                utc_time=str(session_info.get("utc_time", "-")),
                active_sessions=list(session_info.get("active_sessions", []) or []),
                unmet=["h1_data"],
                notes=["failed_to_fetch_h1_data"],
            )
            logger.warning("[XAUUSD] Failed to fetch H1 data")
            return None

        h1_close = float(df_h1["close"].iloc[-1])
        live_price = xauusd_provider.get_current_price()
        current_price = float(live_price) if live_price is not None else h1_close
        logger.info(f"[XAUUSD] Current price: ${current_price:.2f}")
        self._set_last_scan_diagnostics(
            status="signal_eval",
            utc_time=str(session_info.get("utc_time", "-")),
            active_sessions=list(session_info.get("active_sessions", []) or []),
            current_price=round(float(current_price), 4),
            unmet=[],
            notes=[],
        )

        # Generate signal using entry TF (H1) and trend TF (D1)
        signal = sig.score_signal(
            df_entry=df_h1,
            df_trend=df_d1 if df_d1 is not None else df_h4,
            symbol="XAUUSD",
            timeframe=config.XAUUSD_ENTRY_TF,
            session_info=session_info,
        )

        signal_source = "base_signal_generator"
        fallback_diag = {}
        if signal is None:
            signal, fallback_diag = self._behavioral_fallback_signal(
                current_price=float(current_price),
                df_h1=df_h1,
                df_m5=df_m5,
                df_h4=df_h4,
                df_d1=df_d1,
                key_levels=self.analyze_key_levels(float(current_price)),
                session_info=session_info,
                trend_tf_label=str(config.XAUUSD_TREND_TF),
                structure_tf_label=str(config.XAUUSD_STRUCTURE_TF),
                entry_tf_label=str(config.XAUUSD_ENTRY_TF),
            )
            if signal is not None:
                signal_source = "behavioral_fallback_v2"
                # Microstructure Check
                ok, micro_reason = self._check_microstructure_alignment(signal, float(current_price))
                if not ok:
                    logger.warning("[SIGNAL REJECTED] Microstructure misalignment: %s | dir=%s conf=%.1f", 
                                   micro_reason, signal.direction, signal.confidence)
                    self._set_last_scan_diagnostics(
                        status="signal_rejected",
                        micro_reason=micro_reason,
                        utc_time=str(session_info.get("utc_time", "-")),
                        active_sessions=list(session_info.get("active_sessions", []) or []),
                        notes=[f"micro_rejected:{micro_reason}"]
                    )
                    return None
                    
                logger.info(
                    "[XAUUSD] Behavioral fallback produced %s @ %.2f conf=%.1f [MICRO_ALIGNED]",
                    str(signal.direction).upper(),
                    float(getattr(signal, "entry", current_price)),
                    float(getattr(signal, "confidence", 0.0)),
                )
            else:
                notes = ["signal_generator_returned_none"]
                fb_reason = str((fallback_diag or {}).get("reason", "")).strip()
                if fb_reason:
                    notes.append(f"fallback:{fb_reason}")
                self._set_last_scan_diagnostics(
                    status="no_setup",
                    utc_time=str(session_info.get("utc_time", "-")),
                    active_sessions=list(session_info.get("active_sessions", []) or []),
                    current_price=round(float(current_price), 4),
                    unmet=["base_setup", "behavioral_fallback"],
                    notes=notes[:4],
                    fallback=fallback_diag,
                )
                return None

        # Enrich signal with XAUUSD-specific context
        if signal is not None:
            key_levels = self.analyze_key_levels(current_price)
            signal.raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            signal.raw_scores["xau_signal_source"] = signal_source
            if signal_source == "behavioral_fallback_v2":
                signal.raw_scores["behavioral_trigger"] = True
                signal.raw_scores["behavioral_trigger_source"] = "behavioral_fallback_v2"
            if live_price is not None:
                signal.reasons.append(f"💰 Live XAUUSD: ${current_price:.2f}")
            else:
                signal.warnings.append("⚠️ Live quote unavailable; using H1 close")
            signal.reasons.append(
                f"📊 Key levels — Support: ${key_levels['nearest_support']:.0f} | "
                f"Resistance: ${key_levels['nearest_resistance']:.0f}"
            )

            # Asian range context
            if "asian_high" in key_levels:
                ar_high = key_levels["asian_high"]
                ar_low = key_levels["asian_low"]
                if current_price > ar_high:
                    signal.reasons.append(f"✅ Price above Asian Range High (${ar_high:.2f}) - bullish breakout")
                elif current_price < ar_low:
                    signal.reasons.append(f"✅ Price below Asian Range Low (${ar_low:.2f}) - bearish breakdown")
                else:
                    signal.warnings.append(f"⚠️ Price inside Asian Range (${ar_low:.2f}-${ar_high:.2f})")

            # Session boost for XAUUSD
            if "london" in session_info["active_sessions"]:
                signal.reasons.append("🕐 London Session: High liquidity for Gold")
            if "new_york" in session_info["active_sessions"]:
                signal.reasons.append("🕐 New York Session: Gold most volatile")

            signal, _guard = self._apply_trade_location_guard(signal, current_price, key_levels, df_h1, df_m5)
            if signal is None:
                unmet = []
                if (_guard or {}).get("near_round"):
                    unmet.append("trap_near_round")
                if (_guard or {}).get("no_chase"):
                    unmet.append("trap_no_chase")
                if bool(((_guard or {}).get("sweep") or {}).get("detected")):
                    unmet.append("trap_sweep_rejection")
                if (_guard or {}).get("event_risk"):
                    unmet.append("event_risk")
                if bool(((_guard or {}).get("news_freeze") or {}).get("active")):
                    unmet.append("news_freeze")
                if bool(((_guard or {}).get("macro_shock") or {}).get("adverse_for_long")) or bool(((_guard or {}).get("macro_shock") or {}).get("adverse_for_short")):
                    unmet.append("macro_shock")
                self._set_last_scan_diagnostics(
                    status="trap_guard_blocked",
                    utc_time=str(session_info.get("utc_time", "-")),
                    active_sessions=list(session_info.get("active_sessions", []) or []),
                    current_price=round(float(current_price), 4),
                    unmet=unmet or ["trap_guard"],
                    notes=list(((_guard or {}).get("warnings") or [])[:3]),
                    guard_penalty=float((_guard or {}).get("penalty", 0.0) or 0.0),
                )
                return None

            self._set_last_scan_diagnostics(
                status="signal_generated",
                utc_time=str(session_info.get("utc_time", "-")),
                active_sessions=list(session_info.get("active_sessions", []) or []),
                current_price=round(float(current_price), 4),
                unmet=[],
                notes=[],
                confidence=round(float(getattr(signal, "confidence", 0.0) or 0.0), 1),
                direction=str(getattr(signal, "direction", "")),
                source=signal_source,
                fallback=fallback_diag if signal_source != "base_signal_generator" else {},
            )

            self.last_signal = signal
            self.signal_count += 1
            logger.info(f"[XAUUSD] ✅ Signal generated: {signal.direction.upper()} "
                        f"@ ${signal.entry:.2f} | Confidence: {signal.confidence:.1f}%")

        return signal

    def get_market_overview(self) -> dict:
        """Return a full market overview for gold (no signal needed)."""
        overview = {
            "symbol": "XAUUSD",
            "scanned_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "session": session_manager.get_session_info(),
        }

        df_d1 = xauusd_provider.fetch("1d", bars=50)
        df_h4 = xauusd_provider.fetch("4h", bars=100)
        df_h1 = xauusd_provider.fetch("1h", bars=140)
        df_m5 = xauusd_provider.fetch("5m", bars=320)

        h1_ta = None
        if df_h1 is not None and not df_h1.empty:
            h1_summary = ta.summary(df_h1)
            overview["h1_close"] = h1_summary.get("close")
            overview["h1"] = h1_summary
            try:
                h1_ta = ta.add_all(df_h1.copy())
            except Exception:
                h1_ta = None

        live_price = xauusd_provider.get_current_price()
        if live_price is not None:
            overview["price"] = round(float(live_price), 4)
            overview["price_source"] = "live_quote"
        elif "h1_close" in overview:
            overview["price"] = overview["h1_close"]
            overview["price_source"] = "h1_close"

        if df_h4 is not None and not df_h4.empty:
            overview["h4"] = ta.summary(df_h4)
            overview["h4_smc"] = self._smc_summary(df_h4)

        if df_d1 is not None and not df_d1.empty:
            overview["d1"] = ta.summary(df_d1)

        if overview.get("price"):
            overview["key_levels"] = self.analyze_key_levels(overview["price"])
            try:
                overview["liquidity_map"] = self._build_liquidity_map(float(overview["price"]), df_h1, df_m5, h1_ta=h1_ta)
            except Exception as e:
                logger.debug("[XAUUSD] overview liquidity map error: %s", e)
            try:
                overview["macro_shock"] = self._macro_shock_context(df_m5)
            except Exception as e:
                logger.debug("[XAUUSD] overview macro context error: %s", e)
            try:
                overview["news_freeze"] = self._news_freeze_context()
            except Exception as e:
                logger.debug("[XAUUSD] overview news freeze error: %s", e)

        return overview

    def _smc_summary(self, df) -> dict:
        ctx = smc.analyze(df)
        return {
            "bias": ctx.bias,
            "confidence": ctx.confidence,
            "order_blocks": len(ctx.order_blocks),
            "fvgs": len(ctx.fair_value_gaps),
            "trend": ctx.current_trend,
        }

    def get_stats(self) -> dict:
        return {
            "total_scans": self.scan_count,
            "signals_generated": self.signal_count,
            "hit_rate": f"{self.signal_count/max(self.scan_count,1)*100:.1f}%",
        }


xauusd_scanner = XAUUSDScanner()
