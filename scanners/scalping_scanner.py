"""
scanners/scalping_scanner.py

Dedicated scalping signal path (separate from the default signal pipeline):
- XAUUSD and ETH-only (configurable)
- M5 entry profile + M1 trigger confirmation
- Leaves existing scanner behavior untouched
"""
from __future__ import annotations

import logging
import sqlite3
import time
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from analysis.technical import TechnicalAnalysis
from analysis.signals import TradeSignal
from config import config
from learning.entry_template_catalog import load_catalog, pick_template_block, session_bucket_for_entry_template
from market.data_fetcher import xauusd_provider, crypto_provider, session_manager
from scanners.xauusd import xauusd_scanner
from scanners.crypto_sniper import crypto_sniper
from market.tick_bar_engine import TickBarEngine
from scanners.mrd_scanner import MRDScanner
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ScalpingScanResult:
    source: str
    symbol: str
    status: str
    reason: str
    signal: Optional[TradeSignal] = None
    trigger: dict = field(default_factory=dict)


class ScalpingScanner:
    def __init__(self):
        self.ta = TechnicalAnalysis()
        self._xau_winner_cache_ts: float = 0.0
        self._xau_winner_cache: dict = {}
        self._crypto_winner_cache_ts: dict[str, float] = {}
        self._crypto_winner_cache: dict[str, dict] = {}

    @staticmethod
    @contextmanager
    def _temporary_config(**updates):
        originals: dict[str, object] = {}
        try:
            for key, value in updates.items():
                if hasattr(config, key):
                    originals[key] = getattr(config, key)
                    setattr(config, key, value)
            yield
        finally:
            for key, value in originals.items():
                setattr(config, key, value)

    @staticmethod
    def _as_float(v, fallback: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(fallback)

    @staticmethod
    def _round_price_for_symbol(symbol: str, value: float) -> float:
        v = abs(float(value))
        s = str(symbol or "").upper()
        if "XAU" in s or "GOLD" in s:
            return round(float(value), 2)
        if v >= 100:
            return round(float(value), 3)
        if v >= 1:
            return round(float(value), 4)
        return round(float(value), 6)

    @staticmethod
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(float(lo), min(float(hi), float(v)))

    @staticmethod
    def _canonical_session_key(raw: str) -> str:
        s = str(raw or "").strip().lower()
        if not s:
            return "off_hours"
        tokens = {
            t.strip().replace(" ", "_")
            for t in s.replace("/", ",").replace("|", ",").split(",")
            if t and t.strip()
        }
        has_asian = "asian" in tokens
        has_london = "london" in tokens
        has_ny = ("new_york" in tokens) or ("new york" in s) or ("ny" in tokens)
        has_overlap = "overlap" in tokens
        if has_london and has_ny:
            return "london_new_york_overlap"
        if has_overlap and has_london and has_ny:
            return "london_new_york_overlap"
        if has_asian and has_london:
            return "asian_london"
        if has_ny:
            return "new_york"
        if has_london:
            return "london"
        if has_asian:
            return "asian"
        return "off_hours"

    @staticmethod
    def _normalized_signature(raw: str) -> str:
        tokens = [
            str(part or "").strip().lower().replace(" ", "_")
            for part in str(raw or "").split(",")
            if str(part or "").strip()
        ]
        return ",".join(tokens)

    @staticmethod
    def _parse_lower_csv(raw: str) -> set[str]:
        return {
            str(part or "").strip().lower()
            for part in str(raw or "").split(",")
            if str(part or "").strip()
        }

    @staticmethod
    def _normalize_reason_token(raw: str) -> str:
        token = str(raw or "").strip().lower().replace(" ", "_")
        if not token:
            return ""
        return token

    @staticmethod
    def _signature_tokens(signature: str) -> set[str]:
        return {
            str(part or "").strip().lower().replace(" ", "_")
            for part in str(signature or "").split(",")
            if str(part or "").strip()
        }

    @classmethod
    def _session_signature_matches(cls, session_sig: str, allowed_sessions: set[str]) -> bool:
        if not allowed_sessions:
            return True
        sig = str(session_sig or "").strip().lower()
        if not sig:
            return False
        sig_tokens = cls._signature_tokens(sig)
        for allowed in set(allowed_sessions or set()):
            allowed_sig = str(allowed or "").strip().lower()
            if not allowed_sig:
                continue
            if allowed_sig == sig:
                return True
            allowed_tokens = cls._signature_tokens(allowed_sig)
            if allowed_tokens and allowed_tokens.issubset(sig_tokens):
                return True
        return False

    @staticmethod
    def _is_weekend_utc() -> bool:
        return bool(datetime.now(timezone.utc).weekday() >= 5)

    # ── Crypto multi-TF context (ported from XAU _apply_xau_multi_tf_context) ──

    def _apply_crypto_multi_tf_context(self, signal, symbol: str) -> None:
        """Fetch 4h + 1h trends from cTrader and adjust signal confidence."""
        if signal is None:
            return
        try:
            df_h4 = self._fetch_ctrader_ohlcv(symbol, "4h", bars=50)
            df_h1 = self._fetch_ctrader_ohlcv(symbol, "1h", bars=100)
            h4_trend = "unknown"
            h1_trend = "unknown"
            if df_h4 is not None and len(df_h4) >= 20:
                df_h4 = self.ta.add_all(df_h4)
                h4_trend = self.ta.determine_trend(df_h4)
            if df_h1 is not None and len(df_h1) >= 20:
                df_h1 = self.ta.add_all(df_h1)
                h1_trend = self.ta.determine_trend(df_h1)
            direction = str(getattr(signal, "direction", "") or "").lower()
            aligned_trend = "bullish" if direction == "long" else "bearish"
            bonus = 0.0
            h4_match = (h4_trend == aligned_trend)
            h1_match = (h1_trend == aligned_trend)
            if h4_match and h1_match:
                bonus = 5.0
            elif h4_match or h1_match:
                bonus = 2.0
            h4_oppose = (h4_trend == ("bearish" if direction == "long" else "bullish"))
            h1_oppose = (h1_trend == ("bearish" if direction == "long" else "bullish"))
            if h4_oppose and h1_oppose:
                bonus = -3.0
            elif h4_oppose or h1_oppose:
                bonus = min(bonus, 0.0)
            if bonus != 0.0:
                old_conf = float(getattr(signal, "confidence", 0) or 0)
                signal.confidence = round(max(55.0, min(95.0, old_conf + bonus)), 1)
            raw = dict(getattr(signal, "raw_scores", {}) or {})
            raw["crypto_mtf_h4_trend"] = h4_trend
            raw["crypto_mtf_h1_trend"] = h1_trend
            raw["crypto_mtf_bonus"] = round(bonus, 1)
            signal.raw_scores = raw
            if bonus != 0.0:
                logger.info("[ScalpCrypto] %s multi_tf: h4=%s h1=%s bonus=%+.1f conf=%.1f",
                             symbol, h4_trend, h1_trend, bonus, signal.confidence)
        except Exception as e:
            logger.debug("[ScalpCrypto] %s multi_tf error: %s", symbol, e)

    def _m1_trigger_crypto(self, df_m1, direction: str) -> tuple:
        """M1 trigger with crypto-specific RSI/breakout overrides."""
        import contextlib
        from unittest.mock import patch as _mock_patch
        overrides = {
            "SCALPING_M1_TRIGGER_RSI_LONG_MIN": float(getattr(config, "SCALPING_CRYPTO_M1_RSI_LONG_MIN", 48)),
            "SCALPING_M1_TRIGGER_RSI_LONG_MAX": float(getattr(config, "SCALPING_CRYPTO_M1_RSI_LONG_MAX", 75)),
            "SCALPING_M1_TRIGGER_RSI_SHORT_MAX": float(getattr(config, "SCALPING_CRYPTO_M1_RSI_SHORT_MAX", 52)),
            "SCALPING_M1_TRIGGER_BREAKOUT_BARS": int(getattr(config, "SCALPING_CRYPTO_M1_BREAKOUT_BARS", 5)),
        }
        with contextlib.ExitStack() as stack:
            for key, val in overrides.items():
                stack.enter_context(_mock_patch.object(config, key, val))
            return self._m1_trigger(df_m1, direction)

    def _crypto_scalping_profile(self, symbol: str) -> dict:
        sym = str(symbol or "").strip().upper()
        base_min = float(getattr(config, "SCALPING_MIN_CONFIDENCE", getattr(config, "MIN_SIGNAL_CONFIDENCE", 70)) or 70)
        weekend = self._is_weekend_utc()
        if sym == "ETHUSD":
            min_conf = float(
                getattr(
                    config,
                    "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND" if weekend else "SCALPING_ETH_MIN_CONFIDENCE",
                    getattr(config, "SCALPING_ETH_MIN_CONFIDENCE", base_min),
                )
                or getattr(config, "SCALPING_ETH_MIN_CONFIDENCE", base_min)
            )
            allowed = (
                config.get_scalping_eth_allowed_sessions_weekend()
                if weekend
                else config.get_scalping_eth_allowed_sessions()
            )
        elif sym == "BTCUSD":
            min_conf = float(
                getattr(
                    config,
                    "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND" if weekend else "SCALPING_BTC_MIN_CONFIDENCE",
                    getattr(config, "SCALPING_BTC_MIN_CONFIDENCE", base_min),
                )
                or getattr(config, "SCALPING_BTC_MIN_CONFIDENCE", base_min)
            )
            allowed = (
                config.get_scalping_btc_allowed_sessions_weekend()
                if weekend
                else config.get_scalping_btc_allowed_sessions()
            )
        else:
            min_conf = base_min
            allowed = set()
        return {
            "symbol": sym,
            "weekend": bool(weekend),
            "min_confidence": float(min_conf),
            "allowed_sessions": set(allowed or set()),
        }

    @staticmethod
    def _winner_outcome_to_label(outcome: str) -> str:
        o = str(outcome or "").strip().lower()
        if o in {"tp1_hit", "tp2_hit", "tp3_hit", "tp", "win"}:
            return "win"
        if o in {"sl_hit", "sl", "loss"}:
            return "loss"
        return ""

    @staticmethod
    def _new_winner_stat() -> dict:
        return {"total": 0, "resolved": 0, "wins": 0, "losses": 0, "pnl": 0.0, "win_rate": 0.0, "avg_pnl": 0.0}

    @classmethod
    def _update_winner_stat(cls, stat: dict, *, result: str, pnl: float) -> None:
        s = stat if isinstance(stat, dict) else {}
        s["total"] = int(s.get("total", 0) or 0) + 1
        if result not in {"win", "loss"}:
            return
        s["resolved"] = int(s.get("resolved", 0) or 0) + 1
        if result == "win":
            s["wins"] = int(s.get("wins", 0) or 0) + 1
        else:
            s["losses"] = int(s.get("losses", 0) or 0) + 1
        s["pnl"] = float(s.get("pnl", 0.0) or 0.0) + float(pnl)

    @classmethod
    def _finalize_winner_stat(cls, stat: dict) -> dict:
        s = dict(stat or {})
        resolved = max(0, int(s.get("resolved", 0) or 0))
        wins = max(0, int(s.get("wins", 0) or 0))
        pnl = float(s.get("pnl", 0.0) or 0.0)
        s["win_rate"] = (wins / resolved) if resolved > 0 else 0.0
        s["avg_pnl"] = (pnl / resolved) if resolved > 0 else 0.0
        s["pnl"] = round(pnl, 6)
        return s

    def _winner_db_path(self) -> Path:
        data_dir = Path(__file__).resolve().parent.parent / "data"
        cfg = str(getattr(config, "SCALPING_HISTORY_DB_PATH", "") or "").strip()
        if cfg:
            return Path(cfg)
        return data_dir / "scalp_signal_history.db"

    @staticmethod
    def _confidence_band_key(confidence: float) -> str:
        conf = float(confidence or 0.0)
        if conf < 70.0:
            return "lt70"
        if conf < 75.0:
            return "70_74"
        if conf < 80.0:
            return "75_79"
        return "80_plus"

    def _load_crypto_winner_profile(self, symbol: str) -> dict:
        sym = str(symbol or "").strip().upper()
        enabled = bool(getattr(config, "SCALPING_CRYPTO_WINNER_LOGIC_ENABLED", True))
        out = {
            "enabled": enabled,
            "symbol": sym,
            "ok": False,
            "error": "",
            "lookback_days": max(1, int(getattr(config, "SCALPING_CRYPTO_WINNER_LOOKBACK_DAYS", 21) or 21)),
            "generated_at": int(time.time()),
            "overall": self._new_winner_stat(),
            "by_side": {},
            "by_session": {},
            "by_conf_band": {},
            "by_side_session": {},
            "by_side_band": {},
            "by_session_band": {},
            "by_side_session_band": {},
        }
        ttl = max(30, int(getattr(config, "SCALPING_CRYPTO_WINNER_CACHE_SEC", 180) or 180))
        now_ts = time.time()
        cache_ts = float((self._crypto_winner_cache_ts or {}).get(sym, 0.0) or 0.0)
        if (now_ts - cache_ts) <= float(ttl):
            cached = dict((self._crypto_winner_cache or {}).get(sym) or {})
            if cached:
                return cached
        if (not enabled) or sym not in {"ETHUSD", "BTCUSD"}:
            self._crypto_winner_cache[sym] = dict(out)
            self._crypto_winner_cache_ts[sym] = now_ts
            return out

        db_path = self._winner_db_path()
        if not db_path.exists():
            out["error"] = "history_db_missing"
            self._crypto_winner_cache[sym] = dict(out)
            self._crypto_winner_cache_ts[sym] = now_ts
            return out

        since_ts = now_ts - float(out["lookback_days"] * 86400)
        try:
            with sqlite3.connect(str(db_path), timeout=10) as conn:
                rows = conn.execute(
                    """
                    SELECT direction, session, confidence, outcome, pnl_usd, timestamp
                      FROM scalp_signals
                     WHERE UPPER(symbol)=? AND timestamp>=?
                     ORDER BY timestamp ASC
                    """,
                    (sym, since_ts),
                ).fetchall()
        except Exception as e:
            out["error"] = f"query_error:{e}"
            self._crypto_winner_cache[sym] = dict(out)
            self._crypto_winner_cache_ts[sym] = now_ts
            return out

        side_stats: dict[str, dict] = {"long": self._new_winner_stat(), "short": self._new_winner_stat()}
        session_stats: dict[str, dict] = {}
        band_stats: dict[str, dict] = {}
        side_session_stats: dict[str, dict] = {}
        side_band_stats: dict[str, dict] = {}
        session_band_stats: dict[str, dict] = {}
        side_session_band_stats: dict[str, dict] = {}
        overall = self._new_winner_stat()
        for direction, session, confidence, outcome, pnl_usd, _ts in rows:
            side = str(direction or "").strip().lower()
            if side not in {"long", "short"}:
                continue
            session_key = self._canonical_session_key(str(session or ""))
            conf_band = self._confidence_band_key(self._as_float(confidence, 0.0))
            result = self._winner_outcome_to_label(str(outcome or ""))
            pnl = self._as_float(pnl_usd, 0.0)
            self._update_winner_stat(overall, result=result, pnl=pnl)
            self._update_winner_stat(side_stats.setdefault(side, self._new_winner_stat()), result=result, pnl=pnl)
            self._update_winner_stat(session_stats.setdefault(session_key, self._new_winner_stat()), result=result, pnl=pnl)
            self._update_winner_stat(band_stats.setdefault(conf_band, self._new_winner_stat()), result=result, pnl=pnl)
            self._update_winner_stat(
                side_session_stats.setdefault(f"{side}|{session_key}", self._new_winner_stat()),
                result=result,
                pnl=pnl,
            )
            self._update_winner_stat(
                side_band_stats.setdefault(f"{side}|{conf_band}", self._new_winner_stat()),
                result=result,
                pnl=pnl,
            )
            self._update_winner_stat(
                session_band_stats.setdefault(f"{session_key}|{conf_band}", self._new_winner_stat()),
                result=result,
                pnl=pnl,
            )
            self._update_winner_stat(
                side_session_band_stats.setdefault(f"{side}|{session_key}|{conf_band}", self._new_winner_stat()),
                result=result,
                pnl=pnl,
            )

        out["overall"] = self._finalize_winner_stat(overall)
        out["by_side"] = {k: self._finalize_winner_stat(v) for k, v in side_stats.items()}
        out["by_session"] = {k: self._finalize_winner_stat(v) for k, v in session_stats.items()}
        out["by_conf_band"] = {k: self._finalize_winner_stat(v) for k, v in band_stats.items()}
        out["by_side_session"] = {k: self._finalize_winner_stat(v) for k, v in side_session_stats.items()}
        out["by_side_band"] = {k: self._finalize_winner_stat(v) for k, v in side_band_stats.items()}
        out["by_session_band"] = {k: self._finalize_winner_stat(v) for k, v in session_band_stats.items()}
        out["by_side_session_band"] = {k: self._finalize_winner_stat(v) for k, v in side_session_band_stats.items()}
        out["ok"] = True
        self._crypto_winner_cache[sym] = dict(out)
        self._crypto_winner_cache_ts[sym] = now_ts
        return out

    def _pick_crypto_winner_stat(self, profile: dict, *, side: str, session_key: str, conf_band: str) -> tuple[str, dict]:
        min_side_session_band = max(2, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_SIDE_SESSION_BAND_SAMPLES", 3) or 3))
        min_side_session = max(min_side_session_band, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_SIDE_SESSION_SAMPLES", 4) or 4))
        min_session_band = max(min_side_session_band, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_SESSION_BAND_SAMPLES", 4) or 4))
        min_side_band = max(min_side_session_band, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_SIDE_BAND_SAMPLES", 4) or 4))
        min_side = max(min_side_session, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_SIDE_SAMPLES", 5) or 5))
        min_session = max(2, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_SESSION_SAMPLES", 3) or 3))
        min_band = max(2, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_BAND_SAMPLES", 4) or 4))
        min_overall = max(min_side, int(getattr(config, "SCALPING_CRYPTO_WINNER_MIN_OVERALL_SAMPLES", 8) or 8))
        by_side_session_band = dict(profile.get("by_side_session_band") or {})
        by_side_session = dict(profile.get("by_side_session") or {})
        by_session_band = dict(profile.get("by_session_band") or {})
        by_side_band = dict(profile.get("by_side_band") or {})
        by_side = dict(profile.get("by_side") or {})
        by_session = dict(profile.get("by_session") or {})
        by_conf_band = dict(profile.get("by_conf_band") or {})
        overall = dict(profile.get("overall") or {})
        candidates = [
            (f"side_session_band:{side}:{session_key}:{conf_band}", by_side_session_band.get(f"{side}|{session_key}|{conf_band}") or {}, min_side_session_band),
            (f"side_session:{side}:{session_key}", by_side_session.get(f"{side}|{session_key}") or {}, min_side_session),
            (f"session_band:{session_key}:{conf_band}", by_session_band.get(f"{session_key}|{conf_band}") or {}, min_session_band),
            (f"side_band:{side}:{conf_band}", by_side_band.get(f"{side}|{conf_band}") or {}, min_side_band),
            (f"side:{side}", by_side.get(side) or {}, min_side),
            (f"session:{session_key}", by_session.get(session_key) or {}, min_session),
            (f"band:{conf_band}", by_conf_band.get(conf_band) or {}, min_band),
            ("overall", overall, min_overall),
        ]
        for scope, stat, min_required in candidates:
            if int((stat or {}).get("resolved", 0) or 0) >= int(min_required):
                return scope, dict(stat or {})
        return "", {}

    def _crypto_winner_regime(self, stat: dict) -> str:
        if not stat:
            return "neutral"
        wr = float(stat.get("win_rate", 0.0) or 0.0)
        avg_pnl = float(stat.get("avg_pnl", 0.0) or 0.0)
        severe_wr = float(getattr(config, "SCALPING_CRYPTO_WINNER_SEVERE_WR", 0.40) or 0.40)
        weak_wr = float(getattr(config, "SCALPING_CRYPTO_WINNER_WEAK_WR", 0.50) or 0.50)
        strong_wr = float(getattr(config, "SCALPING_CRYPTO_WINNER_STRONG_WR", 0.62) or 0.62)
        severe_avg = float(getattr(config, "SCALPING_CRYPTO_WINNER_SEVERE_AVG_PNL", -8.0) or -8.0)
        weak_avg = float(getattr(config, "SCALPING_CRYPTO_WINNER_WEAK_AVG_PNL", 0.0) or 0.0)
        strong_avg = float(getattr(config, "SCALPING_CRYPTO_WINNER_STRONG_AVG_PNL", 0.0) or 0.0)
        if wr <= severe_wr and avg_pnl <= severe_avg:
            return "severe"
        if wr <= weak_wr and avg_pnl <= weak_avg:
            return "weak"
        if wr >= strong_wr and avg_pnl >= strong_avg:
            return "strong"
        return "neutral"

    def _apply_crypto_winner_logic(self, signal: TradeSignal, *, apply_confidence: bool = True) -> dict:
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        info = {
            "enabled": bool(getattr(config, "SCALPING_CRYPTO_WINNER_LOGIC_ENABLED", True)),
            "symbol": symbol,
            "applied": False,
            "reason": "disabled",
            "scope": "",
            "regime": "neutral",
            "session_key": "",
            "conf_band": "",
            "resolved": 0,
            "win_rate": 0.0,
            "avg_pnl": 0.0,
            "hard_block": False,
        }
        if signal is None or (not bool(info["enabled"])):
            return info
        if symbol not in {"ETHUSD", "BTCUSD"} or direction not in {"long", "short"}:
            info["reason"] = "not_crypto_or_direction_invalid"
            return info

        session_raw = str(getattr(signal, "session", "") or "")
        if not session_raw:
            session_raw = ", ".join(session_manager.current_sessions()) or "off_hours"
        session_key = self._canonical_session_key(session_raw)
        conf_before = self._as_float(getattr(signal, "confidence", 0.0), 0.0)
        conf_band = self._confidence_band_key(conf_before)
        info["session_key"] = session_key
        info["conf_band"] = conf_band

        profile = self._load_crypto_winner_profile(symbol)
        if not bool(profile.get("ok")):
            info["reason"] = str(profile.get("error") or "profile_unavailable")
            return info
        scope, stat = self._pick_crypto_winner_stat(profile, side=direction, session_key=session_key, conf_band=conf_band)
        if not scope or not stat:
            info["reason"] = "insufficient_samples"
            return info

        regime = self._crypto_winner_regime(stat)
        resolved = int(stat.get("resolved", 0) or 0)
        win_rate = float(stat.get("win_rate", 0.0) or 0.0)
        avg_pnl = float(stat.get("avg_pnl", 0.0) or 0.0)
        info.update(
            {
                "scope": scope,
                "regime": regime,
                "resolved": resolved,
                "win_rate": round(win_rate, 4),
                "avg_pnl": round(avg_pnl, 4),
                "reason": "regime_selected",
            }
        )

        changed = False
        if apply_confidence:
            conf_delta = 0.0
            if regime == "strong":
                conf_delta = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_CONF_BONUS", 2.0), 2.0)
            elif regime == "weak":
                conf_delta = -abs(self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_CONF_PENALTY_WEAK", 2.0), 2.0))
            elif regime == "severe":
                conf_delta = -abs(self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_CONF_PENALTY_SEVERE", 4.5), 4.5))
            if abs(conf_delta) > 1e-9:
                conf_min = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_CONF_MIN", 55.0), 55.0)
                conf_max = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_CONF_MAX", 90.0), 90.0)
                signal.confidence = round(self._clamp(conf_before + conf_delta, conf_min, conf_max), 1)
                info["confidence_before"] = round(conf_before, 4)
                info["confidence_after"] = round(float(getattr(signal, "confidence", conf_before) or conf_before), 4)
                changed = True

        retune_info = self._retune_crypto_exits(signal, regime)
        if retune_info.get("applied"):
            changed = True
            info["exit_retuned"] = True
            info["exit_retune_risk_mult"] = retune_info.get("risk_mult", 1.0)
            info["exit_retune_rr_mult"] = retune_info.get("rr_mult", 1.0)

        if regime == "severe" and bool(getattr(config, "SCALPING_CRYPTO_WINNER_HARD_BLOCK_SEVERE", False)):
            hard_min_conf = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_HARD_BLOCK_MIN_CONF", 76.0), 76.0)
            now_conf = self._as_float(getattr(signal, "confidence", conf_before), conf_before)
            if now_conf < hard_min_conf:
                info["hard_block"] = True
                info["reason"] = f"severe_regime_conf<{hard_min_conf:.1f}"

        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["crypto_winner_logic_enabled"] = True
        raw["crypto_winner_logic_symbol"] = symbol
        raw["crypto_winner_logic_scope"] = scope
        raw["crypto_winner_logic_regime"] = regime
        raw["crypto_winner_logic_session"] = session_key
        raw["crypto_winner_logic_conf_band"] = conf_band
        raw["crypto_winner_logic_resolved"] = resolved
        raw["crypto_winner_logic_win_rate"] = round(win_rate, 4)
        raw["crypto_winner_logic_avg_pnl"] = round(avg_pnl, 4)
        raw["crypto_winner_logic_applied"] = bool(changed)
        if "confidence_after" in info:
            raw["crypto_winner_logic_confidence_after"] = info["confidence_after"]
        signal.raw_scores = raw

        reasons = list(getattr(signal, "reasons", []) or [])
        reasons.append(
            f"Crypto winner {symbol} {regime} ({scope}) WR {win_rate*100:.1f}%/{resolved} avgPnL {avg_pnl:+.2f}"
        )
        signal.reasons = reasons[-12:]
        info["applied"] = bool(changed)
        return info

    def _retune_crypto_exits(self, signal: TradeSignal, winner_regime: str) -> dict:
        """Adjust SL/TP based on winner regime — ported from XAU logic."""
        info: dict = {"applied": False, "risk_mult": 1.0, "rr_mult": 1.0}
        if signal is None or winner_regime not in {"strong", "weak", "severe"}:
            return info
        if winner_regime == "strong":
            risk_mult = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_RISK_MULT_STRONG", 1.0), 1.0)
            rr_mult = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_RR_MULT_STRONG", 1.05), 1.05)
        elif winner_regime == "weak":
            risk_mult = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_RISK_MULT_WEAK", 0.85), 0.85)
            rr_mult = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_RR_MULT_WEAK", 0.88), 0.88)
        else:
            risk_mult = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_RISK_MULT_SEVERE", 0.75), 0.75)
            rr_mult = self._as_float(getattr(config, "SCALPING_CRYPTO_WINNER_RR_MULT_SEVERE", 0.78), 0.78)
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        entry = self._as_float(getattr(signal, "entry", 0.0), 0.0)
        stop = self._as_float(getattr(signal, "stop_loss", 0.0), 0.0)
        if entry <= 0 or stop <= 0 or direction not in {"long", "short"}:
            return info
        risk = abs(entry - stop)
        if risk <= 1e-9:
            return info
        new_risk = risk * risk_mult
        if direction == "long":
            signal.stop_loss = round(entry - new_risk, 8)
            signal.take_profit_1 = round(entry + new_risk * rr_mult * 1.0, 8)
            signal.take_profit_2 = round(entry + new_risk * rr_mult * 1.5, 8)
            signal.take_profit_3 = round(entry + new_risk * rr_mult * 2.0, 8)
        else:
            signal.stop_loss = round(entry + new_risk, 8)
            signal.take_profit_1 = round(entry - new_risk * rr_mult * 1.0, 8)
            signal.take_profit_2 = round(entry - new_risk * rr_mult * 1.5, 8)
            signal.take_profit_3 = round(entry - new_risk * rr_mult * 2.0, 8)
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["crypto_winner_exit_retuned"] = True
        raw["crypto_winner_exit_risk_mult"] = risk_mult
        raw["crypto_winner_exit_rr_mult"] = rr_mult
        signal.raw_scores = raw
        info.update({"applied": True, "risk_mult": risk_mult, "rr_mult": rr_mult})
        return info

    def _load_xau_winner_profile(self) -> dict:
        enabled = bool(getattr(config, "SCALPING_XAU_WINNER_LOGIC_ENABLED", True))
        out = {
            "enabled": enabled,
            "ok": False,
            "error": "",
            "lookback_days": max(1, int(getattr(config, "SCALPING_XAU_WINNER_LOOKBACK_DAYS", 14) or 14)),
            "generated_at": int(time.time()),
            "overall": self._new_winner_stat(),
            "by_side": {},
            "by_session": {},
            "by_side_session": {},
        }
        ttl = max(30, int(getattr(config, "SCALPING_XAU_WINNER_CACHE_SEC", 180) or 180))
        now_ts = time.time()
        if (now_ts - float(self._xau_winner_cache_ts or 0.0)) <= float(ttl):
            cached = dict(self._xau_winner_cache or {})
            if cached:
                return cached
        if not enabled:
            self._xau_winner_cache = dict(out)
            self._xau_winner_cache_ts = now_ts
            return out

        db_path = self._winner_db_path()
        if not db_path.exists():
            out["error"] = "history_db_missing"
            self._xau_winner_cache = dict(out)
            self._xau_winner_cache_ts = now_ts
            return out

        since_ts = now_ts - float(out["lookback_days"] * 86400)
        try:
            with sqlite3.connect(str(db_path), timeout=10) as conn:
                rows = conn.execute(
                    """
                    SELECT direction, session, outcome, pnl_usd, timestamp
                      FROM scalp_signals
                     WHERE UPPER(symbol)='XAUUSD' AND timestamp>=?
                     ORDER BY timestamp ASC
                    """,
                    (since_ts,),
                ).fetchall()
        except Exception as e:
            out["error"] = f"query_error:{e}"
            self._xau_winner_cache = dict(out)
            self._xau_winner_cache_ts = now_ts
            return out

        side_stats: dict[str, dict] = {"long": self._new_winner_stat(), "short": self._new_winner_stat()}
        session_stats: dict[str, dict] = {}
        side_session_stats: dict[str, dict] = {}
        overall = self._new_winner_stat()
        for direction, session, outcome, pnl_usd, _ts in rows:
            side = str(direction or "").strip().lower()
            if side not in {"long", "short"}:
                continue
            sess_key = self._canonical_session_key(str(session or ""))
            result = self._winner_outcome_to_label(str(outcome or ""))
            pnl = self._as_float(pnl_usd, 0.0)
            self._update_winner_stat(overall, result=result, pnl=pnl)
            self._update_winner_stat(side_stats.setdefault(side, self._new_winner_stat()), result=result, pnl=pnl)
            self._update_winner_stat(session_stats.setdefault(sess_key, self._new_winner_stat()), result=result, pnl=pnl)
            k = f"{side}|{sess_key}"
            self._update_winner_stat(side_session_stats.setdefault(k, self._new_winner_stat()), result=result, pnl=pnl)

        out["overall"] = self._finalize_winner_stat(overall)
        out["by_side"] = {k: self._finalize_winner_stat(v) for k, v in side_stats.items()}
        out["by_session"] = {k: self._finalize_winner_stat(v) for k, v in session_stats.items()}
        out["by_side_session"] = {k: self._finalize_winner_stat(v) for k, v in side_session_stats.items()}
        out["ok"] = True
        self._xau_winner_cache = dict(out)
        self._xau_winner_cache_ts = now_ts
        return out

    def _pick_winner_stat(self, profile: dict, *, side: str, session_key: str) -> tuple[str, dict]:
        min_side_session = max(3, int(getattr(config, "SCALPING_XAU_WINNER_MIN_SIDE_SESSION_SAMPLES", 6) or 6))
        min_side = max(min_side_session, int(getattr(config, "SCALPING_XAU_WINNER_MIN_SIDE_SAMPLES", 12) or 12))
        min_session = max(min_side_session, int(getattr(config, "SCALPING_XAU_WINNER_MIN_SESSION_SAMPLES", 10) or 10))
        min_overall = max(min_side, int(getattr(config, "SCALPING_XAU_WINNER_MIN_OVERALL_SAMPLES", 20) or 20))
        # Backward-compatible global override (optional).
        legacy_min = int(getattr(config, "SCALPING_XAU_WINNER_MIN_SAMPLES", 0) or 0)
        if legacy_min > 0:
            min_side_session = max(min_side_session, legacy_min)
            min_side = max(min_side, legacy_min)
            min_session = max(min_session, legacy_min)
            min_overall = max(min_overall, legacy_min)
        by_side_session = dict(profile.get("by_side_session") or {})
        by_side = dict(profile.get("by_side") or {})
        by_session = dict(profile.get("by_session") or {})
        overall = dict(profile.get("overall") or {})
        candidates = [
            (f"side_session:{side}:{session_key}", by_side_session.get(f"{side}|{session_key}") or {}, min_side_session),
            (f"side:{side}", by_side.get(side) or {}, min_side),
            (f"session:{session_key}", by_session.get(session_key) or {}, min_session),
            ("overall", overall, min_overall),
        ]
        for scope, stat, min_required in candidates:
            if int((stat or {}).get("resolved", 0) or 0) >= int(min_required):
                return scope, dict(stat or {})
        return "", {}

    def _winner_regime(self, stat: dict) -> str:
        if not stat:
            return "neutral"
        wr = float(stat.get("win_rate", 0.0) or 0.0)
        avg_pnl = float(stat.get("avg_pnl", 0.0) or 0.0)
        severe_wr = float(getattr(config, "SCALPING_XAU_WINNER_SEVERE_WR", 0.40) or 0.40)
        weak_wr = float(getattr(config, "SCALPING_XAU_WINNER_WEAK_WR", 0.50) or 0.50)
        strong_wr = float(getattr(config, "SCALPING_XAU_WINNER_STRONG_WR", 0.60) or 0.60)
        severe_avg = float(getattr(config, "SCALPING_XAU_WINNER_SEVERE_AVG_PNL", -3.0) or -3.0)
        weak_avg = float(getattr(config, "SCALPING_XAU_WINNER_WEAK_AVG_PNL", 0.0) or 0.0)
        strong_avg = float(getattr(config, "SCALPING_XAU_WINNER_STRONG_AVG_PNL", 0.0) or 0.0)
        if wr <= severe_wr and avg_pnl <= severe_avg:
            return "severe"
        if wr <= weak_wr and avg_pnl <= weak_avg:
            return "weak"
        if wr >= strong_wr and avg_pnl >= strong_avg:
            return "strong"
        return "neutral"

    def _apply_xau_winner_logic(
        self,
        signal: TradeSignal,
        *,
        apply_confidence: bool = True,
        apply_exits: bool = True,
    ) -> dict:
        info = {
            "enabled": bool(getattr(config, "SCALPING_XAU_WINNER_LOGIC_ENABLED", True)),
            "applied": False,
            "reason": "disabled",
            "scope": "",
            "regime": "neutral",
            "session_key": "",
            "resolved": 0,
            "win_rate": 0.0,
            "avg_pnl": 0.0,
            "hard_block": False,
        }
        if signal is None or (not bool(info["enabled"])):
            return info
        symbol = str(getattr(signal, "symbol", "") or "").upper()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if symbol not in {"XAUUSD", "GOLD"} or direction not in {"long", "short"}:
            info["reason"] = "not_xau_or_direction_invalid"
            return info

        session_raw = str(getattr(signal, "session", "") or "")
        if not session_raw:
            session_raw = ", ".join(session_manager.current_sessions()) or "off_hours"
        session_key = self._canonical_session_key(session_raw)
        info["session_key"] = session_key

        profile = self._load_xau_winner_profile()
        if not bool(profile.get("ok")):
            info["reason"] = str(profile.get("error") or "profile_unavailable")
            return info
        scope, stat = self._pick_winner_stat(profile, side=direction, session_key=session_key)
        if not scope or not stat:
            info["reason"] = "insufficient_samples"
            return info
        regime = self._winner_regime(stat)
        resolved = int(stat.get("resolved", 0) or 0)
        win_rate = float(stat.get("win_rate", 0.0) or 0.0)
        avg_pnl = float(stat.get("avg_pnl", 0.0) or 0.0)
        info.update(
            {
                "scope": scope,
                "regime": regime,
                "resolved": resolved,
                "win_rate": round(win_rate, 4),
                "avg_pnl": round(avg_pnl, 4),
                "reason": "regime_selected",
            }
        )

        changed = False
        conf_before = self._as_float(getattr(signal, "confidence", 0.0), 0.0)
        if apply_confidence:
            conf_delta = 0.0
            if regime == "strong":
                conf_delta = self._as_float(getattr(config, "SCALPING_XAU_WINNER_CONF_BONUS", 2.0), 2.0)
            elif regime == "weak":
                conf_delta = -abs(self._as_float(getattr(config, "SCALPING_XAU_WINNER_CONF_PENALTY_WEAK", 2.2), 2.2))
            elif regime == "severe":
                conf_delta = -abs(self._as_float(getattr(config, "SCALPING_XAU_WINNER_CONF_PENALTY_SEVERE", 4.8), 4.8))
            if abs(conf_delta) > 1e-9:
                conf_min = self._as_float(getattr(config, "SCALPING_XAU_WINNER_CONF_MIN", 50.0), 50.0)
                conf_max = self._as_float(getattr(config, "SCALPING_XAU_WINNER_CONF_MAX", 90.0), 90.0)
                signal.confidence = round(self._clamp(conf_before + conf_delta, conf_min, conf_max), 1)
                info["confidence_before"] = round(conf_before, 4)
                info["confidence_after"] = round(float(getattr(signal, "confidence", conf_before) or conf_before), 4)
                changed = True

        if apply_exits:
            entry = self._as_float(getattr(signal, "entry", 0.0), 0.0)
            stop = self._as_float(getattr(signal, "stop_loss", 0.0), 0.0)
            tp1 = self._as_float(getattr(signal, "take_profit_1", 0.0), 0.0)
            tp2 = self._as_float(getattr(signal, "take_profit_2", 0.0), 0.0)
            tp3 = self._as_float(getattr(signal, "take_profit_3", 0.0), 0.0)
            risk = abs(entry - stop)
            if entry > 0 and risk > 1e-9:
                rr1 = abs(tp1 - entry) / max(risk, 1e-9) if tp1 > 0 else 0.45
                rr2 = abs(tp2 - entry) / max(risk, 1e-9) if tp2 > 0 else 0.85
                rr3 = abs(tp3 - entry) / max(risk, 1e-9) if tp3 > 0 else 1.20
                rr1 = max(0.18, rr1)
                rr2 = max(rr1 + 0.06, rr2)
                rr3 = max(rr2 + 0.08, rr3)

                risk_mult = 1.0
                rr_mult = 1.0
                if regime == "strong":
                    risk_mult = self._as_float(getattr(config, "SCALPING_XAU_WINNER_RISK_MULT_STRONG", 1.0), 1.0)
                    rr_mult = self._as_float(getattr(config, "SCALPING_XAU_WINNER_RR_MULT_STRONG", 1.03), 1.03)
                elif regime == "weak":
                    risk_mult = self._as_float(getattr(config, "SCALPING_XAU_WINNER_RISK_MULT_WEAK", 0.90), 0.90)
                    rr_mult = self._as_float(getattr(config, "SCALPING_XAU_WINNER_RR_MULT_WEAK", 0.90), 0.90)
                elif regime == "severe":
                    risk_mult = self._as_float(getattr(config, "SCALPING_XAU_WINNER_RISK_MULT_SEVERE", 0.82), 0.82)
                    rr_mult = self._as_float(getattr(config, "SCALPING_XAU_WINNER_RR_MULT_SEVERE", 0.82), 0.82)

                atr = abs(self._as_float(getattr(signal, "atr", 0.0), 0.0))
                risk_new = max(entry * 0.00012, risk * max(0.35, risk_mult))
                if atr > 0:
                    risk_new = self._clamp(risk_new, max(entry * 0.00012, atr * 0.20), max(entry * 0.00075, atr * 1.35))
                rr1_new = max(0.16, rr1 * max(0.65, rr_mult))
                rr2_new = max(rr1_new + 0.06, rr2 * max(0.70, rr_mult))
                rr3_new = max(rr2_new + 0.08, rr3 * max(0.75, rr_mult))

                if direction == "long":
                    stop_new = entry - risk_new
                    tp1_new = entry + (risk_new * rr1_new)
                    tp2_new = entry + (risk_new * rr2_new)
                    tp3_new = entry + (risk_new * rr3_new)
                else:
                    stop_new = entry + risk_new
                    tp1_new = entry - (risk_new * rr1_new)
                    tp2_new = entry - (risk_new * rr2_new)
                    tp3_new = entry - (risk_new * rr3_new)

                old_rr = self._as_float(getattr(signal, "risk_reward", 0.0), 0.0)
                signal.stop_loss = self._round_price_for_symbol(symbol, stop_new)
                signal.take_profit_1 = self._round_price_for_symbol(symbol, tp1_new)
                signal.take_profit_2 = self._round_price_for_symbol(symbol, tp2_new)
                signal.take_profit_3 = self._round_price_for_symbol(symbol, tp3_new)
                denom = max(abs(entry - self._as_float(getattr(signal, "stop_loss", 0.0), 0.0)), 1e-9)
                signal.risk_reward = round(abs(self._as_float(getattr(signal, "take_profit_2", 0.0), 0.0) - entry) / denom, 2)
                info["rr_before"] = round(old_rr, 4)
                info["rr_after"] = round(float(getattr(signal, "risk_reward", old_rr) or old_rr), 4)
                changed = True

        if regime == "severe" and bool(getattr(config, "SCALPING_XAU_WINNER_HARD_BLOCK_SEVERE", False)):
            hard_min_conf = self._as_float(getattr(config, "SCALPING_XAU_WINNER_HARD_BLOCK_MIN_CONF", 58.0), 58.0)
            now_conf = self._as_float(getattr(signal, "confidence", conf_before), conf_before)
            if now_conf < hard_min_conf:
                info["hard_block"] = True
                info["reason"] = f"severe_regime_conf<{hard_min_conf:.1f}"

        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["winner_logic_enabled"] = True
        raw["winner_logic_scope"] = scope
        raw["winner_logic_regime"] = regime
        raw["winner_logic_session"] = session_key
        raw["winner_logic_resolved"] = resolved
        raw["winner_logic_win_rate"] = round(win_rate, 4)
        raw["winner_logic_avg_pnl"] = round(avg_pnl, 4)
        raw["winner_logic_applied"] = bool(changed)
        if "confidence_after" in info:
            raw["winner_logic_confidence_after"] = info["confidence_after"]
        if "rr_after" in info:
            raw["winner_logic_rr_after"] = info["rr_after"]
        signal.raw_scores = raw

        reasons = list(getattr(signal, "reasons", []) or [])
        reasons.append(
            f"Winner logic {regime} ({scope}) WR {win_rate*100:.1f}%/{resolved} avgPnL {avg_pnl:+.2f}"
        )
        signal.reasons = reasons[-12:]

        info["applied"] = bool(changed)
        return info

    def _retune_xau_exits(self, signal: TradeSignal) -> None:
        """
        Retune XAU scalp exits to avoid overly long TP ladders from the base scanner.
        Keeps scalping profile realistic by capping risk and tightening TP RR.
        """
        if signal is None:
            return
        if not bool(getattr(config, "SCALPING_XAU_EXIT_RETUNE_ENABLED", True)):
            return
        symbol = str(getattr(signal, "symbol", "") or "").upper()
        if symbol not in {"XAUUSD", "GOLD"}:
            return

        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return

        entry = self._as_float(getattr(signal, "entry", 0.0), 0.0)
        stop = self._as_float(getattr(signal, "stop_loss", 0.0), 0.0)
        if entry <= 0 or stop <= 0:
            return

        atr = abs(self._as_float(getattr(signal, "atr", 0.0), 0.0))
        if atr <= 0:
            atr = max(entry * 0.0006, 0.8)

        risk = abs(entry - stop)
        if risk <= 1e-9:
            return

        sl_cap_atr = max(0.5, self._as_float(getattr(config, "SCALPING_XAU_SL_MAX_ATR", 1.25), 1.25))
        max_risk = max(atr * sl_cap_atr, entry * 0.00025)
        if risk > max_risk:
            risk = float(max_risk)
            stop = entry - risk if direction == "long" else entry + risk

        tp1_rr = max(0.5, self._as_float(getattr(config, "SCALPING_XAU_TP1_RR", 0.9), 0.9))
        tp2_rr = max(tp1_rr + 0.1, self._as_float(getattr(config, "SCALPING_XAU_TP2_RR", 1.35), 1.35))
        tp3_rr = max(tp2_rr + 0.1, self._as_float(getattr(config, "SCALPING_XAU_TP3_RR", 1.9), 1.9))

        if direction == "long":
            tp1 = entry + risk * tp1_rr
            tp2 = entry + risk * tp2_rr
            tp3 = entry + risk * tp3_rr
            tp2_cap = entry + atr * max(1.0, self._as_float(getattr(config, "SCALPING_XAU_TP2_MAX_ATR", 1.8), 1.8))
            tp3_cap = entry + atr * max(1.2, self._as_float(getattr(config, "SCALPING_XAU_TP3_MAX_ATR", 2.6), 2.6))
            tp2 = min(tp2, tp2_cap)
            tp3 = min(max(tp3, tp2 + risk * 0.15), tp3_cap)
        else:
            tp1 = entry - risk * tp1_rr
            tp2 = entry - risk * tp2_rr
            tp3 = entry - risk * tp3_rr
            tp2_cap = entry - atr * max(1.0, self._as_float(getattr(config, "SCALPING_XAU_TP2_MAX_ATR", 1.8), 1.8))
            tp3_cap = entry - atr * max(1.2, self._as_float(getattr(config, "SCALPING_XAU_TP3_MAX_ATR", 2.6), 2.6))
            tp2 = max(tp2, tp2_cap)
            tp3 = max(min(tp3, tp2 - risk * 0.15), tp3_cap)

        old_tp1 = self._as_float(getattr(signal, "take_profit_1", 0.0), 0.0)
        old_tp2 = self._as_float(getattr(signal, "take_profit_2", 0.0), 0.0)
        old_tp3 = self._as_float(getattr(signal, "take_profit_3", 0.0), 0.0)
        old_rr = self._as_float(getattr(signal, "risk_reward", 0.0), 0.0)

        signal.stop_loss = self._round_price_for_symbol(symbol, stop)
        signal.take_profit_1 = self._round_price_for_symbol(symbol, tp1)
        signal.take_profit_2 = self._round_price_for_symbol(symbol, tp2)
        signal.take_profit_3 = self._round_price_for_symbol(symbol, tp3)
        denom = max(abs(entry - signal.stop_loss), 1e-9)
        signal.risk_reward = round(abs(signal.take_profit_2 - entry) / denom, 2)

        reasons = list(getattr(signal, "reasons", []) or [])
        reasons.append(
            f"⚙️ XAU scalp exit retune: RR {old_rr:.2f}→{signal.risk_reward:.2f} | "
            f"TP {old_tp1:.2f}/{old_tp2:.2f}/{old_tp3:.2f} -> "
            f"{signal.take_profit_1:.2f}/{signal.take_profit_2:.2f}/{signal.take_profit_3:.2f}"
        )
        signal.reasons = reasons[-12:]
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["scalp_exit_retuned"] = True
        raw["scalp_exit_retune_profile"] = "xau_conservative"
        raw["scalp_exit_old_rr"] = old_rr
        raw["scalp_exit_new_rr"] = signal.risk_reward
        signal.raw_scores = raw

    def _xau_m1_micro_snapshot(self) -> dict:
        out = {
            "available": False,
            "close": 0.0,
            "ema9": 0.0,
            "ema21": 0.0,
            "rsi14": 50.0,
            "atr14": 0.0,
            "momentum": 0.0,
            "aligned_long": False,
            "aligned_short": False,
        }
        if not bool(getattr(config, "SCALPING_XAU_FORCE_USE_M1_MICRO", True)):
            return out
        tf = str(getattr(config, "SCALPING_M1_TRIGGER_TF", "1m") or "1m")
        bars = max(60, int(getattr(config, "SCALPING_M1_TRIGGER_LOOKBACK_BARS", 120) or 120))
        df = xauusd_provider.fetch(tf, bars=bars)
        if df is None or getattr(df, "empty", True) or len(df) < 30:
            return out
        try:
            d = self.ta.add_all(df.tail(bars).copy())
        except Exception:
            d = df.tail(bars).copy()
        if d is None or getattr(d, "empty", True) or len(d) < 5:
            return out
        last = d.iloc[-1]
        prev = d.iloc[-2] if len(d) >= 2 else last
        close = self._as_float(last.get("close"), 0.0)
        ema9 = self._as_float(last.get("ema_9"), close)
        ema21 = self._as_float(last.get("ema_21"), close)
        rsi14 = self._as_float(last.get("rsi_14"), 50.0)
        atr14 = abs(self._as_float(last.get("atr_14"), 0.0))
        momentum = close - self._as_float(prev.get("close"), close)
        rsi_long_min = float(getattr(config, "SCALPING_XAU_FORCE_RSI_LONG_MIN", 51.0) or 51.0)
        rsi_short_max = float(getattr(config, "SCALPING_XAU_FORCE_RSI_SHORT_MAX", 49.0) or 49.0)
        out.update(
            {
                "available": close > 0,
                "close": close,
                "ema9": ema9,
                "ema21": ema21,
                "rsi14": rsi14,
                "atr14": atr14,
                "momentum": momentum,
                "aligned_long": bool(close >= ema9 and ema9 >= ema21 and rsi14 >= rsi_long_min),
                "aligned_short": bool(close <= ema9 and ema9 <= ema21 and rsi14 <= rsi_short_max),
            }
        )
        return out

    def _apply_xau_m1_entry_advantage(self, signal: TradeSignal, m1_micro: Optional[dict] = None) -> None:
        """
        Refine entry with M1 microstructure and set limit-entry mode for better price.
        """
        if signal is None:
            return
        symbol = str(getattr(signal, "symbol", "") or "").upper()
        if symbol not in {"XAUUSD", "GOLD"}:
            return
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return

        m1 = dict(m1_micro or {})
        if not m1.get("available"):
            m1 = self._xau_m1_micro_snapshot()
        if not m1.get("available"):
            return

        close_1m = self._as_float(m1.get("close"), 0.0)
        ema9_1m = self._as_float(m1.get("ema9"), close_1m)
        atr_1m = abs(self._as_float(m1.get("atr14"), 0.0))
        if close_1m <= 0:
            return
        if atr_1m <= 0:
            atr_1m = max(close_1m * 0.0002, 0.25)

        adv_mult = max(0.02, self._as_float(getattr(config, "SCALPING_XAU_FORCE_ENTRY_ADV_ATR_M1", 0.18), 0.18))
        max_dist_mult = max(0.08, self._as_float(getattr(config, "SCALPING_XAU_FORCE_ENTRY_MAX_DIST_ATR_M1", 0.45), 0.45))
        adv = max(close_1m * 0.00003, atr_1m * adv_mult)
        max_dist = max(close_1m * 0.00010, atr_1m * max_dist_mult)

        entry_old = self._as_float(getattr(signal, "entry", close_1m), close_1m)
        stop_old = self._as_float(getattr(signal, "stop_loss", 0.0), 0.0)
        tp1_old = self._as_float(getattr(signal, "take_profit_1", 0.0), 0.0)
        tp2_old = self._as_float(getattr(signal, "take_profit_2", 0.0), 0.0)
        tp3_old = self._as_float(getattr(signal, "take_profit_3", 0.0), 0.0)

        risk_old = abs(entry_old - stop_old)
        if risk_old <= 1e-9:
            risk_old = max(abs(close_1m - ema9_1m), atr_1m * 0.55, close_1m * 0.0002)

        rr1 = abs(tp1_old - entry_old) / max(risk_old, 1e-9) if tp1_old > 0 else 0.55
        rr2 = abs(tp2_old - entry_old) / max(risk_old, 1e-9) if tp2_old > 0 else 0.85
        rr3 = abs(tp3_old - entry_old) / max(risk_old, 1e-9) if tp3_old > 0 else 1.15
        rr1 = max(0.20, rr1)
        rr2 = max(rr1 + 0.08, rr2)
        rr3 = max(rr2 + 0.08, rr3)

        if direction == "long":
            candidate = min(close_1m - adv, ema9_1m)
            candidate = max(candidate, close_1m - max_dist)
            candidate = min(candidate, close_1m)
            entry_new = candidate
            stop_new = entry_new - risk_old
            tp1_new = entry_new + (risk_old * rr1)
            tp2_new = entry_new + (risk_old * rr2)
            tp3_new = entry_new + (risk_old * rr3)
        else:
            candidate = max(close_1m + adv, ema9_1m)
            candidate = min(candidate, close_1m + max_dist)
            candidate = max(candidate, close_1m)
            entry_new = candidate
            stop_new = entry_new + risk_old
            tp1_new = entry_new - (risk_old * rr1)
            tp2_new = entry_new - (risk_old * rr2)
            tp3_new = entry_new - (risk_old * rr3)

        signal.entry = self._round_price_for_symbol(symbol, entry_new)
        signal.stop_loss = self._round_price_for_symbol(symbol, stop_new)
        signal.take_profit_1 = self._round_price_for_symbol(symbol, tp1_new)
        signal.take_profit_2 = self._round_price_for_symbol(symbol, tp2_new)
        signal.take_profit_3 = self._round_price_for_symbol(symbol, tp3_new)
        signal.risk_reward = round(abs(signal.take_profit_2 - signal.entry) / max(abs(signal.entry - signal.stop_loss), 1e-9), 2)
        signal.entry_type = "limit"
        reasons = list(getattr(signal, "reasons", []) or [])
        reasons.append(
            f"M1 micro entry advantage: {entry_old:.2f}->{signal.entry:.2f} (limit, adv={adv:.2f})"
        )
        signal.reasons = reasons[-12:]
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["scalp_m1_micro_entry"] = True
        raw["scalp_m1_entry_old"] = round(entry_old, 5)
        raw["scalp_m1_entry_new"] = round(float(signal.entry), 5)
        raw["scalp_m1_entry_advantage"] = round(abs(float(signal.entry) - close_1m), 5)
        raw["scalp_m1_snapshot"] = {
            "close": round(close_1m, 5),
            "ema9": round(ema9_1m, 5),
            "atr14": round(atr_1m, 5),
            "rsi14": round(self._as_float(m1.get("rsi14"), 50.0), 3),
            "momentum": round(self._as_float(m1.get("momentum"), 0.0), 5),
        }
        signal.raw_scores = raw

    @staticmethod
    def _xau_m1_ref_close_at_lookback(m1_df, lookback: int) -> tuple[float, str]:
        if m1_df is None or getattr(m1_df, "empty", True):
            return 0.0, ""
        n = len(m1_df)
        lb = int(lookback)
        if lb < 1 or n <= lb:
            return 0.0, ""
        try:
            ref = float(m1_df["close"].iloc[-1 - lb])
        except Exception:
            return 0.0, ""
        ts_s = ""
        try:
            ts_s = str(m1_df.index[-1 - lb])
        except Exception:
            pass
        return ref, ts_s

    def _apply_xau_entry_template_m1_bias(self, signal: TradeSignal, trigger: Optional[dict] = None) -> None:
        """
        Nudge entry + SL + TPs toward the mined 1m template anchor (close N bars ago),
        preserving risk width. Applies to the shared XAU scalp signal → all scheduler families.
        """
        if signal is None:
            return
        if not bool(getattr(config, "ENTRY_TEMPLATE_SCANNER_BIAS_ENABLED", False)):
            return
        sym = str(getattr(signal, "symbol", "") or "").strip().upper()
        if sym not in {"XAUUSD", "GOLD"}:
            return
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction not in {"long", "short"}:
            return
        catalog = load_catalog()
        if not catalog:
            return
        bucket = session_bucket_for_entry_template(signal)
        tpl = pick_template_block(catalog, symbol="XAUUSD", session_bucket=bucket, direction=direction)
        if not tpl:
            tpl = pick_template_block(catalog, symbol="XAUUSD", session_bucket="global", direction=direction)
        if not tpl:
            return
        lookback = int(tpl.get("best_lookback_bars") or 0)
        if lookback < 1:
            return
        bars_need = max(80, lookback + 25)
        try:
            m1_df = xauusd_provider.fetch(str(getattr(config, "SCALPING_M1_TRIGGER_TF", "1m")), bars=bars_need)
        except Exception:
            m1_df = None
        if m1_df is None or getattr(m1_df, "empty", True):
            return
        ref, ref_ts = self._xau_m1_ref_close_at_lookback(m1_df, lookback)
        if ref <= 0:
            return
        entry = self._as_float(getattr(signal, "entry", 0.0), 0.0)
        stop = self._as_float(getattr(signal, "stop_loss", 0.0), 0.0)
        if entry <= 0 or stop <= 0:
            return
        risk = abs(entry - stop)
        if risk <= 1e-9:
            return
        sym_round = "XAUUSD"
        min_off = self._as_float(getattr(config, "ENTRY_TEMPLATE_SCANNER_MIN_OFFSET_RISK_TO_ACT", 0.04), 0.04)
        offset_risk = (ref - entry) / risk
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["entry_template_scanner_session_bucket"] = bucket
        raw["entry_template_scanner_lookback_bars"] = lookback
        raw["entry_template_scanner_ref_close"] = round(ref, 5)
        raw["entry_template_scanner_ref_bar_ts"] = str(ref_ts or "")[:48]
        raw["entry_template_scanner_offset_risk_units_pre"] = round(offset_risk, 5)
        if abs(offset_risk) < min_off:
            raw["entry_template_scanner_bias_applied"] = False
            raw["entry_template_scanner_bias_skip_reason"] = "offset_below_min_risk_threshold"
            signal.raw_scores = raw
            if trigger is not None:
                trigger["entry_template_scanner"] = {"applied": False, "reason": "offset_too_small"}
            return
        cap = max(0.02, self._as_float(getattr(config, "ENTRY_TEMPLATE_SCANNER_MAX_SHIFT_RISK_RATIO", 0.22), 0.22))
        max_step = risk * cap
        delta = ref - entry
        if delta > max_step:
            delta = max_step
        elif delta < -max_step:
            delta = -max_step
        if abs(delta) < 1e-6:
            raw["entry_template_scanner_bias_applied"] = False
            raw["entry_template_scanner_bias_skip_reason"] = "delta_rounded_zero"
            signal.raw_scores = raw
            return
        entry_n = self._round_price_for_symbol(sym_round, entry + delta)
        d_applied = float(entry_n - entry)
        if abs(d_applied) < 1e-6:
            raw["entry_template_scanner_bias_applied"] = False
            raw["entry_template_scanner_bias_skip_reason"] = "rounded_no_change"
            signal.raw_scores = raw
            return
        signal.entry = entry_n
        signal.stop_loss = self._round_price_for_symbol(sym_round, stop + d_applied)
        for _tp in ("take_profit_1", "take_profit_2", "take_profit_3"):
            v = self._as_float(getattr(signal, _tp, 0.0), 0.0)
            if v > 0:
                setattr(signal, _tp, self._round_price_for_symbol(sym_round, v + d_applied))
        denom = max(abs(signal.entry - signal.stop_loss), 1e-9)
        tp2 = self._as_float(getattr(signal, "take_profit_2", 0.0), 0.0)
        if tp2 > 0:
            signal.risk_reward = round(abs(tp2 - signal.entry) / denom, 2)
        raw["entry_template_scanner_bias_applied"] = True
        raw["entry_template_scanner_shift_price"] = round(d_applied, 5)
        raw["entry_template_scanner_shift_risk_units"] = round(d_applied / risk, 5) if risk > 1e-9 else 0.0
        raw["entry_template_scanner_max_shift_risk_ratio"] = round(cap, 4)
        signal.raw_scores = raw
        reasons = list(getattr(signal, "reasons", []) or [])
        reasons.append(
            f"Entry template M1 bias: LB{lookback} ref={ref:.2f} shift={d_applied:+.2f} ({bucket})"
        )
        signal.reasons = reasons[-12:]
        if trigger is not None:
            trigger["entry_template_scanner"] = {
                "applied": True,
                "lookback": lookback,
                "ref_close": ref,
                "shift": d_applied,
                "bucket": bucket,
            }

    def _build_xau_forced_signal(self, base_signal: Optional[TradeSignal] = None) -> Optional[TradeSignal]:
        """
        Build an always-on micro scalping signal for XAUUSD.
        This is used as a fallback only in the scalping pipeline and does not
        alter the primary H1 system.
        """
        entry_tf = str(getattr(config, "SCALPING_ENTRY_TF", "5m") or "5m")
        lookback = max(120, int(getattr(config, "SCALPING_XAU_FORCE_LOOKBACK_BARS", 220) or 220))
        df_m5 = xauusd_provider.fetch(entry_tf, bars=lookback)
        if df_m5 is None or getattr(df_m5, "empty", True) or len(df_m5) < 80:
            if base_signal is None:
                return None
            try:
                # Use last known base signal as fallback-of-fallback.
                sig = deepcopy(base_signal)
            except Exception:
                return None
            sig.confidence = max(
                float(getattr(config, "SCALPING_XAU_FORCE_MIN_CONFIDENCE", 56.0) or 56.0),
                min(float(getattr(sig, "confidence", 0.0) or 0.0), 72.0),
            )
            reasons = list(getattr(sig, "reasons", []) or [])
            reasons.append("Force fallback: base setup reused (market data short)")
            sig.reasons = reasons[-12:]
            raw = dict(getattr(sig, "raw_scores", {}) or {})
            raw["scalp_force_mode"] = True
            raw["scalp_force_data_mode"] = "base_reuse"
            sig.raw_scores = raw
            return sig

        try:
            d = self.ta.add_all(df_m5.tail(lookback).copy())
        except Exception:
            d = df_m5.tail(lookback).copy()

        if d is None or getattr(d, "empty", True) or len(d) < 50:
            return None

        last = d.iloc[-1]
        prev = d.iloc[-2] if len(d) >= 2 else last

        close = self._as_float(last.get("close"), 0.0)
        if close <= 0:
            return None
        ema9 = self._as_float(last.get("ema_9"), close)
        ema21 = self._as_float(last.get("ema_21"), close)
        rsi14 = self._as_float(last.get("rsi_14"), 50.0)
        atr = abs(self._as_float(last.get("atr_14"), 0.0))
        if atr <= 0:
            atr = max(close * 0.00055, 0.9)
        momentum = close - self._as_float(prev.get("close"), close)

        trend_label = "ranging"
        try:
            df_h1 = xauusd_provider.fetch("1h", bars=220)
            if df_h1 is not None and not getattr(df_h1, "empty", True):
                trend_label = str(self.ta.determine_trend(self.ta.add_all(df_h1.copy())) or "ranging")
        except Exception:
            trend_label = "ranging"
        m1_micro = self._xau_m1_micro_snapshot()

        long_score = 0.0
        short_score = 0.0
        if close >= ema9:
            long_score += 1.0
        else:
            short_score += 1.0
        if ema9 >= ema21:
            long_score += 1.1
        else:
            short_score += 1.1
        if rsi14 >= 52:
            long_score += 0.9
        elif rsi14 <= 48:
            short_score += 0.9
        else:
            long_score += 0.2
            short_score += 0.2
        if momentum >= 0:
            long_score += 0.8
        else:
            short_score += 0.8
        if trend_label == "bullish":
            long_score += 0.8
        elif trend_label == "bearish":
            short_score += 0.8
        if bool(m1_micro.get("available")):
            if bool(m1_micro.get("aligned_long")):
                long_score += 1.15
            if bool(m1_micro.get("aligned_short")):
                short_score += 1.15
            m1_mom = self._as_float(m1_micro.get("momentum"), 0.0)
            if m1_mom > 0:
                long_score += 0.35
            elif m1_mom < 0:
                short_score += 0.35

        if base_signal is not None:
            base_dir = str(getattr(base_signal, "direction", "") or "").strip().lower()
            if base_dir == "long":
                long_score += 0.45
            elif base_dir == "short":
                short_score += 0.45

        direction = "long" if long_score >= short_score else "short"
        edge = abs(long_score - short_score)
        conf_base = float(getattr(config, "SCALPING_XAU_FORCE_CONFIDENCE_BASE", 58.0) or 58.0)
        min_conf = float(getattr(config, "SCALPING_XAU_FORCE_MIN_CONFIDENCE", 56.0) or 56.0)
        confidence = max(min_conf, min(78.0, conf_base + edge * 5.8))

        risk_mult = max(0.20, self._as_float(getattr(config, "SCALPING_XAU_FORCE_SL_ATR", 0.60), 0.60))
        risk = max(atr * risk_mult, close * 0.00018)
        risk = min(risk, max(atr * 0.95, close * 0.0005))
        if risk <= 0:
            return None

        tp1_rr = max(0.20, self._as_float(getattr(config, "SCALPING_XAU_FORCE_TP1_RR", 0.55), 0.55))
        tp2_rr = max(tp1_rr + 0.08, self._as_float(getattr(config, "SCALPING_XAU_FORCE_TP2_RR", 0.90), 0.90))
        tp3_rr = max(tp2_rr + 0.08, self._as_float(getattr(config, "SCALPING_XAU_FORCE_TP3_RR", 1.25), 1.25))

        if direction == "long":
            stop = close - risk
            tp1 = close + (risk * tp1_rr)
            tp2 = close + (risk * tp2_rr)
            tp3 = close + (risk * tp3_rr)
        else:
            stop = close + risk
            tp1 = close - (risk * tp1_rr)
            tp2 = close - (risk * tp2_rr)
            tp3 = close - (risk * tp3_rr)

        session = ", ".join(session_manager.current_sessions()) or "off_hours"
        reasons = [
            "Force fallback mode: keep cadence 1 signal / 5m bar",
            f"H1 bias={trend_label} | M5 momentum={momentum:+.2f}",
            "Quick-exit profile active (low-risk micro TP/SL)",
        ]
        warnings = ["Use reduced risk: fallback scalping flow"]
        raw = {
            "scalp_force_mode": True,
            "scalp_force_long_score": round(long_score, 4),
            "scalp_force_short_score": round(short_score, 4),
            "scalp_force_edge": round(edge, 4),
            "scalp_force_confidence": round(confidence, 2),
            "scalp_force_trend_h1": trend_label,
            "scalp_force_momentum": round(momentum, 6),
            "scalp_force_m1_available": bool(m1_micro.get("available")),
            "scalp_force_m1_aligned_long": bool(m1_micro.get("aligned_long")),
            "scalp_force_m1_aligned_short": bool(m1_micro.get("aligned_short")),
            "scalp_force_last_m5_bar_utc": str(d.index[-1].isoformat()) if len(d.index) else "",
            "scalp_force_last_h1_bar_utc": str(df_h1.index[-1].isoformat()) if ('df_h1' in locals() and df_h1 is not None and len(df_h1.index)) else "",
        }

        sig = TradeSignal(
            symbol="XAUUSD",
            direction=direction,
            confidence=round(confidence, 1),
            entry=self._round_price_for_symbol("XAUUSD", close),
            stop_loss=self._round_price_for_symbol("XAUUSD", stop),
            take_profit_1=self._round_price_for_symbol("XAUUSD", tp1),
            take_profit_2=self._round_price_for_symbol("XAUUSD", tp2),
            take_profit_3=self._round_price_for_symbol("XAUUSD", tp3),
            risk_reward=round(abs(tp2 - close) / max(abs(close - stop), 1e-9), 2),
            timeframe=entry_tf,
            session=session,
            trend=trend_label,
            rsi=round(rsi14, 2),
            atr=round(atr, 4),
            pattern="SCALP_FLOW_FORCE",
            reasons=reasons,
            warnings=warnings,
            raw_scores=raw,
        )
        self._apply_xau_m1_entry_advantage(sig, m1_micro=m1_micro)
        return sig

    def _maybe_force_xau_result(
        self,
        *,
        source: str,
        blocked_status: str,
        blocked_reason: str,
        signal: Optional[TradeSignal],
        trigger: Optional[dict] = None,
    ) -> Optional[ScalpingScanResult]:
        if not bool(getattr(config, "SCALPING_XAU_FORCE_EVERY_SCAN", False)):
            return None
        forced_signal = self._build_xau_forced_signal(base_signal=signal)
        if forced_signal is None:
            return None
        merged_trigger = dict(trigger or {})
        merged_trigger["forced_mode"] = True
        merged_trigger["forced_from_status"] = str(blocked_status or "")
        merged_trigger["forced_from_reason"] = str(blocked_reason or "")
        forced_direction = str(getattr(forced_signal, "direction", "") or "").strip().lower()
        forced_raw = dict(getattr(forced_signal, "raw_scores", {}) or {})
        forced_h1_trend = str(
            forced_raw.get("signal_h1_trend")
            or forced_raw.get("scalp_force_trend_h1")
            or forced_raw.get("trend_h1")
            or forced_raw.get("h1_trend")
            or ""
        ).strip().lower()
        countertrend_confirmed = bool(
            merged_trigger.get("countertrend_confirmed")
            or merged_trigger.get("xau_mtf_countertrend_confirmed")
            or ((merged_trigger.get("winner_logic") or {}).get("countertrend_confirmed"))
        )
        block_reason_tokens = self._parse_lower_csv(getattr(config, "SCALPING_XAU_FORCE_BLOCK_REASONS", ""))
        observed_reasons = {
            self._normalize_reason_token(blocked_status),
            self._normalize_reason_token(blocked_reason),
            self._normalize_reason_token(str(merged_trigger.get("xau_diag_status") or "")),
            self._normalize_reason_token(str(merged_trigger.get("forced_from_reason") or "")),
        }
        if forced_direction == "short" and any(token and token in block_reason_tokens for token in observed_reasons):
            return None
        if (
            forced_direction == "long"
            and bool(getattr(config, "SCALPING_XAU_FORCE_REQUIRE_COUNTERTREND_CONFIRMED_LONG", True))
            and forced_h1_trend == "bearish"
            and not countertrend_confirmed
        ):
            return None
        self._apply_xau_m1_entry_advantage(forced_signal)
        self._retune_xau_exits(forced_signal)
        winner_forced = self._apply_xau_winner_logic(forced_signal, apply_confidence=True, apply_exits=True)
        if winner_forced:
            merged_trigger["winner_logic"] = winner_forced
        if bool((winner_forced or {}).get("hard_block")):
            return None
        self._apply_xau_multi_tf_context(forced_signal, trigger=merged_trigger)
        self._tag_signal(forced_signal, source=source, trigger=merged_trigger)
        self._apply_xau_entry_template_m1_bias(forced_signal, trigger=merged_trigger)
        return ScalpingScanResult(
            source=source,
            symbol="XAUUSD",
            status="ready",
            reason=f"forced:{blocked_status}",
            signal=forced_signal,
            trigger=merged_trigger,
        )

    def _m1_trigger(self, df_m1, direction: str) -> tuple[bool, dict]:
        out = {
            "ok": False,
            "reason": "unknown",
            "close": None,
            "ema9": None,
            "ema21": None,
            "rsi14": None,
            "ref_high": None,
            "ref_low": None,
            "buffer": None,
            "checks": {},
        }
        if df_m1 is None or getattr(df_m1, "empty", True):
            out["reason"] = "m1_data_unavailable"
            return False, out

        bars = max(20, int(getattr(config, "SCALPING_M1_TRIGGER_LOOKBACK_BARS", 120) or 120))
        d = df_m1.tail(bars).copy()
        if len(d) < 20:
            out["reason"] = "m1_data_too_short"
            return False, out
        try:
            d = self.ta.add_all(d)
        except Exception as e:
            out["reason"] = f"m1_indicator_error:{e}"
            return False, out

        last = d.iloc[-1]
        close = self._as_float(last.get("close"), 0.0)
        ema9 = self._as_float(last.get("ema_9"), close)
        ema21 = self._as_float(last.get("ema_21"), close)
        rsi14 = self._as_float(last.get("rsi_14"), 50.0)
        atr = abs(self._as_float(last.get("atr_14"), 0.0))
        breakout_bars = max(2, int(getattr(config, "SCALPING_M1_TRIGGER_BREAKOUT_BARS", 3) or 3))
        prior = d.iloc[-(breakout_bars + 1):-1] if len(d) > (breakout_bars + 1) else d.iloc[:-1]
        if prior is None or getattr(prior, "empty", True):
            out["reason"] = "m1_prior_window_missing"
            return False, out

        ref_high = self._as_float(prior["high"].max(), close)
        ref_low = self._as_float(prior["low"].min(), close)
        prev_close = self._as_float(prior["close"].iloc[-1], close)
        buffer_px = max(close * 0.00008, atr * 0.10)

        out.update(
            {
                "close": round(close, 6),
                "ema9": round(ema9, 6),
                "ema21": round(ema21, 6),
                "rsi14": round(rsi14, 3),
                "ref_high": round(ref_high, 6),
                "ref_low": round(ref_low, 6),
                "buffer": round(buffer_px, 6),
            }
        )

        d_side = str(direction or "").strip().lower()
        if d_side == "long":
            rsi_min = float(getattr(config, "SCALPING_M1_TRIGGER_RSI_LONG_MIN", 52.0) or 52.0)
            rsi_max = float(getattr(config, "SCALPING_M1_TRIGGER_RSI_LONG_MAX", 70.0) or 70.0)
            ref_high_mult = float(getattr(config, "SCALPING_M1_TRIGGER_REFHIGH_BUFFER_MULT_LONG", 1.0) or 1.0)
            checks = {
                "close_vs_ema9": close >= (ema9 - buffer_px * 0.20),
                "ema9_vs_ema21": ema9 >= (ema21 - buffer_px * 0.20),
                "rsi_gate": rsi14 >= rsi_min,
                "rsi_ceiling": rsi14 <= rsi_max,
                "ref_high_break": close >= (ref_high - buffer_px * ref_high_mult),
                "prev_close_hold": close >= (prev_close - buffer_px * 0.50),
            }
            ok = all(checks.values())
            out["reason"] = "m1_long_confirmed" if ok else "m1_long_not_confirmed"
            out["ok"] = bool(ok)
            out["checks"] = checks
            out["ref_high_mult"] = round(ref_high_mult, 3)
            return bool(ok), out

        if d_side == "short":
            rsi_max = float(getattr(config, "SCALPING_M1_TRIGGER_RSI_SHORT_MAX", 48.0) or 48.0)
            ref_low_mult = float(getattr(config, "SCALPING_M1_TRIGGER_REFLOW_BUFFER_MULT_SHORT", 1.25) or 1.25)
            checks = {
                "close_vs_ema9": close <= (ema9 + buffer_px * 0.20),
                "ema9_vs_ema21": ema9 <= (ema21 + buffer_px * 0.20),
                "rsi_gate": rsi14 <= rsi_max,
                "ref_low_break": close <= (ref_low + buffer_px * ref_low_mult),
                "prev_close_hold": close <= (prev_close + buffer_px * 0.50),
            }
            ok = all(checks.values())
            out["reason"] = "m1_short_confirmed" if ok else "m1_short_not_confirmed"
            out["ok"] = bool(ok)
            out["checks"] = checks
            out["ref_low_mult"] = round(ref_low_mult, 3)
            return bool(ok), out

        out["reason"] = "invalid_direction"
        return False, out

    @staticmethod
    def _tag_signal(signal: TradeSignal, *, source: str, trigger: dict) -> None:
        signal.timeframe = f"{str(getattr(config, 'SCALPING_ENTRY_TF', '5m'))}+{str(getattr(config, 'SCALPING_M1_TRIGGER_TF', '1m'))}"
        reasons = list(getattr(signal, "reasons", []) or [])
        reasons.append("⚡ Scalping profile active (M5 entry + M1 trigger)")
        signal.reasons = reasons[-10:]
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["scalping"] = True
        raw["scalping_source"] = str(source)
        raw["scalping_trigger"] = dict(trigger or {})
        raw["scalping_entry_tf"] = str(getattr(config, "SCALPING_ENTRY_TF", "5m"))
        raw["scalping_trigger_tf"] = str(getattr(config, "SCALPING_M1_TRIGGER_TF", "1m"))
        signal.raw_scores = raw

    def _xau_regime_snapshot(self) -> dict:
        trend_tf = str(getattr(config, "SCALPING_XAU_REGIME_GUARD_TREND_TF", "1d") or "1d")
        structure_tf = str(getattr(config, "SCALPING_XAU_REGIME_GUARD_STRUCTURE_TF", "4h") or "4h")
        out = {
            "trend_tf": trend_tf,
            "structure_tf": structure_tf,
            "trend": "ranging",
            "structure": "ranging",
            "dominant_side": None,
        }
        try:
            df_trend = xauusd_provider.fetch(trend_tf, bars=220)
            if df_trend is not None and not getattr(df_trend, "empty", True):
                out["trend"] = str(self.ta.determine_trend(self.ta.add_all(df_trend.copy())) or "ranging")
        except Exception:
            pass
        try:
            df_structure = xauusd_provider.fetch(structure_tf, bars=220)
            if df_structure is not None and not getattr(df_structure, "empty", True):
                out["structure"] = str(self.ta.determine_trend(self.ta.add_all(df_structure.copy())) or "ranging")
        except Exception:
            pass

        if out["trend"] == "bullish" and out["structure"] == "bullish":
            out["dominant_side"] = "long"
        elif out["trend"] == "bearish" and out["structure"] == "bearish":
            out["dominant_side"] = "short"
        return out

    def _xau_execution_mtf_snapshot(self) -> dict:
        out = {
            "d1_tf": "1d",
            "h1_tf": "1h",
            "h4_tf": "4h",
            "d1_trend": "unknown",
            "h1_trend": "unknown",
            "h4_trend": "unknown",
            "aligned_side": "",
            "alignment": "mixed",
            "strict_aligned_side": "",
            "strict_alignment": "mixed",
            "d1_open": 0.0,
            "d1_last": 0.0,
            "h1_open": 0.0,
            "h1_last": 0.0,
            "h4_open": 0.0,
            "h4_last": 0.0,
        }
        try:
            df_d1 = xauusd_provider.fetch("1d", bars=220)
            if df_d1 is not None and not getattr(df_d1, "empty", True):
                out["d1_open"] = float(df_d1["open"].iloc[-1] or 0.0)
                out["d1_last"] = float(df_d1["close"].iloc[-1] or 0.0)
                out["d1_trend"] = str(self.ta.determine_trend(self.ta.add_all(df_d1.copy())) or "unknown").strip().lower() or "unknown"
        except Exception:
            pass
        try:
            df_h1 = xauusd_provider.fetch("1h", bars=220)
            if df_h1 is not None and not getattr(df_h1, "empty", True):
                out["h1_open"] = float(df_h1["open"].iloc[-1] or 0.0)
                out["h1_last"] = float(df_h1["close"].iloc[-1] or 0.0)
                out["h1_trend"] = str(self.ta.determine_trend(self.ta.add_all(df_h1.copy())) or "unknown").strip().lower() or "unknown"
        except Exception:
            pass
        try:
            df_h4 = xauusd_provider.fetch("4h", bars=220)
            if df_h4 is not None and not getattr(df_h4, "empty", True):
                out["h4_open"] = float(df_h4["open"].iloc[-1] or 0.0)
                out["h4_last"] = float(df_h4["close"].iloc[-1] or 0.0)
                out["h4_trend"] = str(self.ta.determine_trend(self.ta.add_all(df_h4.copy())) or "unknown").strip().lower() or "unknown"
        except Exception:
            pass
        d1 = str(out.get("d1_trend") or "").strip().lower()
        h1 = str(out.get("h1_trend") or "").strip().lower()
        h4 = str(out.get("h4_trend") or "").strip().lower()
        if h1 == "bullish" and h4 == "bullish":
            out["aligned_side"] = "long"
            out["alignment"] = "aligned_bullish"
        elif h1 == "bearish" and h4 == "bearish":
            out["aligned_side"] = "short"
            out["alignment"] = "aligned_bearish"
        elif h1 not in {"", "unknown"} or h4 not in {"", "unknown"}:
            out["alignment"] = "mixed"
        else:
            out["alignment"] = "unknown"
        if d1 == "bullish" and h1 == "bullish" and h4 == "bullish":
            out["strict_aligned_side"] = "long"
            out["strict_alignment"] = "aligned_bullish"
        elif d1 == "bearish" and h1 == "bearish" and h4 == "bearish":
            out["strict_aligned_side"] = "short"
            out["strict_alignment"] = "aligned_bearish"
        elif d1 not in {"", "unknown"} or h1 not in {"", "unknown"} or h4 not in {"", "unknown"}:
            out["strict_alignment"] = "mixed"
        else:
            out["strict_alignment"] = "unknown"
        return out

    def _apply_xau_multi_tf_context(
        self,
        signal: TradeSignal,
        *,
        trigger: Optional[dict] = None,
        mtf_snapshot: Optional[dict] = None,
    ) -> None:
        if signal is None:
            return
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        if symbol != "XAUUSD":
            return
        snap = dict(mtf_snapshot or self._xau_execution_mtf_snapshot() or {})
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        d1_trend = str(
            snap.get("d1_trend")
            or raw.get("signal_d1_trend")
            or raw.get("trend_d1")
            or raw.get("d1_trend")
            or "unknown"
        ).strip().lower() or "unknown"
        h1_trend = str(
            snap.get("h1_trend")
            or raw.get("signal_h1_trend")
            or raw.get("scalp_force_trend_h1")
            or raw.get("trend_h1")
            or raw.get("h1_trend")
            or "unknown"
        ).strip().lower() or "unknown"
        h4_trend = str(
            snap.get("h4_trend")
            or raw.get("signal_h4_trend")
            or raw.get("scalp_force_trend_h4")
            or raw.get("trend_h4")
            or raw.get("h4_trend")
            or "unknown"
        ).strip().lower() or "unknown"
        aligned_side = str(snap.get("aligned_side") or "").strip().lower()
        if not aligned_side:
            if h1_trend == "bullish" and h4_trend == "bullish":
                aligned_side = "long"
            elif h1_trend == "bearish" and h4_trend == "bearish":
                aligned_side = "short"
        strict_aligned_side = str(snap.get("strict_aligned_side") or "").strip().lower()
        if not strict_aligned_side:
            if d1_trend == "bullish" and h1_trend == "bullish" and h4_trend == "bullish":
                strict_aligned_side = "long"
            elif d1_trend == "bearish" and h1_trend == "bearish" and h4_trend == "bearish":
                strict_aligned_side = "short"
        trig = dict(trigger or {})
        countertrend_confirmed = bool(
            raw.get("countertrend_confirmed")
            or raw.get("xau_mtf_countertrend_confirmed")
            or trig.get("countertrend_confirmed")
        )
        raw["signal_d1_trend"] = d1_trend
        raw["trend_d1"] = d1_trend
        raw["d1_trend"] = d1_trend
        raw["signal_h1_trend"] = h1_trend
        raw["trend_h1"] = h1_trend
        raw["h1_trend"] = h1_trend
        raw["signal_h4_trend"] = h4_trend
        raw["trend_h4"] = h4_trend
        raw["h4_trend"] = h4_trend
        if bool(raw.get("scalp_force_mode")):
            raw["scalp_force_trend_h1"] = h1_trend
            raw["scalp_force_trend_h4"] = h4_trend
        raw["xau_mtf_countertrend_confirmed"] = countertrend_confirmed
        raw["xau_multi_tf_snapshot"] = {
            "d1_tf": str(snap.get("d1_tf") or "1d"),
            "h1_tf": str(snap.get("h1_tf") or "1h"),
            "h4_tf": str(snap.get("h4_tf") or "4h"),
            "d1_trend": d1_trend,
            "h1_trend": h1_trend,
            "h4_trend": h4_trend,
            "alignment": str(snap.get("alignment") or ("aligned" if aligned_side else "mixed")),
            "aligned_side": aligned_side,
            "strict_alignment": str(snap.get("strict_alignment") or ("aligned" if strict_aligned_side else "mixed")),
            "strict_aligned_side": strict_aligned_side,
            "countertrend_confirmed": countertrend_confirmed,
            "d1_open": float(snap.get("d1_open", 0.0) or 0.0),
            "d1_last": float(snap.get("d1_last", 0.0) or 0.0),
            "h1_open": float(snap.get("h1_open", 0.0) or 0.0),
            "h1_last": float(snap.get("h1_last", 0.0) or 0.0),
            "h4_open": float(snap.get("h4_open", 0.0) or 0.0),
            "h4_last": float(snap.get("h4_last", 0.0) or 0.0),
        }
        signal.raw_scores = raw

    def _xau_regime_guard(
        self,
        signal: TradeSignal,
        trigger: Optional[dict] = None,
    ) -> tuple[bool, dict]:
        snap = self._xau_regime_snapshot()
        side = str(getattr(signal, "direction", "") or "").strip().lower()
        dominant = str(snap.get("dominant_side") or "").strip().lower()
        result = dict(snap)
        result["ok"] = True
        result["reason"] = "aligned_or_neutral"
        if dominant not in {"long", "short"} or side not in {"long", "short"}:
            return True, result
        if side == dominant:
            return True, result

        trig = dict(trigger or {})
        counter_confirmed = bool(trig.get("countertrend_confirmed"))
        if not counter_confirmed:
            result["ok"] = False
            result["reason"] = f"counter_trend_blocked:{side}_vs_{dominant}"
            return False, result
        result["reason"] = f"counter_trend_confirmed:{side}_vs_{dominant}"
        return True, result

    def scan_xauusd(self, require_enabled: bool = True) -> ScalpingScanResult:
        source = "scalp_xauusd"
        if not bool(getattr(config, "SCALPING_ENABLED", False)):
            return ScalpingScanResult(source=source, symbol="XAUUSD", status="disabled", reason="scalping_disabled")
        if require_enabled and (not config.scalping_symbol_enabled("XAUUSD")):
            return ScalpingScanResult(source=source, symbol="XAUUSD", status="disabled", reason="symbol_not_enabled")
        if bool(getattr(config, "SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED", True)) and (not bool(session_manager.is_xauusd_market_open())):
            return ScalpingScanResult(
                source=source,
                symbol="XAUUSD",
                status="market_closed",
                reason="xauusd_market_closed_weekend_window",
            )

        with self._temporary_config(
            XAUUSD_TREND_TF=str(getattr(config, "SCALPING_XAU_TREND_TF", "1h")),
            XAUUSD_STRUCTURE_TF=str(getattr(config, "SCALPING_XAU_STRUCTURE_TF", "15m")),
            XAUUSD_ENTRY_TF=str(getattr(config, "SCALPING_ENTRY_TF", "5m")),
        ):
            signal = xauusd_scanner.scan()
        winner_conf_hint: dict = {}
        if signal is None:
            diag = xauusd_scanner.get_last_scan_diagnostics()
            fallback = dict(diag.get("fallback", {}) or {})
            fb_long = dict(fallback.get("long", {}) or {})
            fb_short = dict(fallback.get("short", {}) or {})
            fb_reason = str(fallback.get("reason", "") or "").strip()
            blocked = ScalpingScanResult(
                source=source,
                symbol="XAUUSD",
                status="no_signal",
                reason=fb_reason or "base_scanner_no_signal",
                trigger={
                    "xau_diag_status": str(diag.get("status", "") or ""),
                    "xau_unmet": list(diag.get("unmet", []) or []),
                    "gating": dict(fallback.get("gating", {}) or {}),
                    "fallback": fallback,
                    "fallback_long": dict(fallback.get("long", {}) or {}),
                    "fallback_short": dict(fallback.get("short", {}) or {}),
                    "fallback_long_summary": {
                        "score": fb_long.get("score"),
                        "confidence": fb_long.get("confidence"),
                        "min_confidence": fb_long.get("min_confidence"),
                        "trigger": fb_long.get("trigger"),
                        "passed": fb_long.get("passed"),
                        "reasons": list((fb_long.get("reasons") or [])[:4]),
                        "warnings": list((fb_long.get("warnings") or [])[:3]),
                    },
                    "fallback_short_summary": {
                        "score": fb_short.get("score"),
                        "confidence": fb_short.get("confidence"),
                        "min_confidence": fb_short.get("min_confidence"),
                        "trigger": fb_short.get("trigger"),
                        "passed": fb_short.get("passed"),
                        "reasons": list((fb_short.get("reasons") or [])[:4]),
                        "warnings": list((fb_short.get("warnings") or [])[:3]),
                    },
                },
            )
            forced = self._maybe_force_xau_result(
                source=source,
                blocked_status=blocked.status,
                blocked_reason=blocked.reason,
                signal=None,
                trigger=blocked.trigger,
            )
            return forced or blocked

        winner_conf_hint = self._apply_xau_winner_logic(signal, apply_confidence=True, apply_exits=False)
        if bool(winner_conf_hint.get("hard_block")):
            blocked = ScalpingScanResult(
                source=source,
                symbol="XAUUSD",
                status="winner_logic_blocked",
                reason=str(winner_conf_hint.get("reason") or "winner_logic_blocked"),
                signal=signal,
                trigger={"winner_logic": winner_conf_hint},
            )
            forced = self._maybe_force_xau_result(
                source=source,
                blocked_status=blocked.status,
                blocked_reason=blocked.reason,
                signal=signal,
                trigger=blocked.trigger,
            )
            return forced or blocked

        base_min_conf = float(getattr(config, "SCALPING_MIN_CONFIDENCE", getattr(config, "MIN_SIGNAL_CONFIDENCE", 70)) or 70)
        min_conf_long_cfg = float(getattr(config, "SCALPING_MIN_CONFIDENCE_XAU_LONG", base_min_conf) or base_min_conf)
        min_conf_short_cfg = float(getattr(config, "SCALPING_MIN_CONFIDENCE_XAU_SHORT", base_min_conf) or base_min_conf)
        if bool(getattr(config, "SCALPING_XAU_BALANCE_SIDE_THRESHOLDS", True)):
            balanced = max(float(min_conf_long_cfg), float(min_conf_short_cfg))
            min_conf_long_cfg = balanced
            min_conf_short_cfg = balanced
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        if direction == "short":
            min_conf = min_conf_short_cfg
        elif direction == "long":
            min_conf = min_conf_long_cfg
        else:
            min_conf = base_min_conf
        if float(getattr(signal, "confidence", 0.0) or 0.0) < min_conf:
            blocked = ScalpingScanResult(
                source=source,
                symbol="XAUUSD",
                status="below_confidence",
                reason=f"confidence<{min_conf:.1f}",
                signal=signal,
                trigger={"winner_logic": winner_conf_hint} if winner_conf_hint else {},
            )
            forced = self._maybe_force_xau_result(
                source=source,
                blocked_status=blocked.status,
                blocked_reason=blocked.reason,
                signal=signal,
                trigger=blocked.trigger,
            )
            return forced or blocked

        m1_df = xauusd_provider.fetch(str(getattr(config, "SCALPING_M1_TRIGGER_TF", "1m")), bars=max(60, int(getattr(config, "SCALPING_M1_TRIGGER_LOOKBACK_BARS", 120) or 120)))
        ok, trigger = self._m1_trigger(m1_df, str(getattr(signal, "direction", "") or ""))
        if winner_conf_hint:
            trigger["winner_logic"] = dict(winner_conf_hint)
        if not ok:
            blocked = ScalpingScanResult(
                source=source,
                symbol="XAUUSD",
                status="m1_rejected",
                reason=str(trigger.get("reason", "m1_rejected")),
                signal=signal,
                trigger=trigger,
            )
            forced = self._maybe_force_xau_result(
                source=source,
                blocked_status=blocked.status,
                blocked_reason=blocked.reason,
                signal=signal,
                trigger=trigger,
            )
            return forced or blocked
        raw_signal = dict(getattr(signal, "raw_scores", {}) or {})
        trigger["countertrend_confirmed"] = bool(raw_signal.get("countertrend_confirmed"))

        if bool(getattr(config, "SCALPING_XAU_REGIME_GUARD_ENABLED", True)):
            allow, guard = self._xau_regime_guard(signal, trigger=trigger)
            trigger["regime_guard"] = guard
            if not allow:
                blocked = ScalpingScanResult(
                    source=source,
                    symbol="XAUUSD",
                    status="regime_blocked",
                    reason=str(guard.get("reason") or "counter_trend_blocked"),
                    signal=signal,
                    trigger=trigger,
                )
                forced = self._maybe_force_xau_result(
                    source=source,
                    blocked_status=blocked.status,
                    blocked_reason=blocked.reason,
                    signal=signal,
                    trigger=trigger,
                )
                return forced or blocked

        self._apply_xau_m1_entry_advantage(signal)
        self._retune_xau_exits(signal)
        winner_exit_tune = self._apply_xau_winner_logic(signal, apply_confidence=False, apply_exits=True)
        if winner_exit_tune:
            trigger["winner_logic"] = dict(winner_exit_tune)
        if bool((winner_exit_tune or {}).get("hard_block")):
            blocked = ScalpingScanResult(
                source=source,
                symbol="XAUUSD",
                status="winner_logic_blocked",
                reason=str((winner_exit_tune or {}).get("reason") or "winner_logic_blocked"),
                signal=signal,
                trigger=trigger,
            )
            forced = self._maybe_force_xau_result(
                source=source,
                blocked_status=blocked.status,
                blocked_reason=blocked.reason,
                signal=signal,
                trigger=trigger,
            )
            return forced or blocked
        self._apply_xau_multi_tf_context(signal, trigger=trigger)
        self._tag_signal(signal, source=source, trigger=trigger)
        
        # --- Gate 3: MRD Live Guard ---
        mrd_guard = self._mrd_guard_check(signal)
        if mrd_guard and mrd_guard.get("suppressed"):
            trigger["mrd"] = mrd_guard
            blocked = ScalpingScanResult(
                source=source,
                symbol="XAUUSD",
                status="mrd_recovery_blocked",
                reason=mrd_guard.get("reason", "mrd_macro_recovery_regime"),
                signal=signal,
                trigger=trigger,
            )
            forced = self._maybe_force_xau_result(
                source=source, blocked_status=blocked.status, blocked_reason=blocked.reason, signal=signal, trigger=trigger
            )
            return forced or blocked

        self._apply_xau_entry_template_m1_bias(signal, trigger=trigger)
        return ScalpingScanResult(source=source, symbol="XAUUSD", status="ready", reason="ok", signal=signal, trigger=trigger)

    def _mrd_guard_check(self, signal: TradeSignal) -> dict:
        """Run MRD dynamically to suppress fake shorts during macro recoveries."""
        direction = str(getattr(signal, "direction", "") or "").lower()
        symbol = str(getattr(signal, "symbol", "") or "").upper()
        
        info = {"suppressed": False, "score": 0.0, "reason": ""}
        
        if symbol != "XAUUSD" or direction != "short":
            return info
            
        try:
            mrd = MRDScanner()
            engine = TickBarEngine("XAUUSD", 100, "tick")
            
            now_utc = datetime.now(timezone.utc)
            from_utc = (now_utc - pd.Timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            to_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            db_path = "data/ctrader_openapi.db"
            with sqlite3.connect(db_path) as conn:
                df_ticks = pd.read_sql_query(
                    "SELECT event_ts, bid, ask FROM ctrader_spot_ticks WHERE symbol = 'XAUUSD' AND event_utc >= ? AND event_utc <= ? ORDER BY event_ts ASC", 
                    conn, params=(from_utc, to_utc))
                
                df_depth = pd.read_sql_query(
                    "SELECT event_ts, side, price, size FROM ctrader_depth_quotes WHERE symbol = 'XAUUSD' AND event_utc >= ? AND event_utc <= ? ORDER BY event_ts ASC", 
                    conn, params=(from_utc, to_utc))
            
            if df_ticks.empty:
                return info
                
            depth_groups = {}
            for _, row in df_depth.iterrows():
                ts = int(float(row['event_ts']) * 1000)
                if ts not in depth_groups:
                    depth_groups[ts] = {'bids': [], 'asks': []}
                if row['side'] == 'bid':
                    depth_groups[ts]['bids'].append({'price': row['price'], 'size': row['size']})
                else:
                    depth_groups[ts]['asks'].append({'price': row['price'], 'size': row['size']})
            
            last_b, last_a = 0.0, 0.0
            for _, row in df_ticks.iterrows():
                b, a = float(row['bid']), float(row['ask'])
                ts_ms = int(float(row['event_ts']) * 1000)
                
                if b > 0: last_b = b
                if a > 0: last_a = a
                if last_b == 0 or last_a == 0: continue
                
                if ts_ms in depth_groups:
                    mrd.on_depth_event(depth_groups[ts_ms]['bids'], depth_groups[ts_ms]['asks'], ts_ms)
                    
                bar = engine.on_quote(last_b, last_a, ts_ms)
                if bar: mrd.on_tick_bar_completed(bar)
                
            suppress, metrics = mrd.should_suppress_short("XAUUSD")
            
            raw = getattr(signal, "raw_scores", {})
            raw["mrd_recovery_score"] = float(metrics["score"])
            raw["mrd_suppressed"] = suppress
            signal.raw_scores = raw
            
            info["score"] = float(metrics["score"])
            
            if suppress:
                info["suppressed"] = True
                info["reason"] = f"mrd_score_{metrics['score']:.3f}_vwap_{metrics['vwap_slope']:.2f}"
                logger.warning(f"[MRD LIVE GUARD] BLOCKED SHORT XAUUSD | Recovery Score={metrics['score']:.3f} | VWAP_slope={metrics['vwap_slope']:.2f}")
            else:
                logger.info(f"[MRD LIVE GUARD] Allowed short | Recovery Score={metrics['score']:.3f}")
                
        except Exception as e:
            logger.error(f"[MRD ERROR] Failed live check: {e}")
            
        return info

    def scan_eth(self, require_enabled: bool = True) -> ScalpingScanResult:
        return self._scan_crypto_symbol(
            canonical_symbol="ETHUSD",
            market_symbol="ETHUSD",
            source="scalp_ethusd",
            require_enabled=require_enabled,
        )

    def scan_btc(self, require_enabled: bool = True) -> ScalpingScanResult:
        return self._scan_crypto_symbol(
            canonical_symbol="BTCUSD",
            market_symbol="BTCUSD",
            source="scalp_btcusd",
            require_enabled=require_enabled,
        )

    @staticmethod
    def _fetch_ctrader_ohlcv(symbol: str, tf: str, bars: int = 200) -> "Optional[pd.DataFrame]":
        """Fetch OHLCV from cTrader OpenAPI for BTCUSD/ETHUSD."""
        import pandas as pd
        try:
            from execution.ctrader_executor import ctrader_executor
        except ImportError:
            logger.debug("[ScalpCrypto] ctrader_executor not available")
            return None
        if not ctrader_executor.enabled:
            return None
        import time as _t
        _tf_minutes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
        mins = _tf_minutes.get(tf, 5)
        to_ms = int(_t.time() * 1000)
        from_ms = to_ms - (bars * mins * 60 * 1000)
        result = ctrader_executor.fetch_trendbars(symbol=symbol, timeframe=tf, from_ms=from_ms, to_ms=to_ms, count=bars)
        if not result.get("ok") or not result.get("bars"):
            return None
        rows = []
        for bar in result["bars"]:
            rows.append({"timestamp": pd.Timestamp(bar["ts_ms"], unit="ms", tz="UTC"), "open": float(bar["open"]), "high": float(bar["high"]), "low": float(bar["low"]), "close": float(bar["close"]), "volume": float(bar.get("volume", 0))})
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df.set_index("timestamp", inplace=True)
        df = df.astype(float)
        return df.tail(bars)

    def _scan_crypto_symbol(
        self,
        *,
        canonical_symbol: str,
        market_symbol: str,
        source: str,
        require_enabled: bool = True,
    ) -> ScalpingScanResult:
        symbol_up = str(canonical_symbol or "").strip().upper()
        market_up = str(market_symbol or "").strip().upper()
        src = str(source or f"scalp_{symbol_up.lower()}")
        if not bool(getattr(config, "SCALPING_ENABLED", False)):
            return ScalpingScanResult(source=src, symbol=symbol_up, status="disabled", reason="scalping_disabled")
        if require_enabled and (not config.scalping_symbol_enabled(symbol_up)):
            return ScalpingScanResult(source=src, symbol=symbol_up, status="disabled", reason="symbol_not_enabled")

        # Fetch data from cTrader OpenAPI (BTCUSD/ETHUSD) — no Binance
        entry_tf = str(getattr(config, "SCALPING_ENTRY_TF", "5m"))
        trend_tf = str(getattr(config, "SCALPING_CRYPTO_TREND_TF", "15m"))
        df_entry = self._fetch_ctrader_ohlcv(symbol_up, entry_tf, bars=200)
        if df_entry is None or len(df_entry) < 50:
            logger.debug("[ScalpCrypto] %s ctrader_data_unavailable tf=%s", symbol_up, entry_tf)
            return ScalpingScanResult(source=src, symbol=symbol_up, status="no_signal", reason="ctrader_data_unavailable")
        df_trend = self._fetch_ctrader_ohlcv(symbol_up, trend_tf, bars=100)
        session_info = session_manager.get_session_info()

        from analysis.signals import SignalGenerator
        # Use a lower floor for crypto so borderline signals survive for
        # multi-TF enrichment.  The profile gate (65/67) is the real filter.
        profile = self._crypto_scalping_profile(symbol_up)
        crypto_floor = max(55, int(profile.get("min_confidence", 65)) - 5)
        _sig = SignalGenerator(min_confidence=crypto_floor)
        signal = _sig.score_signal(df_entry=df_entry, df_trend=df_trend, symbol=symbol_up, timeframe=entry_tf, session_info=session_info)
        if signal is None:
            logger.info("[ScalpCrypto] %s base_scanner_no_signal (floor=%d)", symbol_up, crypto_floor)
            return ScalpingScanResult(source=src, symbol=symbol_up, status="no_signal", reason="base_scanner_no_signal")

        logger.info("[ScalpCrypto] %s score_signal: conf=%.1f dir=%s pattern=%s rr=%.2f",
                     symbol_up, signal.confidence, signal.direction, getattr(signal, "pattern", "?"), getattr(signal, "risk_reward", 0))

        # ── Multi-TF context — fetch 4h + 1h for trend alignment bonus ──
        self._apply_crypto_multi_tf_context(signal, symbol_up)

        # Keep external-facing symbol identity stable (ETHUSD/BTCUSD)
        signal.symbol = symbol_up
        if not str(getattr(signal, "session", "") or "").strip():
            signal.session = ", ".join(session_manager.current_sessions()) or "off_hours"
        raw = dict(getattr(signal, "raw_scores", {}) or {})
        raw["market_symbol"] = market_up
        raw["canonical_symbol"] = symbol_up
        raw["strategy_box"] = f"crypto_{symbol_up.lower()}"
        session_sig = self._normalized_signature(str(getattr(signal, "session", "") or ""))
        raw["scalp_profile_symbol"] = symbol_up
        raw["scalp_profile_weekend"] = bool(profile.get("weekend", False))
        raw["scalp_profile_session"] = session_sig
        raw["scalp_profile_min_confidence"] = float(profile.get("min_confidence", 0.0) or 0.0)
        if profile.get("allowed_sessions"):
            raw["scalp_profile_allowed_sessions"] = sorted(list(profile.get("allowed_sessions") or set()))
        signal.raw_scores = raw
        if profile.get("allowed_sessions") and (not self._session_signature_matches(session_sig, set(profile.get("allowed_sessions") or set()))):
            logger.info("[ScalpCrypto] %s session_filtered: %s not in %s", symbol_up, session_sig, profile.get("allowed_sessions"))
            return ScalpingScanResult(
                source=src,
                symbol=symbol_up,
                status="session_filtered",
                reason=f"session_not_allowed:{session_sig or '-'}",
                signal=signal,
            )

        conf_before_winner = float(getattr(signal, "confidence", 0) or 0)
        winner_info = self._apply_crypto_winner_logic(signal, apply_confidence=True)
        conf_after_winner = float(getattr(signal, "confidence", 0) or 0)
        if winner_info:
            logger.info("[ScalpCrypto] %s winner_logic: regime=%s conf %.1f→%.1f",
                         symbol_up, (winner_info or {}).get("regime", "?"), conf_before_winner, conf_after_winner)
        if bool((winner_info or {}).get("hard_block")):
            return ScalpingScanResult(
                source=src,
                symbol=symbol_up,
                status="winner_logic_blocked",
                reason=str((winner_info or {}).get("reason") or "winner_logic_blocked"),
                signal=signal,
                trigger={"winner_logic": winner_info},
            )

        min_conf = float(profile.get("min_confidence", getattr(config, "SCALPING_MIN_CONFIDENCE", getattr(config, "MIN_SIGNAL_CONFIDENCE", 70))) or 70)
        if float(getattr(signal, "confidence", 0.0) or 0.0) < min_conf:
            logger.info("[ScalpCrypto] %s below_confidence: %.1f < %.1f", symbol_up, signal.confidence, min_conf)
            return ScalpingScanResult(
                source=src,
                symbol=symbol_up,
                status="below_confidence",
                reason=f"confidence<{min_conf:.1f}",
                signal=signal,
                trigger={"winner_logic": winner_info} if winner_info else {},
            )

        # ── M1 trigger with crypto-specific RSI/breakout overrides ──
        m1_df = self._fetch_ctrader_ohlcv(
            symbol_up,
            str(getattr(config, "SCALPING_M1_TRIGGER_TF", "1m")),
            bars=max(60, int(getattr(config, "SCALPING_M1_TRIGGER_LOOKBACK_BARS", 120) or 120)),
        )
        ok, trigger = self._m1_trigger_crypto(m1_df, str(getattr(signal, "direction", "") or ""))
        logger.info("[ScalpCrypto] %s m1_trigger: ok=%s reason=%s rsi=%.1f",
                     symbol_up, ok, trigger.get("reason", "?"), float(trigger.get("rsi14") or 0))
        if winner_info:
            trigger["winner_logic"] = dict(winner_info)
        if not ok:
            return ScalpingScanResult(
                source=src,
                symbol=symbol_up,
                status="m1_rejected",
                reason=str(trigger.get("reason", "m1_rejected")),
                signal=signal,
                trigger=trigger,
            )

        self._tag_signal(signal, source=src, trigger=trigger)
        logger.info("[ScalpCrypto] %s READY: conf=%.1f dir=%s entry=%.2f sl=%.2f tp1=%.2f",
                     symbol_up, signal.confidence, signal.direction, signal.entry, signal.stop_loss, signal.take_profit_1)
        return ScalpingScanResult(source=src, symbol=symbol_up, status="ready", reason="ok", signal=signal, trigger=trigger)

    def detect_xau_sweep_reversal(self) -> dict:
        """
        Detect stop hunt sweep + reversal pattern on XAUUSD M1.

        Pattern (LONG):
          - Bar[-2] (sweep bar): long lower wick (wick_ratio >= min), close near top of bar
          - Bar[-1] (recovery bar): close above body_top of sweep bar

        Pattern (SHORT):
          - Bar[-2]: long upper wick, close near bottom of bar
          - Bar[-1]: close below body_bottom of sweep bar

        Returns dict:
          confirmed: bool
          direction: 'long' | 'short'
          sweep_level: float  (sweep_low for long, sweep_high for short)
          sweep_wick_ratio: float
          current_close: float
          atr: float
          pattern: str
          reason: str (when confirmed=False)
        """
        if not bool(getattr(config, "POST_SL_REVERSAL_ENABLED", False)):
            return {"confirmed": False, "reason": "disabled"}
        tf = str(getattr(config, "SCALPING_M1_TRIGGER_TF", "1m") or "1m")
        bars = 10
        try:
            df = xauusd_provider.fetch(tf, bars=bars)
        except Exception as e:
            return {"confirmed": False, "reason": f"fetch_error:{e}"}
        if df is None or getattr(df, "empty", True) or len(df) < 4:
            return {"confirmed": False, "reason": "no_data"}
        sweep_bar = df.iloc[-2]
        recovery_bar = df.iloc[-1]
        try:
            s_high = float(sweep_bar["high"])
            s_low = float(sweep_bar["low"])
            s_open = float(sweep_bar["open"])
            s_close = float(sweep_bar["close"])
            r_close = float(recovery_bar["close"])
        except Exception as e:
            return {"confirmed": False, "reason": f"parse_error:{e}"}
        bar_range = s_high - s_low
        min_pips = float(getattr(config, "POST_SL_REVERSAL_MIN_SWEEP_PIPS", 3.0) or 3.0)
        if bar_range < min_pips:
            return {"confirmed": False, "reason": f"range_too_small:{bar_range:.2f}<{min_pips}"}
        try:
            highs = df["high"].tail(10).astype(float).values
            lows = df["low"].tail(10).astype(float).values
            atr = float((highs - lows).mean())
        except Exception:
            atr = bar_range
        atr = max(atr, bar_range, 0.5)
        min_wick = float(getattr(config, "POST_SL_REVERSAL_MIN_WICK_RATIO", 0.55) or 0.55)
        body_bottom = min(s_open, s_close)
        body_top = max(s_open, s_close)
        lower_wick = body_bottom - s_low
        upper_wick = s_high - body_top
        lower_wick_ratio = lower_wick / bar_range if bar_range > 0 else 0.0
        upper_wick_ratio = upper_wick / bar_range if bar_range > 0 else 0.0
        # LONG: sweep bar has long lower wick + recovery bar closes above body_top
        if lower_wick_ratio >= min_wick and r_close > body_top:
            return {
                "confirmed": True,
                "direction": "long",
                "sweep_level": s_low,
                "sweep_wick_ratio": round(lower_wick_ratio, 3),
                "current_close": r_close,
                "atr": round(atr, 3),
                "pattern": "sweep_reversal_long",
            }
        # SHORT: sweep bar has long upper wick + recovery bar closes below body_bottom
        if upper_wick_ratio >= min_wick and r_close < body_bottom:
            return {
                "confirmed": True,
                "direction": "short",
                "sweep_level": s_high,
                "sweep_wick_ratio": round(upper_wick_ratio, 3),
                "current_close": r_close,
                "atr": round(atr, 3),
                "pattern": "sweep_reversal_short",
            }
        best = max(lower_wick_ratio, upper_wick_ratio)
        return {"confirmed": False, "reason": f"wick_ratio_low:{best:.2f}<{min_wick}"}


scalping_scanner = ScalpingScanner()
