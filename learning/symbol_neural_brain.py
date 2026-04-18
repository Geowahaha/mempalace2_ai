"""
learning/symbol_neural_brain.py
Per-symbol independent neural brain for Dexter Pro.

Each symbol (XAUUSD, EURUSD, AAPL.NAS, BTCUSD, ETHUSD, …) gets its own
MLP model trained on asset-class-specific behavioral features derived from
institutional trading research.

Fallback chain at prediction time:
  1. Per-symbol model  (e.g. XAUUSD.npz)   — if trained with >= min_samples
  2. Per-family model  (e.g. _family_gold.npz)  — trained on all gold symbols
  3. Global model      (_global → NeuralBrain)  — existing shared model

Features per asset class come from 3 layers:
  A. Shared features (present in every model):
       confidence, risk_reward, rsi, atr_pct, edge, is_long,
       hour_sin, hour_cos, sl_ratio, tp_sl_ratio
  B. Class-specific features (10 per family):
       Gold: real_yield_proxy, dxy_momentum, vix_proxy, asia_range_pct, ...
       FX:   session_overlap, yield_spread_delta, adr_pct_consumed, ...
       Stock: rvol_ratio, vwap_dist_pct, spy_corr_proxy, us_open_min, ...
       BTC:  funding_proxy, oi_delta_proxy, btc_dom_proxy, ...
       ETH:  eth_btc_ratio_delta, gas_proxy, ...
  C. Optional enrichment (default 0.0 if unavailable):
       COT positioning, on-chain flows, IV ratio, etc.

All adjustments are BOUNDED:
  neural_prob < 0.40  → widen SL  (+5%)
  neural_prob > 0.70  → extend TP (+10%..+15% RR)
  size adjustment     ±10% max
"""
from __future__ import annotations

import logging
import math
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from config import config
from learning.symbol_normalizer import canonical_symbol

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value or "").strip()[:19], fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _safe_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else float(default)
    except Exception:
        return float(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(v)))


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.clip(x, -50, 50)
    return 1.0 / (1.0 + np.exp(-x))


# ─────────────────────────────────────────────────────────────────────────────
# Symbol Family Classifier
# ─────────────────────────────────────────────────────────────────────────────

def _classify_symbol(symbol: str) -> str:
    """Map a broker/signal symbol to an asset family."""
    s = canonical_symbol(str(symbol or "").upper().strip()) or str(symbol or "").upper().strip()
    # Gold / Silver
    if any(x in s for x in ("XAU", "GOLD")):
        return "gold"
    if any(x in s for x in ("XAG", "SILVER")):
        return "gold"  # same family — metal precious
    # BTC
    if "BTC" in s:
        return "btc"
    # ETH
    if "ETH" in s and "ETH" == s[:3]:
        return "eth"
    # Crypto (other)
    if any(s.startswith(x) for x in (
        "SOL", "XRP", "DOGE", "ADA", "AVAX", "BNB", "LTC", "DOT",
        "LINK", "TRX", "UNI", "ATOM", "POL", "HBAR", "PEPE", "SHIB",
    )):
        return "crypto"
    # US Indices
    if s in {"US500", "US30", "USTEC", "US2000", "SPX500", "NAS100", "UK100", "JP225", "DE40", "GER40"}:
        return "index"
    # FX — 6 or 7-char pairs with major currencies at end
    if len(s) in (6, 7) and any(s.endswith(x) for x in (
        "USD", "JPY", "EUR", "GBP", "CHF", "AUD", "NZD", "CAD",
        "USDT", "USDC",
    )):
        return "fx"
    # Stocks — contain dot (e.g. AAPL.NAS) or purely alphabetic ≤ 5 chars
    if "." in s:
        return "stock"
    if s.isalpha() and 2 <= len(s) <= 5:
        return "stock"
    return "other"


# ─────────────────────────────────────────────────────────────────────────────
# Feature Registries — One per asset family
# ─────────────────────────────────────────────────────────────────────────────

# Shared base features present in ALL family feature vectors
_SHARED_FEATURES = [
    "confidence",        # signal confidence 0-1
    "risk_reward",       # RR normalized /5
    "rsi",               # RSI /100
    "atr_pct",           # ATR as % of price
    "edge",              # edge score /100
    "is_long",           # 1=long, 0=short
    "hour_sin",          # time-of-day cyclic
    "hour_cos",
    "sl_ratio",          # |entry-SL|/entry
    "tp_sl_ratio",       # proposed RR actual
]

# Family-specific features appended after shared
_FAMILY_FEATURES: dict[str, list[str]] = {
    "gold": [
        "real_yield_proxy",      # inverse: lower = gold bullish  (proxy: DXY 1d delta inverted)
        "dxy_momentum_1d",       # DXY 1-day momentum (negative = gold bullish)
        "vix_proxy",             # VIX-proxy: US500 realized vol 5d
        "asia_range_pct",        # Asia session high-low / ATR  (predictor for London breakout)
        "prior_day_high_dist",   # distance to prior day high as R  (breakout proximity)
        "adr_pct_consumed",      # % of average daily range already used today
        "session_asia",          # 1 if Asia session
        "session_london",        # 1 if London session
        "session_ny",            # 1 if NY session
        "cot_net_proxy",         # placeholder — COT managed-money net (0 if unavailable)
    ],
    "fx": [
        "session_overlap",       # 1 if London/NY overlap (highest liquidity)
        "session_asia",
        "session_london",
        "session_ny",
        "adr_pct_consumed",      # % of ADR used — room left or exhaustion
        "spread_ratio",          # current spread / avg spread (wide = avoid)
        "yield_spread_delta",    # yield spread change proxy (0 if unavailable)
        "dxy_momentum_1d",       # DXY 1d direction proxy
        "vol_regime_ratio",      # ATR14/ATR60 — expanding vs compressing
        "cot_net_proxy",         # COT net positioning proxy (0 if unavailable)
    ],
    "stock": [
        "rvol_ratio",            # relative volume vs 20d avg
        "vwap_dist_pct",         # % distance from VWAP (institutional anchor)
        "spy_corr_proxy",        # US500 vs stock directional alignment
        "us_open_timing_min",    # minutes since 09:30 ET (normalized /390)
        "premarket_gap_pct",     # gap up/down pct at open
        "adr_pct_consumed",      # how much of daily range used
        "earnings_proximity_d",  # days to earnings / 90 normalized (0=unknown)
        "sector_momentum",       # sector ETF proxy momentum (0 if unavailable)
        "market_cap_bucket",     # 0=mega, 0.5=large, 1=mid (liquidity proxy)
        "implied_vol_ratio",     # IV/HV ratio (0 if unavailable)
    ],
    "btc": [
        "funding_proxy",         # funding rate proxy: 0=neutral, +1=crowded long, -1=short
        "oi_delta_proxy",        # open interest change proxy (price momentum confirmation)
        "btc_dom_proxy",         # BTC dominance momentum (0 if unavailable)
        "fear_greed_proxy",      # fear/greed: 0=extreme fear, 1=extreme greed (normalized)
        "session_asia",
        "session_ny",
        "vol_regime_ratio",      # crypto short-term vol ratio
        "exchange_flow_proxy",   # net exchange flow: negative = accumulation (0 if unavailable)
        "us_equity_corr_proxy",  # BTC-equity corr: high = risk-off matters
        "adr_pct_consumed",
    ],
    "eth": [
        "eth_btc_ratio_delta",   # ETH/BTC 1d momentum (ETH-specific strength)
        "gas_proxy",             # gas price normalized (high gas = network busy)
        "defi_tvl_proxy",        # DeFi TVL delta (0 if unavailable)
        "funding_proxy",         # ETH perp funding rate proxy
        "oi_delta_proxy",        # OI change proxy
        "btc_corr_proxy",        # ETH-BTC correlation (when diverging = opportunity)
        "session_asia",
        "session_ny",
        "vol_regime_ratio",
        "adr_pct_consumed",
    ],
    "crypto": [
        "funding_proxy",
        "vol_regime_ratio",
        "session_asia",
        "session_ny",
        "adr_pct_consumed",
        "btc_dom_proxy",
        "fear_greed_proxy",
        "oi_delta_proxy",
        "us_equity_corr_proxy",
        "spread_ratio",
    ],
    "index": [
        "session_overlap",
        "session_ny",
        "vix_proxy",
        "rvol_ratio",
        "adr_pct_consumed",
        "us_open_timing_min",
        "vol_regime_ratio",
        "spy_corr_proxy",
        "dxy_momentum_1d",
        "premarket_gap_pct",
    ],
    "other": [
        "session_overlap",
        "session_ny",
        "adr_pct_consumed",
        "vol_regime_ratio",
        "spread_ratio",
        "rvol_ratio",
        "vwap_dist_pct",
        "dxy_momentum_1d",
        "funding_proxy",
        "fear_greed_proxy",
    ],
}


def _all_features_for(family: str) -> list[str]:
    """Return full feature name list for a family = shared + family-specific."""
    return _SHARED_FEATURES + _FAMILY_FEATURES.get(family, _FAMILY_FEATURES["other"])


# ─────────────────────────────────────────────────────────────────────────────
# Session helpers
# ─────────────────────────────────────────────────────────────────────────────

def _session_flags(utc_hour: int) -> dict[str, float]:
    """Return session membership flags for a UTC hour."""
    asia_start, asia_end = 0, 8        # 00-08 UTC
    london_start, london_end = 7, 16   # 07-16 UTC
    ny_start, ny_end = 13, 22          # 13-22 UTC
    overlap = (ny_start <= utc_hour < london_end)  # 13-16 UTC
    return {
        "session_asia":    1.0 if (asia_start <= utc_hour < asia_end) else 0.0,
        "session_london":  1.0 if (london_start <= utc_hour < london_end) else 0.0,
        "session_ny":      1.0 if (ny_start <= utc_hour < ny_end) else 0.0,
        "session_overlap": 1.0 if overlap else 0.0,
    }


def _us_open_timing_min(utc_hour: int, utc_min: int) -> float:
    """Minutes since 09:30 ET (= 14:30 UTC), normalized to [0,1] over 390-min session."""
    ny_open_utc_hour = 14
    ny_open_utc_min = 30
    total_signal_min = utc_hour * 60 + utc_min
    ny_open_min = ny_open_utc_hour * 60 + ny_open_utc_min
    elapsed = total_signal_min - ny_open_min
    if elapsed < 0:
        return 0.0
    return float(_clamp(elapsed / 390.0, 0.0, 1.0))


# ─────────────────────────────────────────────────────────────────────────────
# Feature computation from signal + context
# ─────────────────────────────────────────────────────────────────────────────

def _compute_shared_features(signal, now_utc: Optional[datetime] = None) -> dict[str, float]:
    """Compute the 10 shared base features from a signal object."""
    now = now_utc or _utc_now()
    hour_frac = (now.hour + now.minute / 60.0) / 24.0
    entry = max(1e-12, _safe_float(getattr(signal, "entry", 0.0), 0.0))
    sl = _safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
    tp2 = _safe_float(getattr(signal, "take_profit_2", 0.0), 0.0)
    raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
    return {
        "confidence":  float(np.clip(_safe_float(getattr(signal, "confidence", 0.0), 0.0) / 100.0, 0.0, 1.5)),
        "risk_reward": float(np.clip(_safe_float(getattr(signal, "risk_reward", 0.0), 0.0) / 5.0, 0.0, 2.0)),
        "rsi":         float(np.clip(_safe_float(getattr(signal, "rsi", 50.0), 50.0) / 100.0, 0.0, 1.0)),
        "atr_pct":     float(np.clip(_safe_float(getattr(signal, "atr", 0.0), 0.0) / entry, 0.0, 0.5)),
        "edge":        float(np.clip(_safe_float(raw_scores.get("edge", 0.0), 0.0) / 100.0, 0.0, 2.0)),
        "is_long":     1.0 if str(getattr(signal, "direction", "")).lower() == "long" else 0.0,
        "hour_sin":    float(math.sin(2 * math.pi * hour_frac)),
        "hour_cos":    float(math.cos(2 * math.pi * hour_frac)),
        "sl_ratio":    float(np.clip(abs(entry - sl) / entry, 0.0, 0.20)) if sl > 0 else 0.0,
        "tp_sl_ratio": float(np.clip(abs(tp2 - entry) / max(abs(entry - sl), 1e-12), 0.0, 5.0)) if (sl > 0 and tp2 > 0) else 0.0,
    }


def _compute_family_features(signal, family: str, now_utc: Optional[datetime] = None) -> dict[str, float]:
    """
    Compute family-specific features.
    Many values are computed from available signal/price data.
    Optional enrichment features (COT, on-chain, IV) default to 0.0.
    """
    now = now_utc or _utc_now()
    sf = _session_flags(now.hour)
    entry = max(1e-12, _safe_float(getattr(signal, "entry", 0.0), 0.0))
    atr = _safe_float(getattr(signal, "atr", 0.0), 0.0)

    # Pull any extra attributes attached to signal (from macro enrichment)
    extra = dict(getattr(signal, "_symbol_brain_extra", {}) or {})

    # ADR pct consumed: if we know a daily range, how much is spent
    day_high = _safe_float(getattr(signal, "day_high", 0.0), 0.0)
    day_low = _safe_float(getattr(signal, "day_low", 0.0), 0.0)
    day_range = (day_high - day_low) if (day_high > day_low) else 0.0
    avg_day_range = atr * 1.5  # rough proxy: ATR14 * 1.5 ≈ ADR
    adr_pct_consumed = float(np.clip(day_range / max(avg_day_range, 1e-12), 0.0, 1.0)) if avg_day_range > 0 else 0.0

    # Prior day high distance (normalized to R)
    prior_day_high = _safe_float(getattr(signal, "prior_day_high", 0.0), 0.0)
    sl = _safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
    r_dist = abs(entry - sl) if sl > 0 else max(atr, 1e-12)
    prior_day_high_dist = float(np.clip(abs(prior_day_high - entry) / max(r_dist, 1e-12), 0.0, 5.0)) if prior_day_high > 0 else 0.0

    # Spread ratio
    spread = _safe_float(getattr(signal, "spread", 0.0), 0.0)
    avg_spread = _safe_float(getattr(signal, "avg_spread", 0.0), 0.0)
    spread_ratio = float(np.clip(spread / max(avg_spread, 1e-12), 0.0, 5.0)) if (spread > 0 and avg_spread > 0) else 1.0

    # Volume / RVOL
    volume = _safe_float(getattr(signal, "volume", 0.0), 0.0)
    avg_volume = _safe_float(getattr(signal, "avg_volume", 0.0), 0.0)
    rvol_ratio = float(np.clip(volume / max(avg_volume, 1e-12), 0.0, 5.0)) if (volume > 0 and avg_volume > 0) else 1.0

    # VWAP distance
    vwap = _safe_float(getattr(signal, "vwap", 0.0), 0.0)
    vwap_dist_pct = float(np.clip(abs(entry - vwap) / entry, 0.0, 0.10)) if vwap > 0 else 0.0

    # Vol regime: ATR14 / ATR60 (or use extra if provided)
    atr_slow = _safe_float(getattr(signal, "atr_slow", 0.0), 0.0)
    vol_regime_ratio = float(np.clip(atr / max(atr_slow, 1e-12), 0.0, 3.0)) if atr_slow > 0 else 1.0

    # US open timing
    us_open_timing = _us_open_timing_min(now.hour, now.minute)

    # Premarket gap
    prev_close = _safe_float(getattr(signal, "prev_close", 0.0), 0.0)
    open_price = _safe_float(getattr(signal, "open_price", 0.0), 0.0)
    premarket_gap_pct = float(np.clip(abs(open_price - prev_close) / max(prev_close, 1e-12), 0.0, 0.15)) if (prev_close > 0 and open_price > 0) else 0.0

    # DXY momentum (1d): attached or 0
    dxy_momentum_1d = float(np.clip(_safe_float(extra.get("dxy_momentum_1d", 0.0), 0.0), -1.0, 1.0))

    # SPY/US500 directional alignment
    spy_corr_proxy = float(np.clip(_safe_float(extra.get("spy_corr_proxy", 0.0), 0.0), -1.0, 1.0))

    # VIX proxy (normalized 0=low=15, 1=high=40)
    vix_level = _safe_float(extra.get("vix_level", 0.0), 0.0)
    vix_proxy = float(np.clip((vix_level - 10.0) / 35.0, 0.0, 1.0)) if vix_level > 0 else 0.0

    # Asia range pct (gold-specific predictor for London breakout)
    asia_range = _safe_float(extra.get("asia_range", 0.0), 0.0)
    asia_range_pct = float(np.clip(asia_range / max(atr, 1e-12), 0.0, 3.0)) if atr > 0 else 0.0

    # Optional enrichment (safe defaults)
    funding_proxy       = float(np.clip(_safe_float(extra.get("funding_proxy", 0.0), 0.0), -1.0, 1.0))
    oi_delta_proxy      = float(np.clip(_safe_float(extra.get("oi_delta_proxy", 0.0), 0.0), -1.0, 1.0))
    btc_dom_proxy       = float(np.clip(_safe_float(extra.get("btc_dom_proxy", 0.0), 0.0), -1.0, 1.0))
    fear_greed_proxy    = float(np.clip(_safe_float(extra.get("fear_greed_proxy", 50.0), 50.0) / 100.0, 0.0, 1.0))
    exchange_flow_proxy = float(np.clip(_safe_float(extra.get("exchange_flow_proxy", 0.0), 0.0), -1.0, 1.0))
    us_equity_corr_proxy= float(np.clip(_safe_float(extra.get("us_equity_corr_proxy", 0.5), 0.5), 0.0, 1.0))
    eth_btc_ratio_delta = float(np.clip(_safe_float(extra.get("eth_btc_ratio_delta", 0.0), 0.0), -1.0, 1.0))
    gas_proxy           = float(np.clip(_safe_float(extra.get("gas_proxy", 0.5), 0.5), 0.0, 1.0))
    defi_tvl_proxy      = float(np.clip(_safe_float(extra.get("defi_tvl_proxy", 0.0), 0.0), -1.0, 1.0))
    btc_corr_proxy      = float(np.clip(_safe_float(extra.get("btc_corr_proxy", 0.7), 0.7), 0.0, 1.0))
    yield_spread_delta  = float(np.clip(_safe_float(extra.get("yield_spread_delta", 0.0), 0.0), -1.0, 1.0))
    cot_net_proxy       = float(np.clip(_safe_float(extra.get("cot_net_proxy", 0.0), 0.0), -1.0, 1.0))
    implied_vol_ratio   = float(np.clip(_safe_float(extra.get("implied_vol_ratio", 1.0), 1.0), 0.0, 3.0))
    earnings_prox       = float(np.clip(_safe_float(extra.get("earnings_proximity_d", 90.0), 90.0) / 90.0, 0.0, 1.0))
    real_yield_proxy    = float(np.clip(_safe_float(extra.get("real_yield_proxy", 0.0), 0.0), -1.0, 1.0))
    sector_momentum     = float(np.clip(_safe_float(extra.get("sector_momentum", 0.0), 0.0), -1.0, 1.0))
    market_cap_bucket   = float(np.clip(_safe_float(extra.get("market_cap_bucket", 0.5), 0.5), 0.0, 1.0))

    all_fd = {
        # Session
        **sf,
        # Vol/momentum
        "vol_regime_ratio":       vol_regime_ratio,
        "adr_pct_consumed":       adr_pct_consumed,
        "spread_ratio":           spread_ratio,
        "rvol_ratio":             _clamp(rvol_ratio / 5.0, 0.0, 1.0),
        "vwap_dist_pct":          vwap_dist_pct,
        "us_open_timing_min":    us_open_timing,
        "premarket_gap_pct":      premarket_gap_pct,
        "prior_day_high_dist":    _clamp(prior_day_high_dist / 5.0, 0.0, 1.0),
        "dxy_momentum_1d":        dxy_momentum_1d,
        "spy_corr_proxy":         spy_corr_proxy,
        "vix_proxy":              vix_proxy,
        "asia_range_pct":         _clamp(asia_range_pct / 3.0, 0.0, 1.0),
        # Gold
        "real_yield_proxy":       real_yield_proxy,
        "cot_net_proxy":          cot_net_proxy,
        # FX
        "yield_spread_delta":     yield_spread_delta,
        # Stock
        "sector_momentum":        sector_momentum,
        "market_cap_bucket":      market_cap_bucket,
        "implied_vol_ratio":      _clamp(implied_vol_ratio / 3.0, 0.0, 1.0),
        "earnings_proximity_d":   earnings_prox,
        # Crypto
        "funding_proxy":          funding_proxy,
        "oi_delta_proxy":         oi_delta_proxy,
        "btc_dom_proxy":          btc_dom_proxy,
        "fear_greed_proxy":       fear_greed_proxy,
        "exchange_flow_proxy":    exchange_flow_proxy,
        "us_equity_corr_proxy":   us_equity_corr_proxy,
        # ETH
        "eth_btc_ratio_delta":    eth_btc_ratio_delta,
        "gas_proxy":              gas_proxy,
        "defi_tvl_proxy":         defi_tvl_proxy,
        "btc_corr_proxy":         btc_corr_proxy,
    }
    # Return only the features registered for this family
    keys = _FAMILY_FEATURES.get(family, _FAMILY_FEATURES["other"])
    return {k: all_fd.get(k, 0.0) for k in keys}


def _build_feature_vector(signal, family: str, feature_names: list[str],
                           now_utc: Optional[datetime] = None) -> np.ndarray:
    shared = _compute_shared_features(signal, now_utc)
    specific = _compute_family_features(signal, family, now_utc)
    merged = {**shared, **specific}
    return np.array([float(merged.get(k, 0.0)) for k in feature_names], dtype=np.float64)


# ─────────────────────────────────────────────────────────────────────────────
# Training Result
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SymbolTrainResult:
    ok: bool
    symbol_key: str        # "XAUUSD" or "_family_gold"
    status: str
    message: str
    samples: int = 0
    train_accuracy: float = 0.0
    val_accuracy: float = 0.0
    win_rate: float = 0.0
    feature_set: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Per-Symbol Model Store
# ─────────────────────────────────────────────────────────────────────────────

class SymbolNeuralBrain:
    """
    Manages one neural MLP per symbol + one per family + global fallback.
    All models are stored as .npz files in data/neural_models/.
    """

    # Minimum labeled samples required to train/use a model
    MIN_SAMPLES: dict[str, int] = {
        "gold":   6,
        "fx":     8,
        "stock":  5,
        "btc":    6,
        "eth":    6,
        "crypto": 6,
        "index":  6,
        "other":  8,
    }

    def __init__(self, db_path: Optional[str] = None, model_dir: Optional[str] = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(db_path or (data_dir / "signal_learning.db"))
        self.model_dir = Path(model_dir or (data_dir / "neural_models"))
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._model_cache: dict[str, Optional[dict]] = {}

    # ──────────────────────────────────────────────────────────────────────
    # Model file paths
    # ──────────────────────────────────────────────────────────────────────

    def _model_path(self, key: str) -> Path:
        """key = symbol (e.g. 'XAUUSD') or '_family_gold'"""
        safe = str(key).replace("/", "_").replace("\\", "_")
        return self.model_dir / f"{safe}.npz"

    def _feature_names_for(self, family: str) -> list[str]:
        return _all_features_for(family)

    # ──────────────────────────────────────────────────────────────────────
    # DB access (read-only from signal_events)
    # ──────────────────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path), timeout=15)

    def _load_rows_for_symbol(self, symbol_key: str, days: int = 120) -> list[sqlite3.Row]:
        """
        Load labeled signal_events rows for a specific symbol.
        Uses COALESCE(closed_at, created_at) as the time anchor so that
        Telegram-signal rows resolved via market-path (closed_at may be NULL)
        are also included.
        """
        since = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        sym_upper = canonical_symbol(str(symbol_key).upper()) or str(symbol_key).upper()
        with self._lock:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT *
                      FROM signal_events
                     WHERE resolved = 1
                       AND outcome IN (0, 1)
                        AND COALESCE(closed_at, created_at) >= ?
                     ORDER BY COALESCE(closed_at, created_at) ASC
                    """,
                    (since,),
                ).fetchall()
        out: list[sqlite3.Row] = []
        for r in rows:
            sig = canonical_symbol(str(r["signal_symbol"] or "").upper())
            bro = canonical_symbol(str(r["broker_symbol"] or "").upper())
            if sig == sym_upper or bro == sym_upper:
                out.append(r)
        return out

    def _load_rows_for_family(self, family: str, days: int = 120) -> list[sqlite3.Row]:
        """
        Load labeled rows for all symbols in a family.
        Uses COALESCE(closed_at, created_at) so market-path resolved rows are included.
        """
        since = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        with self._lock:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT *
                      FROM signal_events
                     WHERE resolved = 1
                       AND outcome IN (0, 1)
                       AND COALESCE(closed_at, created_at) >= ?
                     ORDER BY COALESCE(closed_at, created_at) ASC
                    """,
                    (since,),
                ).fetchall()
        # Filter by family using signal_symbol (primary) or broker_symbol (fallback)
        return [
            r for r in rows
            if _classify_symbol(str(
                (r["signal_symbol"] or "") or (r["broker_symbol"] or "")
            )) == family
        ]

    # ──────────────────────────────────────────────────────────────────────
    # Feature matrix builder (with recency weighting)
    # ──────────────────────────────────────────────────────────────────────

    def _rows_to_xy(
        self,
        rows: list[sqlite3.Row],
        family: str,
        feature_names: list[str],
        recency_days: int = 30,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Build (X, y, sample_weights) from signal_events rows."""
        if not rows:
            return np.zeros((0, len(feature_names))), np.zeros(0), np.zeros(0)

        now_utc = _utc_now()
        recency_cutoff = now_utc - timedelta(days=max(1, int(recency_days)))

        xs, ys, ws = [], [], []
        for r in rows:
            created_at = _parse_iso(str(r["created_at"] or ""))
            # Build a lightweight signal proxy from the row
            sig_proxy = _RowSignalProxy(r)
            now_for_row = created_at or now_utc
            vec = _build_feature_vector(sig_proxy, family, feature_names, now_utc=now_for_row)
            xs.append(vec)
            ys.append(float(r["outcome"]))
            # Recency weight: recent 30d = 2x, older = 1x
            w = 2.0 if (created_at and created_at >= recency_cutoff) else 1.0
            ws.append(w)

        X = np.vstack(xs)
        y = np.array(ys, dtype=np.float64)
        sw = np.array(ws, dtype=np.float64)
        return X, y, sw

    # ──────────────────────────────────────────────────────────────────────
    # Core MLP training
    # ──────────────────────────────────────────────────────────────────────

    def _train_mlp(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weights: np.ndarray,
        hidden: int = 8,
        epochs: int = 150,
        lr: float = 0.02,
        l2: float = 1e-4,
        seed: int = 42,
    ) -> dict:
        """Train a 2-layer MLP with recency-weighted binary cross-entropy."""
        n, d = X.shape
        rng = np.random.default_rng(seed)
        val_size = max(1, int(round(0.2 * n)))
        split = max(1, n - val_size)
        if split >= n:
            split = n - 1
        X_tr, y_tr, sw_tr = X[:split], y[:split], sample_weights[:split]
        X_val, y_val = X[split:], y[split:]
        if len(X_val) == 0:
            X_val, y_val = X_tr, y_tr

        mu = X_tr.mean(axis=0)
        sigma = X_tr.std(axis=0)
        sigma[sigma < 1e-8] = 1.0
        X_tr = (X_tr - mu) / sigma
        X_val = (X_val - mu) / sigma

        w1 = rng.normal(0.0, 0.15, size=(d, hidden))
        b1 = np.zeros((1, hidden))
        w2 = rng.normal(0.0, 0.15, size=(hidden, 1))
        b2 = np.zeros((1, 1))

        y_col = y_tr.reshape(-1, 1)
        w_col = (sw_tr / max(sw_tr.mean(), 1e-8)).reshape(-1, 1)

        for _ in range(epochs):
            z1 = X_tr @ w1 + b1
            a1 = np.maximum(0.0, z1)
            z2 = a1 @ w2 + b2
            y_hat = _sigmoid(z2)
            dz2 = (y_hat - y_col) * w_col / max(1, len(X_tr))
            dw2 = (a1.T @ dz2) + l2 * w2
            db2 = dz2.sum(axis=0, keepdims=True)
            da1 = dz2 @ w2.T
            dz1 = da1 * (z1 > 0).astype(np.float64)
            dw1 = (X_tr.T @ dz1) + l2 * w1
            db1 = dz1.sum(axis=0, keepdims=True)
            w1 -= lr * dw1; b1 -= lr * db1
            w2 -= lr * dw2; b2 -= lr * db2

        def _predict(Xn):
            return _sigmoid(np.maximum(0.0, Xn @ w1 + b1) @ w2 + b2).ravel()

        tr_acc = float((((_predict(X_tr) >= 0.5).astype(float)) == y_tr).mean())
        val_acc = float((((_predict(X_val) >= 0.5).astype(float)) == y_val).mean())

        return {
            "w1": w1, "b1": b1, "w2": w2, "b2": b2,
            "mu": mu, "sigma": sigma,
            "n": n, "train_acc": tr_acc, "val_acc": val_acc,
            "win_rate": float(y.mean()),
        }

    def _save_model(self, key: str, family: str, feature_names: list[str], m: dict) -> None:
        path = self._model_path(key)
        np.savez(
            path,
            w1=m["w1"], b1=m["b1"], w2=m["w2"], b2=m["b2"],
            mu=m["mu"], sigma=m["sigma"],
            feature_names=np.array(feature_names, dtype=object),
            family=np.array([family], dtype=object),
            trained_at=np.array([_iso(_utc_now())], dtype=object),
            samples=np.array([m["n"]], dtype=np.int64),
            train_accuracy=np.array([m["train_acc"]], dtype=np.float64),
            val_accuracy=np.array([m["val_acc"]], dtype=np.float64),
            win_rate=np.array([m["win_rate"]], dtype=np.float64),
        )
        # Invalidate cache
        with self._lock:
            self._model_cache.pop(key, None)
        logger.info(
            "[SymbolBrain] saved model key=%s family=%s samples=%d trAcc=%.3f valAcc=%.3f",
            key, family, m["n"], m["train_acc"], m["val_acc"],
        )

    def _load_model(self, key: str) -> Optional[dict]:
        with self._lock:
            if key in self._model_cache:
                return self._model_cache[key]
        path = self._model_path(key)
        if not path.exists():
            with self._lock:
                self._model_cache[key] = None
            return None
        try:
            data = np.load(path, allow_pickle=True)
            model = {
                "w1": data["w1"], "b1": data["b1"],
                "w2": data["w2"], "b2": data["b2"],
                "mu": data["mu"], "sigma": data["sigma"],
                "feature_names": list(data["feature_names"]) if "feature_names" in data else [],
                "family": str(data["family"][0]) if "family" in data else "other",
                "trained_at": str(data["trained_at"][0]) if "trained_at" in data else "",
                "samples": int(data["samples"][0]) if "samples" in data else 0,
                "train_accuracy": float(data["train_accuracy"][0]) if "train_accuracy" in data else 0.0,
                "val_accuracy": float(data["val_accuracy"][0]) if "val_accuracy" in data else 0.0,
                "win_rate": float(data["win_rate"][0]) if "win_rate" in data else 0.0,
            }
            with self._lock:
                self._model_cache[key] = model
            return model
        except Exception as e:
            logger.debug("[SymbolBrain] failed to load model key=%s: %s", key, e)
            with self._lock:
                self._model_cache[key] = None
            return None

    # ──────────────────────────────────────────────────────────────────────
    # Public training API
    # ──────────────────────────────────────────────────────────────────────

    def train_symbol(self, symbol: str, days: int = 120) -> SymbolTrainResult:
        """Train a model specifically for `symbol`. Falls through to family if too few samples."""
        symbol_key = canonical_symbol(str(symbol or "").upper().strip()) or str(symbol or "").upper().strip()
        if not getattr(config, "NEURAL_BRAIN_ENABLED", True):
            return SymbolTrainResult(False, symbol_key, "disabled", "neural brain disabled")
        family = _classify_symbol(symbol_key)
        feature_names = self._feature_names_for(family)
        rows = self._load_rows_for_symbol(symbol_key, days=days)
        min_s = self.MIN_SAMPLES.get(family, 8)
        if len(rows) < min_s:
            return SymbolTrainResult(
                False, symbol_key, "not_enough_data",
                f"{symbol_key}: need >={min_s} samples, have {len(rows)}",
                samples=len(rows),
                feature_set=family,
            )
        X, y, sw = self._rows_to_xy(rows, family, feature_names)
        try:
            m = self._train_mlp(X, y, sw)
            self._save_model(symbol_key, family, feature_names, m)
            return SymbolTrainResult(
                True, symbol_key, "ok", "training complete",
                samples=m["n"], train_accuracy=m["train_acc"],
                val_accuracy=m["val_acc"], win_rate=m["win_rate"],
                feature_set=family,
            )
        except Exception as e:
            logger.warning("[SymbolBrain] train_symbol %s failed: %s", symbol_key, e, exc_info=True)
            return SymbolTrainResult(False, symbol_key, "error", str(e), feature_set=family)

    def train_family(self, family: str, days: int = 120) -> SymbolTrainResult:
        """Train a grouped model for all symbols of a given family."""
        if not getattr(config, "NEURAL_BRAIN_ENABLED", True):
            return SymbolTrainResult(False, f"_family_{family}", "disabled", "neural brain disabled")
        feature_names = self._feature_names_for(family)
        rows = self._load_rows_for_family(family, days=days)
        min_s = self.MIN_SAMPLES.get(family, 8)
        if len(rows) < min_s:
            return SymbolTrainResult(
                False, f"_family_{family}", "not_enough_data",
                f"family {family}: need >={min_s} samples, have {len(rows)}",
                samples=len(rows), feature_set=family,
            )
        X, y, sw = self._rows_to_xy(rows, family, feature_names)
        try:
            m = self._train_mlp(X, y, sw)
            self._save_model(f"_family_{family}", family, feature_names, m)
            return SymbolTrainResult(
                True, f"_family_{family}", "ok", "training complete",
                samples=m["n"], train_accuracy=m["train_acc"],
                val_accuracy=m["val_acc"], win_rate=m["win_rate"],
                feature_set=family,
            )
        except Exception as e:
            logger.warning("[SymbolBrain] train_family %s failed: %s", family, e, exc_info=True)
            return SymbolTrainResult(False, f"_family_{family}", "error", str(e), feature_set=family)

    def train_all(self, days: int = 120) -> dict[str, SymbolTrainResult]:
        """
        Train per-symbol models for all symbols that have labeled data,
        then train per-family models.
        Returns dict of key → SymbolTrainResult.
        """
        if not getattr(config, "NEURAL_BRAIN_ENABLED", True):
            return {}

        results: dict[str, SymbolTrainResult] = {}

        # Discover all symbols in the DB
        symbols = self._discover_symbols(days=days)
        for sym in symbols:
            r = self.train_symbol(sym, days=days)
            results[sym] = r
            if r.ok:
                logger.info("[SymbolBrain] trained symbol=%s samples=%d val_acc=%.3f family=%s",
                            sym, r.samples, r.val_accuracy, r.feature_set)
            else:
                logger.debug("[SymbolBrain] skip symbol=%s status=%s msg=%s",
                             sym, r.status, r.message)

        # Train per-family models
        all_families = set(results[s].feature_set for s in results) | {
            "gold", "fx", "stock", "btc", "eth", "crypto", "index"
        }
        for fam in sorted(all_families):
            r = self.train_family(fam, days=days)
            results[f"_family_{fam}"] = r
            if r.ok:
                logger.info("[SymbolBrain] trained family=%s samples=%d val_acc=%.3f",
                            fam, r.samples, r.val_accuracy)

        return results

    def _discover_symbols(self, days: int = 120) -> list[str]:
        """
        Return distinct symbols with resolved labeled data in the past N days.
        Uses signal_symbol as primary (set for all Telegram signals),
        broker_symbol as fallback (set for MT5 live trades).
        Filters out noise entries: 'B', '' and TESTUSD/ADAUSD from old tests.
        """
        since = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        _EXCLUDED = {"B", "TESTUSD", "ADAUSD", "WIFUSD", ""}
        try:
            with self._lock:
                with self._connect() as conn:
                    rows = conn.execute(
                        """
                        SELECT DISTINCT
                               UPPER(COALESCE(
                                   NULLIF(TRIM(signal_symbol),''),
                                   NULLIF(TRIM(broker_symbol),'')
                               )) AS sym
                          FROM signal_events
                         WHERE resolved = 1
                           AND outcome IN (0, 1)
                           AND COALESCE(closed_at, created_at) >= ?
                           AND COALESCE(
                                   NULLIF(TRIM(signal_symbol),''),
                                   NULLIF(TRIM(broker_symbol),'')
                               ) IS NOT NULL
                        """,
                        (since,),
                    ).fetchall()
            dedup: set[str] = set()
            out: list[str] = []
            for r in rows:
                raw = str(r[0] or "")
                if not raw:
                    continue
                canon = canonical_symbol(raw) or raw
                if canon in _EXCLUDED or canon in dedup:
                    continue
                dedup.add(canon)
                out.append(canon)
            return out
        except Exception as e:
            logger.debug("[SymbolBrain] discover_symbols failed: %s", e)
            return []

    # ──────────────────────────────────────────────────────────────────────
    # Prediction: fallback chain symbol → family → None (→ caller uses global)
    # ──────────────────────────────────────────────────────────────────────

    def _quality_status_for_model(self, model: Optional[dict], family: str, source_label: str) -> dict:
        min_samples = max(
            int(self.MIN_SAMPLES.get(family, 8)),
            int(getattr(config, "NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES", 10) or 10),
        )
        min_val_acc = float(getattr(config, "NEURAL_BRAIN_FILTER_MIN_VAL_ACC", 0.52) or 0.52)
        max_age_h = max(0, int(getattr(config, "NEURAL_BRAIN_FILTER_MAX_MODEL_AGE_HOURS", 720) or 720))

        out = {
            "source": source_label,
            "family": family,
            "ready": False,
            "reason": "model_unavailable",
            "samples": 0,
            "required_samples": int(min_samples),
            "val_accuracy": 0.0,
            "required_val_accuracy": float(min_val_acc),
            "trained_at": "",
            "age_hours": None,
            "max_age_hours": int(max_age_h),
        }
        if not model:
            return out

        samples = int(model.get("samples", 0) or 0)
        val_acc = float(model.get("val_accuracy", 0.0) or 0.0)
        trained_at = str(model.get("trained_at", "") or "")
        age_h = None
        trained_dt = _parse_iso(trained_at)
        if trained_dt is not None:
            age_h = max(0.0, (_utc_now() - trained_dt).total_seconds() / 3600.0)

        out.update(
            {
                "samples": samples,
                "val_accuracy": val_acc,
                "trained_at": trained_at,
                "age_hours": age_h,
            }
        )
        if samples < min_samples:
            out["reason"] = "insufficient_samples"
            return out
        if val_acc < min_val_acc:
            out["reason"] = "low_val_accuracy"
            return out
        if age_h is not None and max_age_h > 0 and age_h > max_age_h:
            out["reason"] = "stale_model"
            return out
        out["ready"] = True
        out["reason"] = "ready"
        return out

    def predict_for_signal_with_quality(
        self,
        signal,
        source: str,
        enforce_quality: bool = True,
    ) -> tuple[Optional[float], str, dict]:
        """
        Returns (probability, model_source, quality_status).
        model_source: symbol:<SYM> | family:<FAM> | none.
        """
        if not getattr(config, "NEURAL_BRAIN_ENABLED", True):
            return None, "disabled", {"ready": False, "reason": "neural_brain_disabled"}

        raw_sym = str(getattr(signal, "symbol", "") or getattr(signal, "broker_symbol", "") or "").upper()
        sym = canonical_symbol(raw_sym) or raw_sym
        family = _classify_symbol(sym)
        blocked_status: Optional[dict] = None

        # 1) Symbol model
        sym_model = self._load_model(sym)
        sym_status = self._quality_status_for_model(sym_model, family, source_label=f"symbol:{sym}")
        if sym_model:
            p = self._run_inference(signal, sym_model, family)
            if p is not None and (not enforce_quality or bool(sym_status.get("ready"))):
                return p, f"symbol:{sym}", sym_status
            if p is not None and blocked_status is None:
                blocked_status = sym_status

        # 2) Family model chain (btc/eth can fallback to generic crypto family).
        family_chain = [family]
        if family in {"btc", "eth"} and "crypto" not in family_chain:
            family_chain.append("crypto")
        for fam in family_chain:
            fam_model = self._load_model(f"_family_{fam}")
            fam_status = self._quality_status_for_model(fam_model, fam, source_label=f"family:{fam}")
            if not fam_model:
                continue
            p = self._run_inference(signal, fam_model, fam)
            if p is not None and (not enforce_quality or bool(fam_status.get("ready"))):
                return p, f"family:{fam}", fam_status
            if p is not None and blocked_status is None:
                blocked_status = fam_status

        if blocked_status is not None:
            return None, "none", blocked_status
        return None, "none", {"ready": False, "reason": "no_model"}

    def predict_for_signal(self, signal, source: str) -> tuple[Optional[float], str]:
        """
        Returns (probability: 0-1, model_source: str).
        model_source indicates which level of the fallback chain was used:
          "symbol:<SYM>", "family:<FAM>", or "none"
        """
        p, src, _status = self.predict_for_signal_with_quality(
            signal,
            source=source,
            enforce_quality=False,
        )
        return p, src

    def _run_inference(self, signal, model: dict, family: str) -> Optional[float]:
        """Run forward pass of a saved MLP model against the signal's feature vector."""
        try:
            fnames = list(model.get("feature_names") or _all_features_for(family))
            vec = _build_feature_vector(signal, family, fnames)
            mu = model["mu"]
            sigma = model["sigma"]
            d_expected = mu.shape[0]
            x = vec.reshape(1, -1)
            if x.shape[1] < d_expected:
                x = np.pad(x, ((0, 0), (0, d_expected - x.shape[1])))
            elif x.shape[1] > d_expected:
                x = x[:, :d_expected]
            x = (x - mu) / sigma
            z1 = x @ model["w1"] + model["b1"]
            a1 = np.maximum(0.0, z1)
            z2 = a1 @ model["w2"] + model["b2"]
            return float(_sigmoid(z2).ravel()[0])
        except Exception as e:
            logger.debug("[SymbolBrain] inference error: %s", e)
            return None

    # ──────────────────────────────────────────────────────────────────────
    # Model status / diagnostics
    # ──────────────────────────────────────────────────────────────────────

    def model_status_all(self) -> list[dict]:
        """Return status for all saved models in model_dir."""
        out = []
        for npz in sorted(self.model_dir.glob("*.npz")):
            key = npz.stem
            m = self._load_model(key)
            if m:
                out.append({
                    "symbol_key": key,
                    "key": key,
                    "family": m.get("family", "?"),
                    "trained_at": m.get("trained_at", ""),
                    "samples": m.get("samples", 0),
                    "train_accuracy": round(m.get("train_accuracy", 0.0), 4),
                    "val_accuracy": round(m.get("val_accuracy", 0.0), 4),
                    "win_rate": round(m.get("win_rate", 0.0), 4),
                    "feature_count": len(m.get("feature_names", [])),
                })
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Signal Row Proxy — lightweight signal object from a sqlite3.Row
# ─────────────────────────────────────────────────────────────────────────────

class _RowSignalProxy:
    """Wraps a sqlite3.Row to expose signal-like attribute API for feature computation."""
    __slots__ = (
        "symbol", "broker_symbol", "direction", "confidence", "risk_reward",
        "rsi", "atr", "entry", "stop_loss", "take_profit_1", "take_profit_2",
        "take_profit_3", "pattern", "raw_scores", "_symbol_brain_extra",
        "day_high", "day_low", "prior_day_high", "open_price", "prev_close",
        "volume", "avg_volume", "vwap", "spread", "avg_spread", "atr_slow",
    )

    def __init__(self, row: sqlite3.Row):
        def sf(key, default=0.0):
            try:
                return _safe_float(row[key], default)
            except Exception:
                return float(default)

        def ss(key, default=""):
            try:
                v = row[key]
                return str(v) if v is not None else str(default)
            except Exception:
                return str(default)

        self.symbol = ss("broker_symbol") or ss("signal_symbol")
        self.broker_symbol = ss("broker_symbol")
        self.direction = ss("direction")
        self.confidence = sf("confidence")
        self.risk_reward = sf("risk_reward")
        self.rsi = sf("rsi", 50.0)
        self.atr = sf("atr")
        self.entry = sf("entry")
        self.stop_loss = sf("stop_loss")
        self.take_profit_1 = sf("take_profit_1")
        self.take_profit_2 = sf("take_profit_2")
        self.take_profit_3 = sf("take_profit_3")
        self.pattern = ss("pattern")
        # raw_scores from extra_json
        try:
            import json
            extra = json.loads(str(row["extra_json"] or "{}") or "{}")
        except Exception:
            extra = {}
        self.raw_scores = extra.get("raw_scores", {})
        self._symbol_brain_extra = extra.get("symbol_brain_extra", {})
        # Fields that may not exist in older DB rows
        self.day_high = sf("day_high") if "day_high" in row.keys() else 0.0  # type: ignore[attr-defined]
        self.day_low = sf("day_low") if "day_low" in row.keys() else 0.0  # type: ignore[attr-defined]
        self.prior_day_high = 0.0
        self.open_price = 0.0
        self.prev_close = 0.0
        self.volume = 0.0
        self.avg_volume = 0.0
        self.vwap = 0.0
        self.spread = 0.0
        self.avg_spread = 0.0
        self.atr_slow = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

symbol_neural_brain = SymbolNeuralBrain()
