"""
analysis/entry_sharpness.py

Deep data analytics for entry quality scoring.
Computes 8 microstructure features from raw tick/depth data and
a composite Entry Sharpness Score (0-100) that distinguishes
sharp (winning) entries from knife (losing) entries.

All functions are pure — no I/O, no DB access, no side effects.
"""
from __future__ import annotations

import statistics
from typing import Optional


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


# ---------------------------------------------------------------------------
# 1. Tick Acceleration — rate of change of momentum across capture window
# ---------------------------------------------------------------------------

def tick_acceleration(mids: list[float]) -> float:
    """Split mids into thirds; compute delta_proxy per third; return late - early.

    Positive = momentum building.  Negative = momentum decaying (knife signal).
    Returns 0.0 when fewer than 9 data points.
    """
    if len(mids) < 9:
        return 0.0
    third = len(mids) // 3
    early = mids[:third]
    late = mids[-third:]

    def _delta_proxy(segment: list[float]) -> float:
        up = 0.0
        down = 0.0
        for i in range(1, len(segment)):
            d = segment[i] - segment[i - 1]
            if d > 0:
                up += d
            else:
                down += abs(d)
        total = up + down
        return (up - down) / total if total > 0 else 0.0

    return _delta_proxy(late) - _delta_proxy(early)


# ---------------------------------------------------------------------------
# 2. Adverse Flow Streak — max consecutive adverse-direction tick moves
# ---------------------------------------------------------------------------

def adverse_flow_streak(move_deltas: list[float], direction: str = "") -> float:
    """Longest consecutive adverse tick run, normalized by total ticks.

    For longs: adverse = negative delta.  For shorts: adverse = positive delta.
    If direction unknown, returns worst-case (max of both sides).
    Returns 0.0 when fewer than 3 deltas.
    """
    if len(move_deltas) < 3:
        return 0.0

    def _max_streak(deltas: list[float], adverse_sign: int) -> int:
        best = 0
        cur = 0
        for d in deltas:
            if (adverse_sign > 0 and d > 0) or (adverse_sign < 0 and d < 0):
                cur += 1
                best = max(best, cur)
            else:
                cur = 0
        return best

    n = len(move_deltas)
    side = direction.strip().lower()
    if side == "long":
        return _max_streak(move_deltas, -1) / n
    elif side == "short":
        return _max_streak(move_deltas, 1) / n
    else:
        long_streak = _max_streak(move_deltas, -1) / n
        short_streak = _max_streak(move_deltas, 1) / n
        return max(long_streak, short_streak)


# ---------------------------------------------------------------------------
# 3. Depth Absorption Rate — how fast depth refills after consumption
# ---------------------------------------------------------------------------

def depth_absorption_rate(depth_points: list[tuple]) -> float:
    """For consecutive depth snapshots, measure refill vs consumption.

    depth_points: list of (ts, bid_vol, ask_vol) tuples.
    Returns ratio: sum(refills) / sum(consumptions).
    > 1.0 = net depth addition (institutional support).
    < 0.5 = depth draining (knife territory).
    Returns 1.0 (neutral) when fewer than 2 points.
    Clamped to [0.0, 3.0].
    """
    if len(depth_points) < 2:
        return 1.0
    total_consumption = 0.0
    total_refill = 0.0
    for i in range(1, len(depth_points)):
        prev_bid = float(depth_points[i - 1][1])
        prev_ask = float(depth_points[i - 1][2])
        cur_bid = float(depth_points[i][1])
        cur_ask = float(depth_points[i][2])
        consumption = max(0.0, prev_bid - cur_bid) + max(0.0, prev_ask - cur_ask)
        refill = max(0.0, cur_bid - prev_bid) + max(0.0, cur_ask - prev_ask)
        total_consumption += consumption
        total_refill += refill
    if total_consumption < 1e-9:
        return _clamp(total_refill + 1.0, 0.0, 3.0)
    return _clamp(total_refill / total_consumption, 0.0, 3.0)


# ---------------------------------------------------------------------------
# 4. Micro-Volatility — stdev of mid-price percentage changes
# ---------------------------------------------------------------------------

def micro_volatility(mids: list[float]) -> float:
    """Standard deviation of tick-to-tick percentage changes.

    High values = choppy price action (knife territory).
    Returns 0.0 when fewer than 4 data points.
    """
    if len(mids) < 4:
        return 0.0
    pct_changes = []
    for i in range(1, len(mids)):
        if mids[i - 1] > 0:
            pct_changes.append(((mids[i] - mids[i - 1]) / mids[i - 1]) * 100.0)
    if len(pct_changes) < 3:
        return 0.0
    return statistics.stdev(pct_changes)


# ---------------------------------------------------------------------------
# 5. Spread Trajectory — is spread narrowing or widening?
# ---------------------------------------------------------------------------

def spread_trajectory(spread_pcts: list[float]) -> float:
    """Average spread of second half minus first half.

    Negative = narrowing (price stabilizing, good).
    Positive = widening (deteriorating, bad).
    Returns 0.0 when fewer than 4 data points.
    """
    if len(spread_pcts) < 4:
        return 0.0
    mid_idx = len(spread_pcts) // 2
    first_half = spread_pcts[:mid_idx]
    second_half = spread_pcts[mid_idx:]
    return _avg(second_half) - _avg(first_half)


# ---------------------------------------------------------------------------
# 6. VWAP Distance — how far current price is from tick-VWAP
# ---------------------------------------------------------------------------

def vwap_distance(mids: list[float]) -> float:
    """Signed percentage distance from current price to tick-VWAP.

    Positive = above VWAP.  Negative = below VWAP.
    Returns 0.0 when fewer than 2 data points.
    """
    if len(mids) < 2:
        return 0.0
    vwap = _avg(mids)
    if vwap <= 0:
        return 0.0
    return ((mids[-1] - vwap) / vwap) * 100.0


# ---------------------------------------------------------------------------
# 7. Tick Cluster Position — where ticks concentrate in the price range
# ---------------------------------------------------------------------------

def tick_cluster_position(mids: list[float]) -> float:
    """Average normalized position within the price range.

    0.0 = all ticks at bottom (accumulation zone for longs).
    1.0 = all ticks at top (distribution zone for longs).
    0.5 = uniform distribution (neutral).
    Returns 0.5 when fewer than 3 data points or zero range.
    """
    if len(mids) < 3:
        return 0.5
    lo = min(mids)
    hi = max(mids)
    rng = hi - lo
    if rng <= 0:
        return 0.5
    positions = [(mid - lo) / rng for mid in mids]
    return _avg(positions)


# ---------------------------------------------------------------------------
# 8. Depth Imbalance Trend — direction of L2 imbalance change
# ---------------------------------------------------------------------------

def depth_imbalance_trend(depth_points: list[tuple]) -> float:
    """Split depth snapshots into thirds; compute imbalance per third; return late - early.

    depth_points: list of (ts, bid_vol, ask_vol) tuples.
    Positive = bids strengthening.  Negative = asks strengthening.
    Returns 0.0 when fewer than 3 points.
    """
    if len(depth_points) < 3:
        return 0.0
    third = len(depth_points) // 3

    def _avg_imb(segment: list[tuple]) -> float:
        imbs = []
        for _ts, bid, ask in segment:
            total = float(bid) + float(ask)
            if total > 0:
                imbs.append((float(bid) - float(ask)) / total)
        return _avg(imbs)

    early = depth_points[:third]
    late = depth_points[-third:]
    return _avg_imb(late) - _avg_imb(early)


# ---------------------------------------------------------------------------
# Batch: compute all 8 deep features at once
# ---------------------------------------------------------------------------

def compute_deep_features(
    *,
    mids: list[float],
    move_deltas: list[float],
    spot_ts: list[int],
    spread_pcts: list[float],
    depth_points: list[tuple],
) -> dict:
    """Compute all 8 deep microstructure features from raw tick/depth arrays.

    Returns a dict of feature_name -> float, safe to merge into the
    summarize_market_capture() output.
    """
    return {
        "tick_acceleration": round(tick_acceleration(mids), 6),
        "adverse_flow_streak": round(adverse_flow_streak(move_deltas), 6),
        "depth_absorption_rate": round(depth_absorption_rate(depth_points), 6),
        "micro_volatility": round(micro_volatility(mids), 6),
        "spread_trajectory": round(spread_trajectory(spread_pcts), 8),
        "vwap_distance": round(vwap_distance(mids), 6),
        "tick_cluster_position": round(tick_cluster_position(mids), 6),
        "depth_imbalance_trend": round(depth_imbalance_trend(depth_points), 6),
    }


# ---------------------------------------------------------------------------
# Composite Entry Sharpness Score (0-100)
# ---------------------------------------------------------------------------

def compute_entry_sharpness_score(
    features: dict,
    direction: str,
    *,
    weights: Optional[dict] = None,
    micro_vol_scale: float = 0.025,
    max_spread_expansion: float = 1.20,
) -> dict:
    """Compute composite entry sharpness score from feature dict + direction.

    Five dimensions (0-20 each) → total 0-100:
      1. Momentum Quality     — delta alignment, tick acceleration, tick ratio
      2. Flow Persistence     — adverse streak (inverted), imbalance trend, refill
      3. Absorption Quality   — depth absorption rate, rejection, imbalance
      4. Price Stability      — micro-vol (inv), spread trajectory (inv), expansion (inv)
      5. Positioning Quality  — vwap distance (favorable), tick cluster, bar volume

    Returns dict with score, band, per-dimension breakdown, and reasons.
    """
    feat = dict(features or {})
    side = str(direction or "").strip().lower()
    sign = 1.0 if side == "long" else -1.0 if side == "short" else 0.0
    w = dict(weights or {})
    w_mom = _safe_float(w.get("momentum"), 1.0)
    w_flow = _safe_float(w.get("flow"), 1.0)
    w_abs = _safe_float(w.get("absorption"), 1.0)
    w_stab = _safe_float(w.get("stability"), 1.0)
    w_pos = _safe_float(w.get("positioning"), 1.0)

    # --- Extract features (existing + deep) ---
    delta_proxy = _safe_float(feat.get("delta_proxy"), 0.0)
    tick_up_ratio = _safe_float(feat.get("tick_up_ratio"), 0.5)
    rejection_ratio = _safe_float(feat.get("rejection_ratio"), 0.0)
    depth_imbalance = _safe_float(feat.get("depth_imbalance"), 0.0)
    depth_refill_shift = _safe_float(feat.get("depth_refill_shift"), 0.0)
    spread_expansion = _safe_float(feat.get("spread_expansion"), 1.0)
    bar_volume_proxy = _safe_float(feat.get("bar_volume_proxy"), 0.0)
    # Deep features (default to neutral when missing)
    t_accel = _safe_float(feat.get("tick_acceleration"), 0.0)
    adv_streak = _safe_float(feat.get("adverse_flow_streak"), 0.0)
    d_absorb = _safe_float(feat.get("depth_absorption_rate"), 1.0)
    m_vol = _safe_float(feat.get("micro_volatility"), 0.0)
    s_traj = _safe_float(feat.get("spread_trajectory"), 0.0)
    v_dist = _safe_float(feat.get("vwap_distance"), 0.0)
    t_cluster = _safe_float(feat.get("tick_cluster_position"), 0.5)
    d_imb_trend = _safe_float(feat.get("depth_imbalance_trend"), 0.0)

    reasons: list[str] = []

    # ---- Dimension 1: Momentum Quality (0-20) ----
    aligned_delta = sign * delta_proxy
    delta_score = _clamp((aligned_delta + 0.15) / 0.30 * 10.0, 0.0, 10.0)
    aligned_accel = sign * t_accel
    accel_score = _clamp((aligned_accel + 0.10) / 0.20 * 5.0, 0.0, 5.0)
    tick_ratio = tick_up_ratio if side == "long" else (1.0 - tick_up_ratio) if side == "short" else 0.5
    tick_score = _clamp((tick_ratio - 0.35) / 0.30 * 5.0, 0.0, 5.0)
    momentum_quality = (delta_score + accel_score + tick_score) * w_mom
    if aligned_delta < -0.05:
        reasons.append("adverse_delta")
    if aligned_accel < -0.05:
        reasons.append("decaying_momentum")

    # ---- Dimension 2: Flow Persistence (0-20) ----
    streak_score = _clamp((0.35 - adv_streak) / 0.35 * 8.0, 0.0, 8.0)
    aligned_imb_trend = sign * d_imb_trend
    imb_trend_score = _clamp((aligned_imb_trend + 0.05) / 0.15 * 6.0, 0.0, 6.0)
    aligned_refill = sign * depth_refill_shift
    refill_score = _clamp((aligned_refill + 0.03) / 0.08 * 6.0, 0.0, 6.0)
    flow_persistence = (streak_score + imb_trend_score + refill_score) * w_flow
    if adv_streak > 0.30:
        reasons.append("long_adverse_streak")
    if aligned_imb_trend < -0.04:
        reasons.append("depth_deteriorating")

    # ---- Dimension 3: Absorption Quality (0-20) ----
    absorb_score = _clamp((d_absorb - 0.40) / 1.20 * 8.0, 0.0, 8.0)
    rejection_score = _clamp(rejection_ratio / 0.50 * 6.0, 0.0, 6.0)
    aligned_imb = sign * depth_imbalance
    imb_score = _clamp((aligned_imb + 0.02) / 0.10 * 6.0, 0.0, 6.0)
    absorption_quality = (absorb_score + rejection_score + imb_score) * w_abs
    if d_absorb < 0.50:
        reasons.append("depth_draining")

    # ---- Dimension 4: Price Stability (0-20) ----
    vol_scale = max(0.001, micro_vol_scale)
    vol_score = _clamp((vol_scale - m_vol) / vol_scale * 8.0, 0.0, 8.0)
    traj_score = _clamp((-s_traj + 0.0005) / 0.001 * 6.0, 0.0, 6.0)
    max_exp = max(1.01, max_spread_expansion)
    expansion_score = _clamp((max_exp - spread_expansion) / (max_exp - 1.0) * 6.0, 0.0, 6.0)
    price_stability = (vol_score + traj_score + expansion_score) * w_stab
    if m_vol > vol_scale * 0.8:
        reasons.append("choppy_micro_vol")
    if s_traj > 0.0003:
        reasons.append("spread_widening")

    # ---- Dimension 5: Positioning Quality (0-20) ----
    aligned_vwap = -sign * v_dist
    vwap_score = _clamp((aligned_vwap + 0.01) / 0.03 * 6.0, 0.0, 6.0)
    favorable_cluster = (0.5 - t_cluster) * sign * 2.0
    cluster_score = _clamp((favorable_cluster + 0.3) / 0.6 * 6.0, 0.0, 6.0)
    volume_score = _clamp(bar_volume_proxy / 0.65 * 8.0, 0.0, 8.0)
    positioning_quality = (vwap_score + cluster_score + volume_score) * w_pos
    if bar_volume_proxy < 0.20:
        reasons.append("low_volume")

    # ---- Composite ----
    raw_total = momentum_quality + flow_persistence + absorption_quality + price_stability + positioning_quality
    sharpness_score = int(_clamp(round(raw_total), 0, 100))

    # Band classification
    if sharpness_score < 30:
        band = "knife"
    elif sharpness_score < 50:
        band = "caution"
    elif sharpness_score < 70:
        band = "normal"
    else:
        band = "sharp"

    return {
        "sharpness_score": sharpness_score,
        "sharpness_band": band,
        "momentum_quality": round(momentum_quality, 2),
        "flow_persistence": round(flow_persistence, 2),
        "absorption_quality": round(absorption_quality, 2),
        "price_stability": round(price_stability, 2),
        "positioning_quality": round(positioning_quality, 2),
        "sharpness_reasons": reasons[:6],
    }
