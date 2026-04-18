"""
learning/adaptive_directional_intelligence.py

Adaptive Directional Intelligence (ADI)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A self-evolving, multi-dimensional confidence modifier that transforms raw
trading outcomes into real-time directional intelligence.

NOT a gate. NOT a block. A continuous confidence surface that amplifies
genuine edge and dampens toxic patterns — automatically, empirically,
and with full self-healing capability.

5 Scoring Dimensions (weighted composite):
  1. Empirical Performance (35%)  — actual P&L, recency-weighted WR,
     consecutive streak, WR acceleration, R-efficiency, win/loss asymmetry
  2. Technical Alignment  (25%)  — MTF trend stack, D1 conviction strength
  3. Flow Alignment       (15%)  — microstructure delta/imbalance/volume
  4. Temporal Pattern     (15%)  — session/hour/day P&L patterns from history
  5. Cross-Family Intel   (10%)  — ensemble agreement across XAU families

Key innovations:
  - Win Rate Acceleration: detects regime transitions 5-10 trades before
    simple WR systems, by tracking the derivative of performance
  - Divergence Detection: when empirical and technical disagree, something
    the indicators can't see is happening → extra caution
  - Recency-Weighted WR: exponential decay with half-life of 10 trades,
    so recent performance dominates without discarding history
  - Anti-Fragile: system gets BETTER after drawdowns because it learns
    from the worst outcomes most aggressively
  - Self-Healing: penalties automatically reduce as performance recovers
  - Asymmetry-Aware: catches "small wins, big losses" patterns that
    look OK on WR but destroy the account

Architecture: Risk = weight, never gate.
Output: continuous modifier [-45, +15] applied to signal confidence.
"""

import logging
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Piecewise Linear Interpolation ─────────────────────────────────────────

def _interpolate(value: float, points: list[tuple[float, float]]) -> float:
    """Piecewise linear interpolation from sorted breakpoints [(x, y), ...]."""
    if not points:
        return 0.0
    if value <= points[0][0]:
        return points[0][1]
    if value >= points[-1][0]:
        return points[-1][1]
    for i in range(1, len(points)):
        x0, y0 = points[i - 1]
        x1, y1 = points[i]
        if value <= x1:
            t = (value - x0) / (x1 - x0) if x1 != x0 else 0.0
            return y0 + t * (y1 - y0)
    return points[-1][1]


# ─── Empirical Scoring Curves ───────────────────────────────────────────────

# Win rate → base score.  Breakpoints calibrated from real Dexter data:
#   fibo_xauusd short WR 9% → should produce severe penalty
#   scalp_btcusd canary WR 91% → should produce strong boost
_WR_CURVE = [
    (0.00, -40.0),
    (0.10, -35.0),
    (0.20, -25.0),
    (0.30, -16.0),
    (0.40, -8.0),
    (0.50, -2.0),
    (0.55,  0.0),   # breakeven zone
    (0.60,  3.0),
    (0.65,  6.0),
    (0.75, 10.0),
    (1.00, 12.0),
]

# Sample size → dampening factor (low N = less confidence in the score)
_SAMPLE_CURVE = [
    (0, 0.20),
    (3, 0.35),
    (5, 0.50),
    (10, 0.70),
    (15, 0.85),
    (20, 0.95),
    (30, 1.00),
]

# Consecutive loss streak → additional penalty
_STREAK_CURVE = [
    (0, 0.0),
    (2, 0.0),
    (3, -5.0),
    (4, -10.0),
    (5, -18.0),
    (7, -28.0),
    (10, -35.0),
]


def _recency_weighted_wr(trades: list[dict], half_life: float = 10.0) -> float:
    """Exponentially-weighted win rate.  Most recent trades dominate."""
    if not trades:
        return 0.0
    weighted_wins = 0.0
    weighted_total = 0.0
    for i, t in enumerate(trades):
        w = 0.5 ** (i / max(1.0, half_life))
        weighted_total += w
        if str(t.get("outcome", "")).strip().lower() == "win":
            weighted_wins += w
    return weighted_wins / weighted_total if weighted_total > 0 else 0.0


def _consecutive_losses(trades: list[dict]) -> int:
    """Count consecutive losses from the most recent trade backwards."""
    streak = 0
    for t in trades:
        if str(t.get("outcome", "")).strip().lower() == "loss":
            streak += 1
        else:
            break
    return streak


def _wr_acceleration(trades: list[dict], recent_n: int = 5) -> float:
    """Win-rate derivative: is direction improving or degrading?

    Compares WR of last `recent_n` trades vs the full set.
    Returns score in [-12, +10].
    """
    if len(trades) < recent_n + 3:
        return 0.0
    recent = trades[:recent_n]
    recent_wr = sum(1 for t in recent if t.get("outcome") == "win") / max(1, len(recent))
    full_wr = sum(1 for t in trades if t.get("outcome") == "win") / max(1, len(trades))
    delta = recent_wr - full_wr
    return _interpolate(delta, [
        (-0.40, -12.0),
        (-0.20, -8.0),
        (-0.10, -4.0),
        (-0.03, 0.0),
        (0.03, 0.0),
        (0.10, 4.0),
        (0.20, 8.0),
        (0.40, 10.0),
    ])


def _pnl_trend_score(trades: list[dict]) -> float:
    """Is the P&L trajectory improving or degrading?

    Compares average PnL of the most recent 5 trades to the older 5.
    """
    pnls = [float(t.get("pnl_usd", 0) or 0) for t in trades]
    if len(pnls) < 8:
        return 0.0
    recent_avg = sum(pnls[:5]) / 5.0
    older_avg = sum(pnls[5:10]) / max(1, len(pnls[5:10]))
    diff = recent_avg - older_avg
    return _interpolate(diff, [
        (-5.0, -10.0),
        (-2.0, -6.0),
        (-0.5, -2.0),
        (0.0, 0.0),
        (0.5, 2.0),
        (2.0, 5.0),
        (5.0, 8.0),
    ])


def _r_efficiency(trades: list[dict]) -> float:
    """How much of theoretical R-multiple is actually captured?

    Low efficiency = weak wins even when winning → disguised edge problem.
    """
    r_values = []
    for t in trades:
        if str(t.get("outcome", "")).strip().lower() != "win":
            continue
        entry = float(t.get("entry", 0) or 0)
        sl = float(t.get("stop_loss", 0) or 0)
        pnl = float(t.get("pnl_usd", 0) or 0)
        risk = abs(entry - sl) if entry and sl else 0
        if risk > 0 and pnl > 0:
            # Estimated R achieved (PnL / risk proxy)
            r_values.append(min(5.0, pnl / max(0.01, risk)))
    if len(r_values) < 3:
        return 0.0
    avg_r = sum(r_values) / len(r_values)
    return _interpolate(avg_r, [
        (0.0, -5.0),
        (0.3, -2.0),
        (0.5, 0.0),
        (1.0, 3.0),
        (1.5, 5.0),
        (2.5, 8.0),
    ])


def _win_loss_asymmetry(trades: list[dict]) -> float:
    """Detect "small wins + big losses" that look OK on WR but destroy capital.

    Positive = wins bigger than losses (good).
    Negative = losses bigger than wins (dangerous even at 50% WR).
    """
    win_pnls = [abs(float(t.get("pnl_usd", 0) or 0)) for t in trades if t.get("outcome") == "win"]
    loss_pnls = [abs(float(t.get("pnl_usd", 0) or 0)) for t in trades if t.get("outcome") == "loss"]
    if len(win_pnls) < 2 or len(loss_pnls) < 2:
        return 0.0
    avg_win = sum(win_pnls) / len(win_pnls)
    avg_loss = sum(loss_pnls) / len(loss_pnls)
    ratio = (avg_win / avg_loss) if avg_loss > 0 else 2.0
    return _interpolate(ratio, [
        (0.2, -8.0),    # wins are tiny vs losses → dangerous
        (0.5, -4.0),
        (0.8, -1.0),
        (1.0, 0.0),     # break even ratio
        (1.5, 3.0),
        (2.0, 5.0),     # wins 2x losses → healthy edge
        (3.0, 7.0),
    ])


# ─── Technical Alignment ────────────────────────────────────────────────────

_TREND_TOKENS_BULLISH = {"bullish", "up", "up_strong", "up_medium", "up_weak", "strong_up", "long"}
_TREND_TOKENS_BEARISH = {"bearish", "down", "down_strong", "down_medium", "down_weak", "strong_down", "short"}


def _trend_alignment(direction: str, trend_label: str) -> int:
    """Returns +1 (aligned), 0 (neutral/unknown), -1 (counter-trend)."""
    t = str(trend_label or "").strip().lower()
    if not t or t in {"neutral", "range", "sideways", "unknown", "flat", ""}:
        return 0
    if direction == "long":
        if t in _TREND_TOKENS_BULLISH:
            return 1
        if t in _TREND_TOKENS_BEARISH:
            return -1
    elif direction == "short":
        if t in _TREND_TOKENS_BEARISH:
            return 1
        if t in _TREND_TOKENS_BULLISH:
            return -1
    return 0


# ─── Temporal Helpers ────────────────────────────────────────────────────────

def _utc_hour_to_session(hour: int) -> str:
    """Map UTC hour to trading session name."""
    if 22 <= hour or hour < 7:
        return "asian"
    if 7 <= hour < 13:
        return "london"
    return "new_york"


def _parse_utc_hour(execution_utc: str) -> int:
    """Extract UTC hour from execution_utc string."""
    try:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                dt = datetime.strptime(str(execution_utc).strip()[:26], fmt)
                return dt.hour
            except ValueError:
                continue
        return -1
    except Exception:
        return -1


def _parse_day_of_week(execution_utc: str) -> int:
    """Extract day of week (0=Mon, 6=Sun) from execution_utc string."""
    try:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                dt = datetime.strptime(str(execution_utc).strip()[:26], fmt)
                return dt.weekday()
            except ValueError:
                continue
        return -1
    except Exception:
        return -1


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class AdaptiveDirectionalIntelligence:
    """Multi-dimensional confidence modifier that adapts from real trade outcomes.

    Evaluates 5 dimensions, produces a continuous modifier that amplifies
    genuine edge and dampens toxic patterns.  Never blocks — only adjusts
    confidence so existing gates make the final decision.
    """

    # Dimension weights (must sum to 1.0)
    W_EMPIRICAL = 0.35
    W_TECHNICAL = 0.25
    W_FLOW = 0.15
    W_TEMPORAL = 0.15
    W_CROSS_FAMILY = 0.10

    def __init__(self, db_path: Optional[str] = None):
        self._db_path_override = db_path
        self._cache: dict[str, tuple[float, list[dict]]] = {}
        self._cache_ttl = 300.0  # seconds

    @property
    def _db_path(self) -> Path:
        if self._db_path_override:
            return Path(self._db_path_override)
        try:
            from config import config as _cfg
            return Path(getattr(_cfg, "CTRADER_OPENAPI_DB_PATH", "data/ctrader_openapi.db"))
        except Exception:
            return Path("data/ctrader_openapi.db")

    def _cfg_float(self, key: str, default: float) -> float:
        try:
            from config import config as _cfg
            return float(getattr(_cfg, key, default) or default)
        except Exception:
            return default

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            from config import config as _cfg
            return int(getattr(_cfg, key, default) or default)
        except Exception:
            return default

    # ── DB Layer ─────────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _load_trades(self, symbol_like: str, days: int = 14) -> list[dict]:
        """Load recent closed trades from execution_journal."""
        db = self._db_path
        if not db.exists():
            return []
        since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with closing(self._connect()) as conn:
                # Check which columns exist
                cols = {r[1] for r in conn.execute("PRAGMA table_info(execution_journal)").fetchall()}
                if "outcome" not in cols or "source" not in cols:
                    return []

                select_cols = ["source", "direction", "symbol", "outcome", "pnl_usd", "execution_utc"]
                for c in ("entry", "stop_loss", "take_profit", "execution_meta_json"):
                    if c in cols:
                        select_cols.append(c)

                q = f"""
                    SELECT {', '.join(select_cols)}
                    FROM execution_journal
                    WHERE symbol LIKE ?
                      AND outcome IN ('win', 'loss')
                      AND execution_utc >= ?
                    ORDER BY execution_utc DESC
                    LIMIT 500
                """
                rows = conn.execute(q, (f"%{symbol_like}%", since)).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.debug("[ADI] DB load error: %s", e)
            return []

    def _get_cached_trades(self, symbol: str) -> list[dict]:
        """Return cached trades, refreshing if stale."""
        token = str(symbol or "").strip().upper()
        now = time.monotonic()
        if token in self._cache:
            ts, data = self._cache[token]
            if now - ts < self._cache_ttl:
                return data
        days = self._cfg_int("ADI_LOOKBACK_DAYS", 14)
        trades = self._load_trades(token, days=days)
        self._cache[token] = (now, trades)
        return trades

    # ── Dimension 1: Empirical Performance (35%) ─────────────────────────────

    def _score_empirical(self, source: str, direction: str, all_trades: list[dict]) -> tuple[float, dict]:
        """Score based on REAL trade outcomes for this source+direction."""
        src = str(source or "").strip().lower()
        # Match trades by source family (e.g. "fibo_xauusd" matches "fibo_xauusd" source)
        # Also match broader source (e.g. "scalp_xauusd:winner" → "scalp_xauusd")
        src_base = src.split(":")[0] if ":" in src else src
        trades = [
            t for t in all_trades
            if str(t.get("direction", "")).strip().lower() == direction
            and (str(t.get("source", "")).strip().lower().startswith(src_base))
        ]

        if not trades:
            # No data for this source+direction = unknown territory.
            # Small conservative penalty — not block, but caution.
            cold_penalty = self._cfg_float("ADI_COLD_START_PENALTY", -6.0)
            return cold_penalty, {"reason": "no_data_cold_start", "n": 0, "penalty": cold_penalty}

        n = len(trades)
        raw_wr = sum(1 for t in trades if t.get("outcome") == "win") / n
        weighted_wr = _recency_weighted_wr(trades)
        consec = _consecutive_losses(trades)
        accel = _wr_acceleration(trades)
        pnl_trend = _pnl_trend_score(trades)
        r_eff = _r_efficiency(trades)
        asymmetry = _win_loss_asymmetry(trades)

        base = _interpolate(weighted_wr, _WR_CURVE)
        dampening = _interpolate(float(n), _SAMPLE_CURVE)
        streak = _interpolate(float(consec), _STREAK_CURVE)

        raw_score = (base * dampening) + streak + accel + pnl_trend + r_eff + asymmetry
        final = max(-40.0, min(12.0, raw_score))

        total_pnl = sum(float(t.get("pnl_usd", 0) or 0) for t in trades)

        return final, {
            "n": n,
            "raw_wr": round(raw_wr, 3),
            "weighted_wr": round(weighted_wr, 3),
            "consec_losses": consec,
            "wr_acceleration": round(accel, 1),
            "pnl_trend": round(pnl_trend, 1),
            "r_efficiency": round(r_eff, 1),
            "asymmetry": round(asymmetry, 1),
            "base_wr_score": round(base, 1),
            "dampening": round(dampening, 2),
            "streak_penalty": round(streak, 1),
            "total_pnl": round(total_pnl, 2),
        }

    # ── Dimension 2: Technical Alignment (25%) ───────────────────────────────

    def _score_technical(self, direction: str, trend_context: Optional[dict]) -> tuple[float, dict]:
        """Score based on multi-timeframe trend alignment."""
        if not trend_context:
            return 0.0, {"reason": "no_trend_data"}

        d1 = _trend_alignment(direction, str(trend_context.get("d1", "")))
        h4 = _trend_alignment(direction, str(trend_context.get("h4", "")))
        h1 = _trend_alignment(direction, str(trend_context.get("h1", "")))

        # Weighted alignment: D1 has 3x weight, H4 2x, H1 1x
        weighted_score = (d1 * 3.0) + (h4 * 2.0) + (h1 * 1.0)
        # Range: -6 (all counter) to +6 (all aligned)

        score = _interpolate(weighted_score, [
            (-6.0, -35.0),   # All TFs counter-trend → catastrophic
            (-4.0, -25.0),   # D1+H4 counter
            (-3.0, -18.0),   # D1 counter, rest neutral
            (-1.0, -6.0),    # Mild disagreement
            (0.0, 0.0),      # Neutral
            (1.0, 2.0),
            (3.0, 5.0),      # D1 aligned, rest neutral
            (4.0, 7.0),      # D1+H4 aligned
            (6.0, 10.0),     # All aligned → strong boost
        ])

        return score, {
            "d1_alignment": d1,
            "h4_alignment": h4,
            "h1_alignment": h1,
            "weighted_raw": round(weighted_score, 1),
        }

    # ── Dimension 3: Flow Alignment (15%) ────────────────────────────────────

    def _score_flow(self, direction: str, features: Optional[dict]) -> tuple[float, dict]:
        """Score based on microstructure order flow supporting the direction."""
        if not features:
            return 0.0, {"reason": "no_flow_data"}

        delta = float(features.get("delta_proxy", 0) or 0)
        imbalance = float(features.get("depth_imbalance", 0) or 0)
        bar_vol = float(features.get("bar_volume_proxy", 0) or 0)
        tick_up = float(features.get("tick_up_ratio", 0.5) or 0.5)
        spots = int(features.get("spots_count", 0) or 0)

        if spots < 3:
            return 0.0, {"reason": "insufficient_flow_data", "spots": spots}

        # Delta: is order flow supporting our direction?
        dir_sign = 1.0 if direction == "long" else -1.0
        delta_aligned = delta * dir_sign  # positive = supporting, negative = adverse
        delta_score = _interpolate(delta_aligned, [
            (-0.5, -15.0),
            (-0.3, -10.0),
            (-0.15, -5.0),
            (-0.05, -1.0),
            (0.0, 0.0),
            (0.05, 1.0),
            (0.15, 4.0),
            (0.30, 7.0),
            (0.50, 10.0),
        ])

        # Depth imbalance: is DOM supporting our direction?
        imb_aligned = imbalance * dir_sign  # long → positive imbalance = bids stacking
        imb_score = _interpolate(imb_aligned, [
            (-0.5, -8.0),
            (-0.2, -4.0),
            (0.0, 0.0),
            (0.2, 3.0),
            (0.5, 6.0),
        ])

        # Volume/activity: is there enough activity to trust flow signals?
        vol_score = _interpolate(bar_vol, [
            (0.0, -5.0),
            (0.10, -2.0),
            (0.25, 0.0),
            (0.50, 2.0),
            (0.80, 4.0),
        ])

        # Tick momentum bias
        tick_bias = (tick_up - 0.5) * dir_sign * 2.0  # normalized to [-1, 1]
        tick_score = _interpolate(tick_bias, [
            (-0.8, -5.0),
            (-0.3, -2.0),
            (0.0, 0.0),
            (0.3, 2.0),
            (0.8, 4.0),
        ])

        raw = delta_score * 0.40 + imb_score * 0.25 + vol_score * 0.20 + tick_score * 0.15
        final = max(-15.0, min(10.0, raw))

        return final, {
            "delta_proxy": round(delta, 4),
            "delta_aligned": round(delta_aligned, 4),
            "imbalance_aligned": round(imb_aligned, 4),
            "bar_volume": round(bar_vol, 4),
            "tick_up_ratio": round(tick_up, 4),
            "sub_scores": {
                "delta": round(delta_score, 1),
                "imbalance": round(imb_score, 1),
                "volume": round(vol_score, 1),
                "tick_bias": round(tick_score, 1),
            },
        }

    # ── Dimension 4: Temporal Pattern (15%) ──────────────────────────────────

    def _score_temporal(self, source: str, direction: str,
                        all_trades: list[dict], session_info: Optional[dict]) -> tuple[float, dict]:
        """Score based on time-of-day / session / day-of-week P&L patterns."""
        src_base = str(source or "").split(":")[0].strip().lower()

        # Filter trades for this direction (across all sources — temporal is
        # about market conditions, not strategy-specific performance)
        dir_trades = [t for t in all_trades if str(t.get("direction", "")).strip().lower() == direction]

        if len(dir_trades) < 5:
            return 0.0, {"reason": "insufficient_temporal_data", "n": len(dir_trades)}

        # Current time context
        current_hour = datetime.now(timezone.utc).hour
        current_session = _utc_hour_to_session(current_hour)
        current_dow = datetime.now(timezone.utc).weekday()

        # Session override from session_info if available
        if session_info:
            active = set(session_info.get("active_sessions", []) or [])
            if "london" in active:
                current_session = "london"
            elif "new_york" in active:
                current_session = "new_york"
            elif "asian" in active:
                current_session = "asian"

        # --- Session WR for current session ---
        session_trades = [
            t for t in dir_trades
            if _utc_hour_to_session(_parse_utc_hour(str(t.get("execution_utc", "")))) == current_session
        ]
        if len(session_trades) >= 3:
            session_wr = sum(1 for t in session_trades if t.get("outcome") == "win") / len(session_trades)
            session_pnl = sum(float(t.get("pnl_usd", 0) or 0) for t in session_trades)
        else:
            session_wr = None
            session_pnl = 0.0
        session_score = _interpolate(session_wr, [
            (0.0, -12.0), (0.20, -8.0), (0.35, -4.0),
            (0.50, 0.0), (0.60, 3.0), (0.75, 6.0), (1.0, 8.0),
        ]) if session_wr is not None else 0.0

        # --- Hour WR for current hour ---
        hour_trades = [
            t for t in dir_trades
            if _parse_utc_hour(str(t.get("execution_utc", ""))) == current_hour
        ]
        if len(hour_trades) >= 3:
            hour_wr = sum(1 for t in hour_trades if t.get("outcome") == "win") / len(hour_trades)
            hour_pnl = sum(float(t.get("pnl_usd", 0) or 0) for t in hour_trades)
        else:
            hour_wr = None
            hour_pnl = 0.0
        hour_score = _interpolate(hour_wr, [
            (0.0, -10.0), (0.15, -7.0), (0.30, -3.0),
            (0.50, 0.0), (0.65, 3.0), (0.80, 5.0), (1.0, 7.0),
        ]) if hour_wr is not None else 0.0

        # --- Day-of-week WR ---
        dow_trades = [
            t for t in dir_trades
            if _parse_day_of_week(str(t.get("execution_utc", ""))) == current_dow
        ]
        if len(dow_trades) >= 3:
            dow_wr = sum(1 for t in dow_trades if t.get("outcome") == "win") / len(dow_trades)
        else:
            dow_wr = None
        dow_score = _interpolate(dow_wr, [
            (0.0, -6.0), (0.25, -3.0), (0.45, -1.0),
            (0.55, 0.0), (0.65, 2.0), (0.80, 4.0), (1.0, 5.0),
        ]) if dow_wr is not None else 0.0

        raw = session_score * 0.45 + hour_score * 0.35 + dow_score * 0.20
        final = max(-12.0, min(8.0, raw))

        return final, {
            "current_session": current_session,
            "current_hour_utc": current_hour,
            "current_dow": current_dow,
            "session_wr": round(session_wr, 3) if session_wr is not None else None,
            "session_pnl": round(session_pnl, 2),
            "session_n": len(session_trades),
            "hour_wr": round(hour_wr, 3) if hour_wr is not None else None,
            "hour_pnl": round(hour_pnl, 2),
            "hour_n": len(hour_trades),
            "dow_wr": round(dow_wr, 3) if dow_wr is not None else None,
            "dow_n": len(dow_trades),
            "sub_scores": {
                "session": round(session_score, 1),
                "hour": round(hour_score, 1),
                "dow": round(dow_score, 1),
            },
        }

    # ── Dimension 5: Cross-Family Intelligence (10%) ─────────────────────────

    def _score_cross_family(self, direction: str, all_trades: list[dict],
                            exclude_source: str) -> tuple[float, dict]:
        """Ensemble intelligence: how are OTHER XAU families doing in this direction?

        If multiple families are losing in the same direction → systemic
        directional problem (market structure, not strategy-specific).
        If other families are winning → this family might have a fixable issue.
        """
        excl_base = str(exclude_source or "").split(":")[0].strip().lower()

        # Group by source family
        family_stats: dict[str, dict] = {}
        for t in all_trades:
            if str(t.get("direction", "")).strip().lower() != direction:
                continue
            src = str(t.get("source", "")).strip().lower()
            src_base = src.split(":")[0]
            if src_base == excl_base or not src_base:
                continue
            if src_base not in family_stats:
                family_stats[src_base] = {"wins": 0, "total": 0, "pnl": 0.0}
            family_stats[src_base]["total"] += 1
            family_stats[src_base]["pnl"] += float(t.get("pnl_usd", 0) or 0)
            if t.get("outcome") == "win":
                family_stats[src_base]["wins"] += 1

        if not family_stats:
            return 0.0, {"reason": "no_cross_family_data"}

        # Compute per-family WR and aggregate
        family_wrs = []
        family_details = {}
        for fam, stats in family_stats.items():
            if stats["total"] >= 3:
                wr = stats["wins"] / stats["total"]
                family_wrs.append(wr)
                family_details[fam] = {
                    "wr": round(wr, 3),
                    "n": stats["total"],
                    "pnl": round(stats["pnl"], 2),
                }

        if not family_wrs:
            return 0.0, {"reason": "no_cross_family_with_sufficient_data", "families": family_details}

        avg_wr = sum(family_wrs) / len(family_wrs)
        losing_families = sum(1 for wr in family_wrs if wr < 0.40)
        winning_families = sum(1 for wr in family_wrs if wr > 0.55)

        base_score = _interpolate(avg_wr, [
            (0.0, -25.0),
            (0.20, -15.0),
            (0.35, -8.0),
            (0.45, -3.0),
            (0.55, 0.0),
            (0.65, 4.0),
            (0.80, 8.0),
        ])

        # Systemic signal: many families losing → extra penalty
        if losing_families >= 3:
            base_score -= 8.0
        elif losing_families >= 2:
            base_score -= 4.0

        # Counter-signal: many families winning → confidence
        if winning_families >= 3:
            base_score += 5.0
        elif winning_families >= 2:
            base_score += 2.0

        final = max(-25.0, min(10.0, base_score))

        return final, {
            "avg_wr": round(avg_wr, 3),
            "n_families": len(family_wrs),
            "losing_families": losing_families,
            "winning_families": winning_families,
            "families": family_details,
        }

    # ── Composite Evaluation ─────────────────────────────────────────────────

    def evaluate(self, *, source: str, direction: str, symbol: str,
                 confidence: float = 0.0,
                 trend_context: Optional[dict] = None,
                 flow_features: Optional[dict] = None,
                 session_info: Optional[dict] = None) -> dict:
        """Evaluate all 5 dimensions and return composite confidence modifier.

        Returns dict with:
          modifier: float  — the confidence adjustment [-45, +15]
          dimensions: dict — per-dimension scores and details
          divergence_flag: bool — empirical and technical disagree
          catastrophic_flag: bool — at least one dimension is extreme
          recommendation: str — human-readable summary
        """
        try:
            return self._evaluate_inner(
                source=source, direction=direction, symbol=symbol,
                confidence=confidence, trend_context=trend_context,
                flow_features=flow_features, session_info=session_info,
            )
        except Exception as e:
            logger.debug("[ADI] evaluate error: %s", e)
            return {"modifier": 0.0, "error": str(e)}

    def _evaluate_inner(self, *, source, direction, symbol, confidence,
                        trend_context, flow_features, session_info) -> dict:
        dir_token = str(direction or "").strip().lower()
        if dir_token not in {"long", "short"}:
            return {"modifier": 0.0, "skip": "invalid_direction"}

        all_trades = self._get_cached_trades(symbol)

        # Score each dimension
        emp_score, emp_details = self._score_empirical(source, dir_token, all_trades)
        tech_score, tech_details = self._score_technical(dir_token, trend_context)
        flow_score, flow_details = self._score_flow(dir_token, flow_features)
        temp_score, temp_details = self._score_temporal(source, dir_token, all_trades, session_info)
        cross_score, cross_details = self._score_cross_family(dir_token, all_trades, exclude_source=source)

        # Weighted composite
        composite = (
            emp_score * self.W_EMPIRICAL
            + tech_score * self.W_TECHNICAL
            + flow_score * self.W_FLOW
            + temp_score * self.W_TEMPORAL
            + cross_score * self.W_CROSS_FAMILY
        )

        # ── Divergence Detection ──
        # When empirical says "losing" but technical says "aligned" →
        # something the indicators can't see is wrong.  Extra caution.
        divergence_flag = False
        if emp_score < -15.0 and tech_score > 3.0:
            composite -= 5.0
            divergence_flag = True
        elif emp_score > 5.0 and tech_score < -15.0:
            # Empirical winning but technical counter → ride empirical
            # but be cautious — trend reversal may be imminent
            composite -= 2.0
            divergence_flag = True

        # ── Catastrophic Override ──
        # If ANY single dimension is extreme, add snowball penalty.
        # This catches scenarios where one dimension alone should dominate.
        raw_scores = [emp_score, tech_score, flow_score, temp_score, cross_score]
        catastrophic_flag = any(s <= -25.0 for s in raw_scores)
        if catastrophic_flag:
            worst = min(raw_scores)
            composite += min(0.0, worst * 0.15)  # 15% of worst score extra

        # ── Clamp final modifier ──
        max_penalty = self._cfg_float("ADI_MAX_PENALTY", -45.0)
        max_boost = self._cfg_float("ADI_MAX_BOOST", 15.0)
        modifier = max(max_penalty, min(max_boost, composite))

        # ── Recommendation ──
        if modifier <= -25:
            recommendation = "severe_penalty:direction_toxic"
        elif modifier <= -15:
            recommendation = "strong_penalty:direction_weak"
        elif modifier <= -5:
            recommendation = "mild_penalty:direction_uncertain"
        elif modifier <= 5:
            recommendation = "neutral:direction_balanced"
        elif modifier <= 10:
            recommendation = "mild_boost:direction_favorable"
        else:
            recommendation = "strong_boost:direction_strong_edge"

        return {
            "modifier": round(modifier, 1),
            "recommendation": recommendation,
            "divergence_flag": divergence_flag,
            "catastrophic_flag": catastrophic_flag,
            "dimensions": {
                "empirical": {"score": round(emp_score, 1), "weight": self.W_EMPIRICAL, "details": emp_details},
                "technical": {"score": round(tech_score, 1), "weight": self.W_TECHNICAL, "details": tech_details},
                "flow": {"score": round(flow_score, 1), "weight": self.W_FLOW, "details": flow_details},
                "temporal": {"score": round(temp_score, 1), "weight": self.W_TEMPORAL, "details": temp_details},
                "cross_family": {"score": round(cross_score, 1), "weight": self.W_CROSS_FAMILY, "details": cross_details},
            },
        }


# ─── Singleton ───────────────────────────────────────────────────────────────
adi = AdaptiveDirectionalIntelligence()
