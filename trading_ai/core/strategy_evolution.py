from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


def build_strategy_key(features: Dict[str, Any], setup_tag: str) -> str:
    """
    Canonical strategy id: trend * volatility * session _ setup_tag
    Example: UP*HIGH*NY_breakout
    """
    t = str(features.get("trend_direction") or "RANGE").upper()
    v = str(features.get("volatility") or "MEDIUM").upper()
    s = str(features.get("session") or "ASIA").upper()
    tag = str(setup_tag or "trend_follow").lower()
    return f"{t}*{v}*{s}_{tag}"


@dataclass
class StrategyStats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    total_profit: float = 0.0
    score: float = 0.0
    """Fundamental: (wins/trades) * total_profit — refreshed on each closed trade."""
    ranking_score: float = 0.0
    """Competition ranking: decays each loop; reset from `score` when a new trade closes."""
    active: bool = True
    lane_stage: str = "candidate"
    pending_recommendation: str = ""
    shadow_trades: int = 0
    shadow_wins: int = 0
    shadow_losses: int = 0
    shadow_total_profit: float = 0.0


class StrategyRegistry:
    """
    Runtime strategy evolution with v2: global ranking, exploration, aging, capital weights.
    """

    def __init__(self, persist_path: Path) -> None:
        self._path = Path(persist_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._stats: Dict[str, StrategyStats] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if not self._path.is_file():
            log.info("StrategyRegistry: no snapshot at %s — starting empty", self._path)
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("StrategyRegistry: failed to load %s: %s", self._path, exc)
            return
        if not isinstance(raw, dict):
            return
        for key, blob in raw.items():
            if not isinstance(blob, dict):
                continue
            try:
                score_f = float(blob.get("score", 0.0))
                rk = blob.get("ranking_score")
                rk_f = float(rk) if rk is not None else score_f
                self._stats[str(key)] = StrategyStats(
                    trades=int(blob.get("trades", 0)),
                    wins=int(blob.get("wins", 0)),
                    losses=int(blob.get("losses", 0)),
                    total_profit=float(blob.get("total_profit", 0.0)),
                    score=score_f,
                    ranking_score=rk_f,
                    active=bool(blob.get("active", True)),
                    lane_stage=str(blob.get("lane_stage") or "candidate"),
                    pending_recommendation=str(blob.get("pending_recommendation") or ""),
                    shadow_trades=int(blob.get("shadow_trades", 0)),
                    shadow_wins=int(blob.get("shadow_wins", 0)),
                    shadow_losses=int(blob.get("shadow_losses", 0)),
                    shadow_total_profit=float(blob.get("shadow_total_profit", 0.0)),
                )
            except (TypeError, ValueError):
                continue
        log.info("StrategyRegistry: loaded %s strategies from %s", len(self._stats), self._path)

    def _persist_unlocked(self) -> None:
        out: Dict[str, Dict[str, Any]] = {}
        for k, st in self._stats.items():
            out[k] = asdict(st)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)

    def save(self) -> None:
        with self._lock:
            self._persist_unlocked()

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {k: asdict(v) for k, v in self._stats.items()}

    def get_stats(self, strategy_key: str) -> Optional[StrategyStats]:
        with self._lock:
            st = self._stats.get(strategy_key)
            return StrategyStats(**asdict(st)) if st else None

    def win_rate(self, strategy_key: str) -> float:
        with self._lock:
            st = self._stats.get(strategy_key)
            if not st or st.trades <= 0:
                return 0.0
            return st.wins / float(st.trades)

    def _refresh_score_and_active(self, st: StrategyStats) -> None:
        if st.trades <= 0:
            st.score = 0.0
            st.ranking_score = 0.0
            st.active = True
            return
        wr = st.wins / float(st.trades)
        st.score = wr * st.total_profit
        st.ranking_score = float(st.score)
        disable = (st.trades >= 10 and wr < 0.45) or (st.total_profit < 0)
        st.active = not disable

    def apply_aging(self, factor: float) -> None:
        """Decay ranking_score each loop (fundamental `score` unchanged until next close)."""
        if factor <= 0.0 or factor >= 1.0:
            return
        with self._lock:
            for st in self._stats.values():
                st.ranking_score *= factor
            self._persist_unlocked()

    def get_top_strategies(
        self,
        n: int,
        *,
        active_only: bool = True,
        min_trades: int = 0,
    ) -> List[Tuple[str, StrategyStats]]:
        with self._lock:
            items: List[Tuple[str, StrategyStats]] = []
            for k, st in self._stats.items():
                if active_only and not st.active:
                    continue
                if st.trades < min_trades:
                    continue
                items.append((k, StrategyStats(**asdict(st))))
        items.sort(key=lambda x: x[1].ranking_score, reverse=True)
        return items[: max(0, n)]

    def is_exploration(self, strategy_key: str, exploration_max_trades: int) -> bool:
        """Few samples — bypass global rank gate so new setups are not starved."""
        with self._lock:
            st = self._stats.get(strategy_key)
            if st is None:
                return True
            return st.trades < exploration_max_trades

    def passes_global_rank(
        self,
        strategy_key: str,
        *,
        top_n: int,
        exploration_max_trades: int,
    ) -> Tuple[bool, str]:
        """
        Mature strategies (trades >= exploration_max_trades) must sit in top-N by ranking_score
        among active mature peers. If fewer than top_n mature peers exist, all pass.
        """
        with self._lock:
            if strategy_key not in self._stats:
                return True, "exploration_unknown_key"
            st = self._stats[strategy_key]
            if st.trades < exploration_max_trades:
                return True, "exploration_sample"
            mature: List[Tuple[str, StrategyStats]] = [
                (k, s)
                for k, s in self._stats.items()
                if s.active and s.trades >= exploration_max_trades
            ]
            if len(mature) <= top_n:
                return True, "insufficient_mature_pool"
            mature.sort(key=lambda x: x[1].ranking_score, reverse=True)
            top_keys = {k for k, _ in mature[:top_n]}
            if strategy_key in top_keys:
                return True, f"in_top_{top_n}"
            return False, "not_in_global_top"

    def get_position_size_multiplier(
        self,
        strategy_key: str,
        *,
        pool: int,
        clamp_min: float,
        clamp_max: float,
    ) -> float:
        """
        Share of capital among top `pool` strategies by max(0, ranking_score).
        mult ≈ 1.0 when weights are uniform in the pool.
        """
        top = self.get_top_strategies(pool, active_only=True, min_trades=1)
        if not top:
            return 1.0
        key_to = {k: st for k, st in top}
        if strategy_key not in key_to:
            return float(max(clamp_min, min(clamp_max, clamp_min)))
        pos = [max(0.0, st.ranking_score) for st in key_to.values()]
        tot = float(sum(pos))
        if tot <= 0.0:
            return max(clamp_min, min(clamp_max, 1.0))
        my_w = max(0.0, key_to[strategy_key].ranking_score)
        w_share = my_w / tot
        mult = w_share * float(len(pos))
        return float(max(clamp_min, min(clamp_max, mult)))

    def update_strategy(self, strategy_key: str, result: Dict[str, Any]) -> None:
        """
        result: expects 'pnl' (float) and 'score' (int: -1 loss, 0 neutral, +1 win)
        """
        pnl = float(result.get("pnl") or 0.0)
        outcome = int(result.get("score", 0))

        with self._lock:
            st = self._stats.setdefault(strategy_key, StrategyStats())
            st.trades += 1
            st.total_profit += pnl
            if outcome > 0:
                st.wins += 1
            elif outcome < 0:
                st.losses += 1
            self._refresh_score_and_active(st)
            self._persist_unlocked()
            td, wn, ls, tp, ac, stage, sc, rk, wr = (
                st.trades,
                st.wins,
                st.losses,
                st.total_profit,
                st.active,
                st.lane_stage,
                st.score,
                st.ranking_score,
                (st.wins / float(st.trades)) if st.trades else 0.0,
            )

        log.info(
            "StrategyRegistry updated: %s trades=%s wins=%s losses=%s pnl_sum=%.5f wr=%.3f active=%s stage=%s score=%.5f rank=%.5f",
            strategy_key,
            td,
            wn,
            ls,
            tp,
            wr,
            ac,
            stage,
            sc,
            rk,
        )

    def is_strategy_allowed(self, strategy_key: str) -> bool:
        with self._lock:
            st = self._stats.get(strategy_key)
            if st is None:
                return True
            if st.lane_stage == "retired":
                return False
            if st.pending_recommendation in {"quarantine", "quarantine_shadow"}:
                return False
            if st.active:
                return True
            if st.pending_recommendation in {"probation_boost", "promote_from_shadow"}:
                return True
            if st.shadow_trades >= 2 and st.shadow_total_profit > 0 and st.shadow_wins >= st.shadow_losses:
                return True
            return False

    def get_strategy_boost(self, strategy_key: str) -> float:
        with self._lock:
            st = self._stats.get(strategy_key)
            if st is None:
                return 0.0
            if st.pending_recommendation in {"quarantine", "quarantine_shadow"} or st.lane_stage == "retired":
                return 0.0
            shadow_wr = (st.shadow_wins / float(st.shadow_trades)) if st.shadow_trades else 0.0
            if st.pending_recommendation == "promote_from_shadow" and st.shadow_trades >= 2 and shadow_wr >= 0.6:
                return 0.08
            if st.pending_recommendation == "probation_boost" and (st.shadow_trades + st.trades) >= 2:
                return 0.05
            if not st.active or st.trades <= 0:
                return 0.0
            wr = st.wins / float(st.trades)
            if wr > 0.6 and st.trades > 15:
                return 0.15
            return 0.0

    def record_shadow_probe(
        self,
        strategy_key: str,
        *,
        pnl: float,
        score: int,
    ) -> None:
        with self._lock:
            st = self._stats.setdefault(strategy_key, StrategyStats())
            st.shadow_trades += 1
            st.shadow_total_profit += float(pnl)
            if score > 0:
                st.shadow_wins += 1
            elif score < 0:
                st.shadow_losses += 1
            if st.lane_stage == "candidate":
                st.lane_stage = "shadow"
            shadow_wr = (st.shadow_wins / float(st.shadow_trades)) if st.shadow_trades else 0.0
            if st.shadow_trades >= 2 and shadow_wr >= 0.6 and st.shadow_total_profit > 0:
                st.pending_recommendation = "promote_from_shadow"
            elif st.shadow_trades >= 2 and st.shadow_losses >= st.shadow_wins + 1 and st.shadow_total_profit < 0:
                st.pending_recommendation = "quarantine_shadow"
            self._persist_unlocked()

    def sync_skill_feedback(
        self,
        strategy_key: str,
        *,
        risk_adjusted_score: float,
        trades_seen: int,
        win_rate: float,
    ) -> None:
        with self._lock:
            st = self._stats.setdefault(strategy_key, StrategyStats())
            if trades_seen >= 2 and win_rate >= 0.6 and risk_adjusted_score > 0.15:
                if st.lane_stage == "candidate":
                    st.lane_stage = "shadow"
                st.pending_recommendation = "probation_boost"
            elif trades_seen >= 2 and win_rate <= 0.35 and risk_adjusted_score < -0.2:
                st.pending_recommendation = "quarantine"
                if trades_seen >= 4 or st.shadow_losses >= 2:
                    st.lane_stage = "retired"
            self._persist_unlocked()

    def hydrate_from_closed_trades(self, rows: list[Dict[str, Any]]) -> None:
        """Rebuild stats from structured memory rows (features, setup_tag, score, pnl)."""
        with self._lock:
            self._stats.clear()
            for r in rows:
                feat = r.get("features")
                if not isinstance(feat, dict):
                    continue
                tag = str(r.get("setup_tag") or "trend_follow")
                key = build_strategy_key(feat, tag)
                st = self._stats.setdefault(key, StrategyStats())
                st.trades += 1
                st.total_profit += float(r.get("pnl") or 0.0)
                sc = int(r.get("score", 0))
                if sc > 0:
                    st.wins += 1
                elif sc < 0:
                    st.losses += 1
            for st in self._stats.values():
                self._refresh_score_and_active(st)
            self._persist_unlocked()
        log.info("StrategyRegistry hydrated from %s historical rows → %s keys", len(rows), len(self._stats))

    def set_lane_stage(self, strategy_key: str, stage: str) -> None:
        stage_norm = str(stage or "").strip().lower()
        if stage_norm not in {"candidate", "shadow", "live", "retired"}:
            raise ValueError(f"Unsupported lane stage: {stage}")
        with self._lock:
            st = self._stats.setdefault(strategy_key, StrategyStats())
            st.lane_stage = stage_norm
            self._persist_unlocked()

    def set_pending_recommendation(self, strategy_key: str, recommendation: str) -> None:
        rec = str(recommendation or "").strip()
        with self._lock:
            st = self._stats.setdefault(strategy_key, StrategyStats())
            st.pending_recommendation = rec
            self._persist_unlocked()

    def sync_promotion_hints(self, hints: List[Dict[str, Any]]) -> None:
        hint_map = {
            str(item.get("strategy_key") or item.get("name") or ""): str(item.get("recommendation") or "")
            for item in hints
            if str(item.get("strategy_key") or item.get("name") or "")
        }
        with self._lock:
            for key, st in self._stats.items():
                st.pending_recommendation = hint_map.get(key, "")
            self._persist_unlocked()

    def promotion_snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = []
            for key, st in self._stats.items():
                rows.append(
                    {
                        "strategy_key": key,
                        "lane_stage": st.lane_stage,
                        "pending_recommendation": st.pending_recommendation,
                        "trades": st.trades,
                        "wins": st.wins,
                        "losses": st.losses,
                        "total_profit": st.total_profit,
                        "score": st.score,
                        "ranking_score": st.ranking_score,
                        "active": st.active,
                    }
                )
        rows.sort(key=lambda item: (-float(item["ranking_score"]), item["strategy_key"]))
        return rows
