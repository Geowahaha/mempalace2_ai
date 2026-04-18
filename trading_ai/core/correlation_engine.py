from __future__ import annotations

import json
import math
import threading
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


def _pair_key(a: str, b: str) -> str:
    x, y = sorted((str(a), str(b)))
    return f"{x}|||{y}"


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def compute_correlation(xs: Sequence[float], ys: Sequence[float], *, min_samples: int = 10) -> float:
    """Pearson r; not enough data or zero variance → 0."""
    n = min(len(xs), len(ys))
    if n < min_samples:
        return 0.0
    a = [float(xs[i]) for i in range(-n, 0)]
    b = [float(ys[i]) for i in range(-n, 0)]
    n = len(a)
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    var_a = sum((x - mean_a) ** 2 for x in a)
    var_b = sum((y - mean_b) ** 2 for y in b)
    if var_a < 1e-18 or var_b < 1e-18:
        return 0.0
    cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n))
    r = cov / math.sqrt(var_a * var_b)
    if math.isnan(r) or not math.isfinite(r):
        return 0.0
    return max(-1.0, min(1.0, float(r)))


class CorrelationEngine:
    """
    Rolling per-strategy PnL series → pairwise correlation, penalty / diversity for fusion.
    Async-safe: mutations serialized with a lock (call from async loop — one thread).
    """

    def __init__(
        self,
        persist_path: Path,
        *,
        max_len: int = 100,
        min_samples_matrix: int = 10,
        penalty_mid_threshold: float = 0.8,
        penalty_high_threshold: float = 0.9,
        penalty_mid: float = 0.3,
        penalty_high: float = 0.5,
        max_penalty: float = 0.7,
        diversity_threshold: float = 0.2,
        diversity_bonus: float = 0.1,
    ) -> None:
        self._path = Path(persist_path)
        self._max_len = max(1, int(max_len))
        self._min_samples = max(2, int(min_samples_matrix))
        self._pen_mid_t = float(penalty_mid_threshold)
        self._pen_hi_t = float(penalty_high_threshold)
        self._pen_mid = float(penalty_mid)
        self._pen_hi = float(penalty_high)
        self._max_pen = float(max_penalty)
        self._div_t = float(diversity_threshold)
        self._div_bonus = float(diversity_bonus)
        self._series: Dict[str, List[float]] = {}
        self._lock = threading.Lock()
        self._cycle_matrix: Optional[Dict[str, float]] = None
        self._load()

    def _load(self) -> None:
        if not self._path.is_file():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("CorrelationEngine: load failed %s: %s", self._path, exc)
            return
        if not isinstance(raw, dict):
            return
        for k, v in raw.items():
            if not isinstance(v, list):
                continue
            try:
                self._series[str(k)] = [float(x) for x in v[-self._max_len :]]
            except (TypeError, ValueError):
                continue
        log.info("CorrelationEngine: loaded %s strategy series from %s", len(self._series), self._path)

    def start_cycle(self) -> None:
        """Invalidate correlation matrix cache (call once per main loop iteration)."""
        self._cycle_matrix = None

    def update_pnl(self, strategy_key: str, pnl: float) -> None:
        key = str(strategy_key or "").strip()
        if not key:
            return
        p = float(pnl)
        with self._lock:
            seq = self._series.setdefault(key, [])
            seq.append(p)
            if len(seq) > self._max_len:
                del seq[: len(seq) - self._max_len]
            self._persist_unlocked()
        self._cycle_matrix = None

    def _persist_unlocked(self) -> None:
        out = {k: list(v) for k, v in sorted(self._series.items())}
        _atomic_write_json(self._path, out)

    def series_snapshot(self) -> Dict[str, List[float]]:
        with self._lock:
            return {k: list(v) for k, v in self._series.items()}

    def get_correlation(self, key_a: str, key_b: str) -> float:
        if key_a == key_b:
            return 1.0
        with self._lock:
            sa = list(self._series.get(key_a, []))
            sb = list(self._series.get(key_b, []))
        return compute_correlation(sa, sb, min_samples=self._min_samples)

    def build_correlation_matrix(self) -> Dict[str, float]:
        """Pairwise Pearson for strategies with ≥ min_samples; keys `a|||b` sorted."""
        with self._lock:
            keys = [k for k, v in self._series.items() if len(v) >= self._min_samples]
            snap = {k: list(self._series[k]) for k in keys}
        out: Dict[str, float] = {}
        for i, ka in enumerate(keys):
            for kb in keys[i + 1 :]:
                r = compute_correlation(snap[ka], snap[kb], min_samples=self._min_samples)
                out[_pair_key(ka, kb)] = r
        return out

    def get_correlation_matrix_cached(self) -> Dict[str, float]:
        if self._cycle_matrix is None:
            self._cycle_matrix = self.build_correlation_matrix()
        return self._cycle_matrix

    def get_correlation_penalty(
        self,
        strategy_key: str,
        active_strategies: Sequence[str],
        *,
        matrix: Optional[Mapping[str, float]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Sum penalties for positive correlation with active peers; negative r → no penalty.
        Clamped to max_penalty.
        """
        sk = str(strategy_key or "").strip()
        meta: Dict[str, Any] = {"pairs": []}
        if not sk:
            return 0.0, meta

        mat = dict(matrix) if matrix is not None else self.get_correlation_matrix_cached()
        others = [s for s in active_strategies if s and s != sk]
        if not others:
            return 0.0, meta

        penalty = 0.0
        with self._lock:
            my_n = len(self._series.get(sk, []))

        for ob in others:
            with self._lock:
                ob_n = len(self._series.get(ob, []))
            if my_n < self._min_samples or ob_n < self._min_samples:
                continue
            pk = _pair_key(sk, ob)
            r = mat.get(pk)
            if r is None:
                r = self.get_correlation(sk, ob)
            if r <= self._pen_mid_t:
                continue
            contrib = 0.0
            if r > self._pen_hi_t:
                contrib = self._pen_hi
            elif r > self._pen_mid_t:
                contrib = self._pen_mid
            penalty += contrib
            meta["pairs"].append({"peer": ob, "r": round(r, 4), "penalty": contrib})

        penalty = max(0.0, min(self._max_pen, penalty))
        meta["total"] = round(penalty, 4)
        return penalty, meta

    def get_diversity_bonus(
        self,
        strategy_key: str,
        active_strategies: Sequence[str],
        *,
        matrix: Optional[Mapping[str, float]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """+diversity_bonus if r < threshold vs every comparable peer; else 0."""
        sk = str(strategy_key or "").strip()
        diag: Dict[str, Any] = {"eligible_peers": 0, "ok": True}
        if not sk:
            return 0.0, diag

        mat = dict(matrix) if matrix is not None else self.get_correlation_matrix_cached()
        others = [s for s in active_strategies if s and s != sk]
        with self._lock:
            my_n = len(self._series.get(sk, []))

        eligible = [ob for ob in others if my_n >= self._min_samples and len(self._series.get(ob, [])) >= self._min_samples]
        if not eligible:
            diag["ok"] = False
            diag["reason"] = "no_eligible_peers"
            return 0.0, diag

        for ob in eligible:
            pk = _pair_key(sk, ob)
            r = mat.get(pk)
            if r is None:
                r = self.get_correlation(sk, ob)
            diag["eligible_peers"] += 1
            if r >= self._div_t:
                diag["ok"] = False
                diag["failed_peer"] = ob
                diag["failed_r"] = round(r, 4)
                return 0.0, diag

        return self._div_bonus, diag

    def top_correlation_pairs(
        self,
        matrix: Mapping[str, float],
        *,
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        rows: List[Tuple[str, float]] = sorted(matrix.items(), key=lambda x: abs(x[1]), reverse=True)
        out: List[Dict[str, Any]] = []
        for pk, r in rows[:limit]:
            parts = pk.split("|||", 1)
            if len(parts) == 2:
                out.append({"a": parts[0], "b": parts[1], "r": round(r, 4)})
            else:
                out.append({"pair": pk, "r": round(r, 4)})
        return out


def active_strategy_keys_from_registry(
    snapshot: Mapping[str, Mapping[str, Any]],
    *,
    min_trades: int = 1,
) -> List[str]:
    """Keys that are active in StrategyRegistry and have enough history to matter."""
    out: List[str] = []
    for k, st in snapshot.items():
        if not isinstance(st, dict):
            continue
        if not bool(st.get("active", True)):
            continue
        if int(st.get("trades", 0) or 0) < min_trades:
            continue
        out.append(str(k))
    return sorted(out)


def apply_correlation_to_fusion_weight(
    base_weight: float,
    *,
    penalty: float,
    diversity_bonus: float,
) -> float:
    """final_weight = base * (1 - penalty) + bonus, clamp [0, 1]."""
    b = max(0.0, min(1.0, float(base_weight)))
    p = max(0.0, min(1.0, float(penalty)))
    bon = max(0.0, float(diversity_bonus))
    w = b * (1.0 - p) + bon
    return max(0.0, min(1.0, w))
