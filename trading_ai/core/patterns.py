from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)

# Volatility label in key must match feature extraction: LOW / MEDIUM / HIGH
SETUP_TAGS = ("trend_follow", "reversal", "breakout")


def pattern_key(
    trend: str,
    volatility: str,
    session: str,
    setup_tag: str,
) -> str:
    t = str(trend or "RANGE").upper()
    v = str(volatility or "MEDIUM").upper()
    s = str(session or "ASIA").upper()
    tag = str(setup_tag or "trend_follow").lower()
    return f"{t}_{v}_{s}_{tag}"


def pattern_key_from_features(features: Dict[str, Any], setup_tag: str) -> str:
    return pattern_key(
        str(features.get("trend_direction") or "RANGE"),
        str(features.get("volatility") or "MEDIUM"),
        str(features.get("session") or "ASIA"),
        setup_tag,
    )


def _row_key_fields(r: Dict[str, Any]) -> Tuple[str, str, str, str]:
    feat = r.get("features")
    if isinstance(feat, dict):
        return (
            str(feat.get("trend_direction") or "RANGE"),
            str(feat.get("volatility") or "MEDIUM"),
            str(feat.get("session") or "ASIA"),
            str(r.get("setup_tag") or "trend_follow"),
        )
    return (
        str(r.get("trend_direction") or "RANGE"),
        str(r.get("volatility") or "MEDIUM"),
        str(r.get("session") or "ASIA"),
        str(r.get("setup_tag") or "trend_follow"),
    )


def extract_winning_patterns(
    memory_records: Iterable[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Discover pattern buckets that contain ≥1 winning trade (score > 0).

    For each such bucket, stats use **all** trades in the bucket:
    win_rate = (# wins) / count, avg_profit = mean pnl over **winning** rows only,
    count = total closed trades in bucket.
    """
    by_key: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in memory_records:
        t, v, sess, tag = _row_key_fields(r)
        key = pattern_key(t, v, sess, tag)
        by_key[key].append(r)

    out: Dict[str, Dict[str, Any]] = {}
    for key, items in by_key.items():
        wins = [x for x in items if int(x.get("score", 0)) > 0]
        if not wins:
            continue
        total = len(items)
        win_rate = len(wins) / float(total) if total else 0.0
        avg_profit = sum(float(x.get("pnl") or 0.0) for x in wins) / float(len(wins))
        out[key] = {
            "win_rate": float(win_rate),
            "avg_profit": float(avg_profit),
            "count": int(total),
        }
    return out


@dataclass(slots=True)
class PatternScoreResult:
    confidence_boost: float
    success_probability: float
    matched_key: str
    matched: bool


def score_pattern(
    features: Dict[str, Any],
    patterns: Mapping[str, Dict[str, Any]],
    setup_tag: str,
) -> PatternScoreResult:
    """
    Map current features + intended setup_tag to a pattern bucket.
    success_probability ≈ historical win_rate (0.5 if unknown).
    confidence_boost scales slightly with edge over 50%.
    """
    key = pattern_key_from_features(features, setup_tag)
    stat = patterns.get(key)
    if not stat:
        return PatternScoreResult(
            confidence_boost=0.0,
            success_probability=0.5,
            matched_key=key,
            matched=False,
        )
    wr = float(stat.get("win_rate", 0.5))
    boost = max(0.0, min(0.12, (wr - 0.5) * 0.35))
    return PatternScoreResult(
        confidence_boost=float(boost),
        success_probability=float(wr),
        matched_key=key,
        matched=True,
    )


def build_pattern_analysis_for_prompt(
    features: Dict[str, Any],
    patterns: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    """Structured block for the LLM: summary + per-tag historical stats."""
    per_tag: Dict[str, Any] = {}
    best: Optional[Dict[str, Any]] = None
    for tag in SETUP_TAGS:
        key = pattern_key_from_features(features, tag)
        st = patterns.get(key)
        if st:
            row = {
                "matched_pattern": key,
                "win_rate": round(st["win_rate"], 4),
                "sample_size": int(st["count"]),
                "avg_profit": round(st.get("avg_profit", 0.0), 6),
            }
            per_tag[tag] = row
            if best is None or st["win_rate"] > float(best["win_rate"]):
                best = dict(row)
        else:
            per_tag[tag] = {
                "matched_pattern": key,
                "win_rate": None,
                "sample_size": 0,
                "avg_profit": None,
            }
    summary = best or {
        "matched_pattern": pattern_key_from_features(features, "trend_follow"),
        "win_rate": None,
        "sample_size": 0,
        "avg_profit": None,
    }
    return {
        "matched_pattern": summary["matched_pattern"],
        "win_rate": summary.get("win_rate"),
        "sample_size": summary.get("sample_size") or 0,
        "avg_profit": summary.get("avg_profit"),
        "per_setup_tag": per_tag,
    }


def passes_pattern_execution_gate(
    features: Dict[str, Any],
    patterns: Mapping[str, Dict[str, Any]],
    setup_tag: str,
    *,
    min_win_rate: float,
    min_sample_size: int,
    strict_unknown: bool = False,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    key = pattern_key_from_features(features, setup_tag)
    st = patterns.get(key)
    if not st:
        if strict_unknown:
            return False, f"pattern_unknown:{key}", None
        return True, "pattern_unknown_bootstrap_ok", None
    wr = float(st["win_rate"])
    n = int(st["count"])
    if n < min_sample_size:
        return False, f"pattern_low_sample:{n}<{min_sample_size}", st
    if wr < min_win_rate:
        return False, f"pattern_low_win_rate:{wr:.3f}<{min_win_rate}", st
    return True, "ok", st


def apply_pattern_confidence_boost(
    confidence: float,
    stats: Dict[str, Any],
    *,
    boost_min_win_rate: float,
    boost_min_sample: int,
    delta: float,
    cap: float = 0.95,
) -> Tuple[float, bool]:
    wr = float(stats.get("win_rate", 0.0))
    n = int(stats.get("count", 0))
    if wr > boost_min_win_rate and n > boost_min_sample:
        return min(cap, confidence + delta), True
    return confidence, False


@dataclass
class PatternBook:
    """
    Incremental pattern stats (mirrors closed-trade memory). Refresh from Chroma on hydrate.
    """

    _rows: List[Dict[str, Any]] = field(default_factory=list)

    def hydrate_from_rows(self, rows: Iterable[Dict[str, Any]]) -> None:
        self._rows = list(rows)
        log.info("PatternBook hydrated: %s closed-trade rows", len(self._rows))

    def append_closed_trade(
        self,
        *,
        features: Dict[str, Any],
        setup_tag: str,
        score: int,
        pnl: float,
    ) -> None:
        self._rows.append(
            {
                "features": dict(features),
                "setup_tag": setup_tag,
                "score": int(score),
                "pnl": float(pnl),
            }
        )

    def patterns_dict(self) -> Dict[str, Dict[str, Any]]:
        return extract_winning_patterns(self._rows)

    def export_rows(self) -> List[Dict[str, Any]]:
        return list(self._rows)


def parse_memory_document_to_row(doc_json: str, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = json.loads(doc_json)
    except json.JSONDecodeError:
        return None
    feat = body.get("features")
    if not isinstance(feat, dict):
        return None
    res = body.get("result") or {}
    pnl = res.get("pnl")
    if pnl is None:
        pnl = metadata.get("pnl")
    try:
        pnl_f = float(pnl) if pnl is not None else 0.0
    except (TypeError, ValueError):
        pnl_f = 0.0
    score = body.get("score", metadata.get("outcome_score", 0))
    try:
        score_i = int(float(score))
    except (TypeError, ValueError):
        score_i = 0
    tag = str(body.get("setup_tag") or metadata.get("setup_tag") or "trend_follow")
    return {
        "features": feat,
        "setup_tag": tag,
        "score": score_i,
        "pnl": pnl_f,
    }
