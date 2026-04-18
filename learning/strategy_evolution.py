"""
learning/strategy_evolution.py

Strategy Evolution Log — structured, machine-readable tracking of all system improvements.
Each entry records: what changed, metrics before/after, and whether it helped.

Stored in data/reports/strategy_evolution_log.json (via report_store pattern).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_LOG_PATH = Path(__file__).resolve().parent.parent / "data" / "reports" / "strategy_evolution_log.json"
_MAX_ENTRIES = 500


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime] = None) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# 1. Log entry structure
# ---------------------------------------------------------------------------

def make_evolution_entry(
    *,
    change_type: str,
    description: str,
    component: str = "",
    metric_before: Optional[dict] = None,
    metric_after: Optional[dict] = None,
    impact: str = "unknown",
    auto: bool = False,
    source: str = "",
    metadata: Optional[dict] = None,
) -> dict:
    """Create a standardized evolution log entry.

    Args:
        change_type: Category — 'weight_calibration' | 'threshold_tune' | 'family_decay_action' |
                     'config_change' | 'feature_added' | 'bug_fix' | 'guard_tune' | 'strategy_added'
        description: Human-readable description of the change
        component: File or module affected (e.g., 'analysis/entry_sharpness.py')
        metric_before: Metrics before the change {win_rate, pnl_usd, trades, ...}
        metric_after: Metrics after the change (filled later if auto-tracked)
        impact: Assessment — 'positive' | 'negative' | 'neutral' | 'unknown' | 'pending'
        auto: True if change was auto-applied (not human-initiated)
        source: What triggered the change (e.g., 'sharpness_feedback', 'conductor', 'manual')
        metadata: Any extra context
    """
    return {
        "timestamp": _iso(),
        "change_type": str(change_type or "").strip(),
        "description": str(description or "").strip(),
        "component": str(component or "").strip(),
        "metric_before": dict(metric_before or {}),
        "metric_after": dict(metric_after or {}),
        "impact": str(impact or "unknown").strip(),
        "auto": bool(auto),
        "source": str(source or "").strip(),
        "metadata": dict(metadata or {}),
    }


# ---------------------------------------------------------------------------
# 2. Persistence (JSON file)
# ---------------------------------------------------------------------------

def _load_log() -> list[dict]:
    """Load existing evolution log from disk."""
    try:
        if _LOG_PATH.exists():
            with open(_LOG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return list(data.get("entries") or [])
    except Exception as e:
        logger.debug("[StrategyEvolution] load error: %s", e)
    return []


def _save_log(entries: list[dict]) -> bool:
    """Persist evolution log to disk."""
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Keep only latest _MAX_ENTRIES
        trimmed = entries[-_MAX_ENTRIES:]
        with open(_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump({"entries": trimmed, "updated_utc": _iso(), "count": len(trimmed)}, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error("[StrategyEvolution] save error: %s", e)
        return False


def log_evolution(entry: dict) -> bool:
    """Append an evolution entry to the log. Returns True on success."""
    entries = _load_log()
    entries.append(entry)
    return _save_log(entries)


def log_change(
    *,
    change_type: str,
    description: str,
    component: str = "",
    metric_before: Optional[dict] = None,
    metric_after: Optional[dict] = None,
    impact: str = "unknown",
    auto: bool = False,
    source: str = "",
    metadata: Optional[dict] = None,
) -> bool:
    """Convenience: create entry + log it in one call."""
    entry = make_evolution_entry(
        change_type=change_type,
        description=description,
        component=component,
        metric_before=metric_before,
        metric_after=metric_after,
        impact=impact,
        auto=auto,
        source=source,
        metadata=metadata,
    )
    return log_evolution(entry)


# ---------------------------------------------------------------------------
# 3. Query / analysis
# ---------------------------------------------------------------------------

def get_recent_entries(
    *,
    n: int = 20,
    change_type: Optional[str] = None,
    component: Optional[str] = None,
    since: Optional[str] = None,
) -> list[dict]:
    """Get recent evolution entries with optional filters."""
    entries = _load_log()

    if change_type:
        entries = [e for e in entries if e.get("change_type") == change_type]
    if component:
        entries = [e for e in entries if component in str(e.get("component", ""))]
    if since:
        entries = [e for e in entries if str(e.get("timestamp", "")) >= since]

    return entries[-max(1, n):]


def compute_evolution_stats() -> dict:
    """Compute summary statistics from the evolution log.

    Returns {
        total_entries, auto_changes, manual_changes,
        by_type: {type: count},
        by_impact: {impact: count},
        recent_trend: str,   # 'improving' | 'degrading' | 'stable' | 'unknown'
    }
    """
    entries = _load_log()
    if not entries:
        return {
            "total_entries": 0,
            "auto_changes": 0,
            "manual_changes": 0,
            "by_type": {},
            "by_impact": {},
            "recent_trend": "unknown",
        }

    by_type: dict[str, int] = {}
    by_impact: dict[str, int] = {}
    auto_count = 0
    for e in entries:
        ct = str(e.get("change_type", "unknown") or "unknown")
        by_type[ct] = by_type.get(ct, 0) + 1
        imp = str(e.get("impact", "unknown") or "unknown")
        by_impact[imp] = by_impact.get(imp, 0) + 1
        if bool(e.get("auto")):
            auto_count += 1

    # Recent trend from last 10 entries
    recent = entries[-10:]
    pos = sum(1 for e in recent if e.get("impact") == "positive")
    neg = sum(1 for e in recent if e.get("impact") == "negative")
    if pos > neg + 2:
        trend = "improving"
    elif neg > pos + 2:
        trend = "degrading"
    elif pos + neg == 0:
        trend = "unknown"
    else:
        trend = "stable"

    return {
        "total_entries": len(entries),
        "auto_changes": auto_count,
        "manual_changes": len(entries) - auto_count,
        "by_type": by_type,
        "by_impact": by_impact,
        "recent_trend": trend,
    }


def update_entry_impact(
    timestamp: str,
    impact: str,
    metric_after: Optional[dict] = None,
) -> bool:
    """Update the impact assessment of an existing entry by timestamp.

    Used when we can now measure the outcome of a change that was initially 'pending'.
    """
    entries = _load_log()
    updated = False
    for entry in entries:
        if entry.get("timestamp") == timestamp:
            entry["impact"] = str(impact or "unknown")
            if metric_after:
                entry["metric_after"] = dict(metric_after)
            updated = True
            break
    if updated:
        return _save_log(entries)
    return False


# ---------------------------------------------------------------------------
# 4. Telegram-friendly formatting
# ---------------------------------------------------------------------------

def format_evolution_summary(n: int = 5) -> str:
    """Format recent evolution entries for Telegram notification."""
    stats = compute_evolution_stats()
    entries = get_recent_entries(n=n)

    lines = ["\U0001f4c8 Strategy Evolution"]
    lines.append(
        f"Total: {stats['total_entries']} changes "
        f"({stats['auto_changes']} auto, {stats['manual_changes']} manual) "
        f"| Trend: {stats['recent_trend']}"
    )

    if not entries:
        lines.append("No recent entries.")
        return "\n".join(lines)

    by_impact = stats.get("by_impact") or {}
    if by_impact:
        parts = [f"{k}:{v}" for k, v in sorted(by_impact.items())]
        lines.append("Impact: " + " | ".join(parts))

    lines.append("")
    for e in entries[-n:]:
        ts = str(e.get("timestamp", "")[:10] or "")
        ct = str(e.get("change_type", "") or "")
        desc = str(e.get("description", "") or "")[:60]
        impact = str(e.get("impact", "") or "")
        auto_tag = " [auto]" if bool(e.get("auto")) else ""
        lines.append(f"{ts} {ct}: {desc} ({impact}){auto_tag}")

    return "\n".join(lines)
