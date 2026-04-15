"""
Trade Memory System — adapted from hermes-agent's memory_manager.py.

Stores and recalls trade patterns, lessons, and market contexts.
Enables the trading agent to learn from past decisions and improve
over time.

Two memory providers:
  1. PatternMemory - trade setup patterns and their outcomes
  2. LessonMemory - mistakes, insights, and learned heuristics
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("mempalace2.memory")


class MemoryItem:
    """A single memory item with content, metadata, and relevance scoring."""

    def __init__(self, content: str, memory_type: str = "general",
                 metadata: Dict = None, relevance: float = 0.0,
                 created_at: float = None):
        self.content = content
        self.memory_type = memory_type
        self.metadata = metadata or {}
        self.relevance = relevance
        self.created_at = created_at or time.time()

    def to_dict(self) -> Dict:
        return {
            "content": self.content,
            "memory_type": self.memory_type,
            "metadata": self.metadata,
            "relevance": self.relevance,
            "created_at": self.created_at,
        }

    def __repr__(self):
        return f"MemoryItem(type={self.memory_type}, relevance={self.relevance:.2f}, content={self.content[:60]}...)"


class TradeMemory:
    """
    Trade memory system combining pattern recall and lesson learning.

    Inspired by hermes-agent's MemoryManager but specialized for trading:
    - Pattern memory: stores setup patterns with win rates
    - Lesson memory: stores mistakes and insights as heuristics
    - Context memory: stores market context snapshots for similarity matching
    """

    def __init__(self, state_store=None):
        self.store = state_store
        self._pattern_cache: List[Dict] = []
        self._lesson_cache: List[Dict] = []
        self._cache_ts: float = 0
        self._cache_ttl: float = 300  # 5 min cache

    def _refresh_cache(self):
        """Refresh memory caches from state store."""
        now = time.time()
        if now - self._cache_ts < self._cache_ttl:
            return
        if self.store:
            self._pattern_cache = self.store.get_best_patterns(limit=50)
            self._lesson_cache = self.store.get_learning_events(limit=50)
        self._cache_ts = now

    # ── Pattern Memory ──────────────────────────────────

    def store_trade_pattern(self, symbol: str, setup_type: str, direction: str,
                            conditions: Dict, outcome: Dict):
        """Store a trade pattern with its outcome for future reference."""
        if not self.store:
            return

        pattern = {
            "pattern_name": f"{symbol}_{setup_type}_{direction}",
            "pattern_type": setup_type,
            "symbol": symbol,
            "timeframe": conditions.get("timeframe", "1h"),
            "conditions": conditions,
            "win_rate": 1.0 if outcome.get("pnl", 0) > 0 else 0.0,
            "avg_pnl_pct": outcome.get("pnl_pct", 0),
            "avg_risk_reward": outcome.get("risk_reward_ratio", 0),
        }
        self.store.record_pattern(pattern)
        self._cache_ts = 0  # Invalidate cache

    def recall_similar_patterns(self, symbol: str, setup_type: str,
                                conditions: Dict = None) -> List[MemoryItem]:
        """Recall patterns similar to the current setup."""
        self._refresh_cache()
        items = []

        for p in self._pattern_cache:
            score = 0.0
            # Symbol match
            if p.get("symbol") == symbol:
                score += 0.4
            # Setup type match
            if p.get("pattern_type") == setup_type:
                score += 0.3
            # Sample size confidence
            samples = p.get("sample_count", 0)
            if samples >= 10:
                score += 0.2
            elif samples >= 5:
                score += 0.1
            # Win rate bonus
            wr = p.get("win_rate", 0)
            if wr > 0.6:
                score += 0.1

            if score > 0.3:
                items.append(MemoryItem(
                    content=f"Pattern: {p.get('pattern_name')} | Win Rate: {wr:.1%} | "
                            f"Avg P&L: {p.get('avg_pnl_pct', 0):.2f}% | "
                            f"Samples: {samples} | Avg R:R: {p.get('avg_risk_reward', 0):.2f}",
                    memory_type="pattern",
                    metadata=p,
                    relevance=score,
                ))

        items.sort(key=lambda x: x.relevance, reverse=True)
        return items[:5]

    # ── Lesson Memory ───────────────────────────────────

    def store_lesson(self, event_type: str, trade_id: str = None,
                     description: str = "", lesson: str = "",
                     metadata: Dict = None):
        """Store a learned lesson (mistake, insight, heuristic)."""
        if not self.store:
            return
        self.store.record_learning_event(
            event_type=event_type, trade_id=trade_id,
            description=description, lesson=lesson, metadata=metadata
        )
        self._cache_ts = 0

    def recall_lessons(self, context: str = None, event_type: str = None) -> List[MemoryItem]:
        """Recall relevant lessons based on context."""
        self._refresh_cache()
        items = []

        for e in self._lesson_cache:
            score = 0.0
            # Type relevance
            if event_type and e.get("event_type") == event_type:
                score += 0.5
            elif not event_type:
                score += 0.2
            # Recency (decay over 30 days)
            age_days = (time.time() - e.get("created_at", 0)) / 86400
            recency = max(0, 1 - age_days / 30)
            score += recency * 0.3
            # Context keyword matching
            if context:
                desc = (e.get("description", "") + " " + e.get("lesson", "")).lower()
                keywords = context.lower().split()
                matches = sum(1 for k in keywords if k in desc)
                score += min(0.2, matches * 0.05)

            if score > 0.1:
                items.append(MemoryItem(
                    content=f"[{e.get('event_type', 'lesson')}] {e.get('lesson', e.get('description', ''))}",
                    memory_type="lesson",
                    metadata=e,
                    relevance=score,
                ))

        items.sort(key=lambda x: x.relevance, reverse=True)
        return items[:10]

    # ── Trade Context Building ──────────────────────────

    def build_context_for_analysis(self, symbol: str, setup_type: str,
                                    direction: str) -> str:
        """
        Build a context block for the analyst agent, combining
        relevant patterns and lessons.

        Similar to hermes-agent's memory-context fencing.
        """
        patterns = self.recall_similar_patterns(symbol, setup_type)
        lessons = self.recall_lessons(context=f"{symbol} {setup_type} {direction}")

        if not patterns and not lessons:
            return ""

        sections = []
        sections.append("<trade-memory>")
        sections.append("[System note: The following is recalled trading memory, "
                        "NOT new market data. Use to inform but not override analysis.]")

        if patterns:
            sections.append("\n## Relevant Patterns (historical)")
            for p in patterns:
                sections.append(f"  • {p.content}")

        if lessons:
            sections.append("\n## Relevant Lessons")
            for l in lessons:
                sections.append(f"  • {l.content}")

        sections.append("</trade-memory>")
        return "\n".join(sections)

    def build_system_prompt_block(self) -> str:
        """Build memory section for the system prompt (persistent context)."""
        self._refresh_cache()
        if not self._pattern_cache and not self._lesson_cache:
            return ""

        parts = []
        parts.append("## Trading Memory (learned from experience)")

        # Top patterns
        top_patterns = [p for p in self._pattern_cache if p.get("win_rate", 0) > 0.55]
        if top_patterns:
            parts.append("\n### High-Win-Rate Patterns")
            for p in top_patterns[:5]:
                parts.append(
                    f"- {p.get('pattern_name')}: {p.get('win_rate', 0):.0%} win rate, "
                    f"{p.get('sample_count', 0)} trades, avg R:R {p.get('avg_risk_reward', 0):.1f}"
                )

        # Recent lessons
        recent_lessons = self._lesson_cache[:3]
        if recent_lessons:
            parts.append("\n### Recent Lessons")
            for e in recent_lessons:
                parts.append(f"- [{e.get('event_type')}] {e.get('lesson', e.get('description', ''))}")

        return "\n".join(parts)

    # ── Analytics ───────────────────────────────────────

    def get_memory_stats(self) -> Dict:
        """Get memory system statistics."""
        self._refresh_cache()
        return {
            "patterns_stored": len(self._pattern_cache),
            "lessons_stored": len(self._lesson_cache),
            "high_confidence_patterns": len(
                [p for p in self._pattern_cache
                 if p.get("win_rate", 0) > 0.6 and p.get("sample_count", 0) >= 5]
            ),
            "cache_age_seconds": time.time() - self._cache_ts,
        }
