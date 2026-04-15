"""
Trading Skill Manager — adapted from hermes-agent's skills discovery pattern.

Discovers, loads, and manages trading skills. Implements the self-learning
loop: auto-creates skills from successful trades and improves existing
skills with updated win rates after each trade.

Key hermes-agent concepts adapted:
  - discover_builtin_skills(): filesystem skill discovery
  - skill matching: score skills against market context
  - progressive improvement: update metadata after each trade
"""

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from skills.base import (
    SkillEntry,
    load_skill,
    save_skill_metadata,
    parse_frontmatter,
    DEFAULT_SKILLS_DIR,
)

logger = logging.getLogger("mempalace2.skills.manager")


class SkillManager:
    """
    Manages trading skills: discovery, matching, auto-creation, and improvement.

    Implements the hermes-agent self-learning loop:
      1. Discover skills from filesystem on boot
      2. Match skills to current market context
      3. After a trade closes, update the skill's stats
      4. If no skill matches a successful trade, create one
    """

    def __init__(self, skills_dir: Path = None, state_store=None):
        self.skills_dir = skills_dir or DEFAULT_SKILLS_DIR
        self.skills_dir.mkdir(parents=True, exist_ok=True)
        self.state_store = state_store
        self._skills: Dict[str, SkillEntry] = {}
        self._contents: Dict[str, str] = {}  # Full SKILL.md content cache

    # ── Discovery ───────────────────────────────────────

    def discover_skills(self) -> int:
        """
        Discover all trading skills from the skills directory.

        Scans for directories containing SKILL.md files.
        Returns the number of skills found.
        """
        self._skills.clear()
        self._contents.clear()

        if not self.skills_dir.exists():
            logger.info(f"Skills directory does not exist: {self.skills_dir}")
            return 0

        count = 0
        for skill_md in self.skills_dir.rglob("SKILL.md"):
            skill_dir = skill_md.parent
            entry, content = load_skill(skill_dir)
            if entry:
                self._skills[entry.name] = entry
                self._contents[entry.name] = content
                count += 1
                logger.debug(f"Discovered skill: {entry.name} (WR: {entry.win_rate:.0%})")

        logger.info(f"Discovered {count} trading skills")
        return count

    # ── Listing & Viewing ───────────────────────────────

    def list_skills(self, category: str = None,
                    min_win_rate: float = 0.0) -> List[SkillEntry]:
        """
        List discovered skills, optionally filtered.

        Args:
            category: Filter by category (e.g., "trading")
            min_win_rate: Minimum win rate filter
        """
        skills = list(self._skills.values())

        if category:
            skills = [s for s in skills if s.category == category]
        if min_win_rate > 0:
            skills = [s for s in skills if s.win_rate >= min_win_rate]

        # Sort by win rate descending, then by sample count
        skills.sort(key=lambda s: (s.win_rate, s.sample_count), reverse=True)
        return skills

    def view_skill(self, name: str) -> Optional[str]:
        """Get the full content of a skill by name."""
        return self._contents.get(name)

    def get_skill_entry(self, name: str) -> Optional[SkillEntry]:
        """Get skill metadata by name."""
        return self._skills.get(name)

    # ── Matching ────────────────────────────────────────

    def match_skills(self, context: Dict[str, Any],
                     min_score: float = 0.3) -> List[Tuple[SkillEntry, float]]:
        """
        Find skills that match the given market context.

        Args:
            context: Dict with keys like symbol, timeframe, direction,
                     setup_type, market_regime
            min_score: Minimum match score (0-1)

        Returns:
            List of (SkillEntry, score) sorted by score descending.
        """
        matches = []
        for entry in self._skills.values():
            score = entry.matches_conditions(context)
            if score >= min_score:
                matches.append((entry, score))

        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    # ── Self-Learning: Update After Trade ───────────────

    def update_skill_from_trade(self, skill_name: str, won: bool,
                                pnl_pct: float, risk_reward: float):
        """
        Update a skill's performance statistics after a trade.

        Exponential moving average for win rate to weight recent results.
        """
        entry = self._skills.get(skill_name)
        if not entry:
            logger.warning(f"Skill '{skill_name}' not found for update")
            return

        old_n = entry.sample_count
        new_n = old_n + 1

        # EMA-style update (alpha = 1/N for equal weighting)
        alpha = 1.0 / new_n
        entry.win_rate = (1 - alpha) * entry.win_rate + alpha * (1.0 if won else 0.0)
        entry.avg_pnl_pct = (1 - alpha) * entry.avg_pnl_pct + alpha * pnl_pct
        entry.avg_risk_reward = (1 - alpha) * entry.avg_risk_reward + alpha * risk_reward
        entry.sample_count = new_n
        entry.last_used = time.time()

        # Persist to SKILL.md frontmatter
        skill_dir = Path(entry.path)
        if skill_dir.exists():
            save_skill_metadata(skill_dir, entry)

        logger.info(
            f"Skill '{skill_name}' updated: WR={entry.win_rate:.1%}, "
            f"samples={new_n}, avg_RR={entry.avg_risk_reward:.2f}"
        )

    # ── Self-Learning: Auto-Create from Trade ───────────

    def create_skill_from_trade(self, trade_data: Dict[str, Any]) -> Optional[str]:
        """
        Auto-create a new skill from a successful trade when no existing
        skill matched.

        Args:
            trade_data: Dict with trade details (symbol, setup_type,
                        direction, indicators, reasoning, outcome)

        Returns:
            Name of the created skill, or None on failure.
        """
        symbol = trade_data.get("symbol", "UNKNOWN")
        setup_type = trade_data.get("setup_type", "custom")
        direction = trade_data.get("direction", "long")

        # Check if we already have a similar skill
        context = {
            "symbol": symbol,
            "setup_type": setup_type,
            "direction": direction,
        }
        existing = self.match_skills(context, min_score=0.7)
        if existing:
            logger.debug(f"Similar skill exists: {existing[0][0].name}, updating instead")
            return existing[0][0].name

        # Create new skill
        skill_name = f"{symbol.lower()}-{setup_type}-{direction}"
        # Sanitize
        skill_name = "".join(c if c.isalnum() or c == "-" else "-" for c in skill_name)
        skill_name = skill_name[:MAX_NAME_LENGTH].rstrip("-")

        skill_dir = self.skills_dir / skill_name
        skill_dir.mkdir(parents=True, exist_ok=True)

        indicators = trade_data.get("indicators", {})
        reasoning = trade_data.get("reasoning", "")
        outcome = trade_data.get("outcome", {})

        # Build SKILL.md content
        content = f"""---
name: {skill_name}
description: Auto-generated skill from {setup_type} setup on {symbol} ({direction})
version: 1.0.0
category: trading
tags: [auto-generated, {setup_type}, {symbol.lower()}]
conditions:
  symbols: [{symbol}]
  setup_types: [{setup_type}]
  direction: {direction}
win_rate: {1.0 if outcome.get('pnl', 0) > 0 else 0.0}
sample_count: 1
avg_pnl_pct: {outcome.get('pnl_pct', 0)}
avg_risk_reward: {outcome.get('risk_reward_ratio', 0)}
---

# {skill_name}

## Setup Type
{setup_type} on {symbol} ({direction})

## Conditions at Trade Entry
```json
{json.dumps(indicators, indent=2)}
```

## Entry Reasoning
{reasoning}

## Risk Parameters
- R:R Ratio: {outcome.get('risk_reward_ratio', 'N/A')}
- Position Size: {outcome.get('position_size_pct', 'N/A')}%

## Outcome
- P&L: {outcome.get('pnl', 'N/A')}
- P&L %: {outcome.get('pnl_pct', 'N/A')}%
- Status: {'WIN' if outcome.get('pnl', 0) > 0 else 'LOSS'}

## Improvement Notes
_Auto-generated skill. Update this section with refinements after more trades._
"""

        skill_md = skill_dir / "SKILL.md"
        skill_md.write_text(content, encoding="utf-8")

        # Load into manager
        entry, _ = load_skill(skill_dir)
        if entry:
            self._skills[entry.name] = entry
            self._contents[entry.name] = content

        logger.info(f"Created skill '{skill_name}' from trade on {symbol}")
        return skill_name

    # ── Context Block for Prompts ───────────────────────

    def build_skills_context_block(self, context: Dict[str, Any] = None,
                                    max_skills: int = 5) -> str:
        """
        Build a fenced context block of relevant skills for prompt injection.

        Similar to hermes-agent's skills index but context-filtered.
        """
        if context:
            matches = self.match_skills(context, min_score=0.3)
        else:
            # Return top skills by win rate
            matches = [(s, s.win_rate) for s in self.list_skills()]

        if not matches:
            return ""

        lines = []
        lines.append("<trading-skills>")
        lines.append("[System note: The following are learned trading skills. "
                     "Use them to inform your analysis but adapt to current conditions.]")

        for entry, score in matches[:max_skills]:
            status = "🟢" if entry.win_rate > 0.55 else "🟡" if entry.win_rate > 0.4 else "🔴"
            lines.append(
                f"\n  {status} **{entry.name}** (match: {score:.0%})\n"
                f"    Win Rate: {entry.win_rate:.0%} | "
                f"Samples: {entry.sample_count} | "
                f"Avg R:R: {entry.avg_risk_reward:.1f}\n"
                f"    {entry.description}"
            )

        lines.append("</trading-skills>")
        return "\n".join(lines)

    # ── Stats ───────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Get skill manager statistics."""
        skills = list(self._skills.values())
        if not skills:
            return {"total": 0, "avg_win_rate": 0, "high_confidence": 0}

        total_wr = sum(s.win_rate for s in skills)
        high_conf = [s for s in skills if s.win_rate > 0.6 and s.sample_count >= 5]

        return {
            "total": len(skills),
            "avg_win_rate": total_wr / len(skills),
            "high_confidence": len(high_conf),
            "skills": [s.to_dict() for s in skills],
        }
