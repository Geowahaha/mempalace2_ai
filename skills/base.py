"""
Trading Skill Framework — adapted from hermes-agent's tools/skills_tool.py.

Provides agentskills.io compatible SKILL.md format with YAML frontmatter
for encoding trading patterns as reusable, self-improving skills.

Each trading pattern becomes a skill with:
  - Metadata (name, description, conditions)
  - Win rate, sample count, avg P&L tracking
  - Progressive disclosure: metadata → full instructions → linked files

Key hermes-agent concepts adapted:
  - SKILL.md with YAML frontmatter
  - SkillEntry for metadata management
  - Frontmatter parsing for conditions and outcomes
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("mempalace2.skills")

# Max lengths for progressive disclosure efficiency (hermes pattern)
MAX_NAME_LENGTH = 64
MAX_DESCRIPTION_LENGTH = 1024

# Skill directory root
DEFAULT_SKILLS_DIR = Path.home() / ".mempalace2" / "skills"


def parse_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    """
    Parse YAML frontmatter from a SKILL.md file.

    Returns (frontmatter_dict, body_text).
    Falls back to empty frontmatter on parse errors.
    """
    if not content.startswith("---"):
        return {}, content

    end = content.find("\n---", 3)
    if end == -1:
        return {}, content

    fm_text = content[4:end].strip()
    body = content[end + 4:].lstrip("\n")

    # Simple YAML-like parsing (avoids requiring pyyaml at skill level)
    fm: Dict[str, Any] = {}
    current_key = None
    current_list: Optional[List] = None
    current_dict: Optional[Dict] = None
    indent_level = 0

    for line in fm_text.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        # Top-level key: value
        if not line.startswith(" "):
            if current_key and current_list is not None:
                fm[current_key] = current_list
            if current_key and current_dict is not None:
                fm[current_key] = current_dict
            current_list = None
            current_dict = None

            if ":" in stripped:
                key, _, value = stripped.partition(":")
                key = key.strip()
                value = value.strip().strip('"').strip("'")

                if value.startswith("[") and value.endswith("]"):
                    # Inline list
                    items = value[1:-1].split(",")
                    fm[key] = [i.strip().strip('"').strip("'") for i in items if i.strip()]
                elif value:
                    fm[key] = _auto_convert(value)
                else:
                    fm[key] = {}
                    current_key = key
                    current_dict = fm[key]
                    current_list = None
            continue

        # Indented list item
        if stripped.startswith("- "):
            if current_list is None:
                current_list = []
                if current_key:
                    fm[current_key] = current_list
            item = stripped[2:].strip().strip('"').strip("'")
            current_list.append(_auto_convert(item))
        elif ":" in stripped and current_dict is not None:
            # Indented dict entry
            key, _, value = stripped.partition(":")
            current_dict[key.strip()] = _auto_convert(value.strip())

    # Flush last key
    if current_key and current_list is not None:
        fm[current_key] = current_list
    if current_key and current_dict is not None:
        fm[current_key] = current_dict

    return fm, body


def _auto_convert(value: str) -> Any:
    """Auto-convert string values to appropriate Python types."""
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if value.lower() in ("null", "none", ""):
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


@dataclass
class SkillEntry:
    """
    Metadata for a discovered trading skill.

    Lightweight representation for listing/matching before
    loading the full SKILL.md content.
    """
    name: str
    description: str = ""
    category: str = "trading"
    path: str = ""
    version: str = "1.0.0"
    tags: List[str] = field(default_factory=list)
    conditions: Dict[str, Any] = field(default_factory=dict)
    # Performance tracking (updated by SkillManager after trades)
    win_rate: float = 0.0
    sample_count: int = 0
    avg_pnl_pct: float = 0.0
    avg_risk_reward: float = 0.0
    last_used: Optional[float] = None
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "path": self.path,
            "version": self.version,
            "tags": self.tags,
            "conditions": self.conditions,
            "win_rate": self.win_rate,
            "sample_count": self.sample_count,
            "avg_pnl_pct": self.avg_pnl_pct,
            "avg_risk_reward": self.avg_risk_reward,
            "last_used": self.last_used,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "SkillEntry":
        return cls(**{k: v for k, v in data.items()
                      if k in cls.__dataclass_fields__})

    def matches_conditions(self, context: Dict[str, Any]) -> float:
        """
        Check if current market context matches this skill's conditions.

        Returns a match score 0-1 based on condition overlap.
        """
        if not self.conditions:
            return 0.0

        score = 0.0
        total = 0

        # Helper: normalize condition value to a list for `in` checks
        def _as_list(val):
            if isinstance(val, list):
                return val
            if isinstance(val, str):
                return [v.strip() for v in val.split(",")]
            return [val]

        # Symbol match
        if "symbols" in self.conditions:
            total += 1
            sym = context.get("symbol")
            if sym and sym in _as_list(self.conditions["symbols"]):
                score += 1.0

        # Timeframe match
        if "timeframes" in self.conditions:
            total += 1
            tf = context.get("timeframe")
            if tf and tf in _as_list(self.conditions["timeframes"]):
                score += 1.0

        # Direction match
        if "direction" in self.conditions:
            total += 1
            if context.get("direction") == self.conditions["direction"]:
                score += 1.0

        # Setup type match
        if "setup_types" in self.conditions:
            total += 1
            st = context.get("setup_type")
            if st and st in _as_list(self.conditions["setup_types"]):
                score += 1.0

        # Market regime match
        if "market_regimes" in self.conditions:
            total += 1
            mr = context.get("market_regime")
            if mr and mr in _as_list(self.conditions["market_regimes"]):
                score += 1.0

        if total == 0:
            return 0.0
        return score / total


def load_skill(skill_dir: Path) -> Tuple[Optional[SkillEntry], str]:
    """
    Load a skill from its directory.

    Returns (SkillEntry, full_content) or (None, "") on failure.
    """
    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        return None, ""

    try:
        content = skill_md.read_text(encoding="utf-8")
        fm, body = parse_frontmatter(content)

        name = fm.get("name", skill_dir.name)[:MAX_NAME_LENGTH]
        description = fm.get("description", "")
        if not description:
            # Fall back to first non-header line
            for line in body.strip().split("\n"):
                line = line.strip()
                if line and not line.startswith("#"):
                    description = line[:MAX_DESCRIPTION_LENGTH]
                    break

        # Parse tags
        tags = fm.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",")]

        # Parse conditions
        conditions = fm.get("conditions", {})

        entry = SkillEntry(
            name=name,
            description=description,
            category=fm.get("category", "trading"),
            path=str(skill_dir),
            version=str(fm.get("version", "1.0.0")),
            tags=tags,
            conditions=conditions if isinstance(conditions, dict) else {},
            win_rate=float(fm.get("win_rate", 0)),
            sample_count=int(fm.get("sample_count", 0)),
            avg_pnl_pct=float(fm.get("avg_pnl_pct", 0)),
            avg_risk_reward=float(fm.get("avg_risk_reward", 0)),
        )

        return entry, content

    except Exception as e:
        logger.warning(f"Failed to load skill from {skill_dir}: {e}")
        return None, ""


def save_skill_metadata(skill_dir: Path, entry: SkillEntry):
    """
    Update a SKILL.md's frontmatter with current performance stats.
    Preserves the body content.
    """
    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        return

    try:
        content = skill_md.read_text(encoding="utf-8")
        fm, body = parse_frontmatter(content)

        # Update performance fields
        fm["win_rate"] = round(entry.win_rate, 4)
        fm["sample_count"] = entry.sample_count
        fm["avg_pnl_pct"] = round(entry.avg_pnl_pct, 4)
        fm["avg_risk_reward"] = round(entry.avg_risk_reward, 2)
        fm["last_used"] = datetime.fromtimestamp(
            time.time(), tz=timezone.utc
        ).isoformat()

        # Rebuild content
        new_fm_lines = ["---"]
        for key, value in fm.items():
            if isinstance(value, list):
                new_fm_lines.append(f"{key}: [{', '.join(str(v) for v in value)}]")
            elif isinstance(value, dict):
                new_fm_lines.append(f"{key}:")
                for k, v in value.items():
                    new_fm_lines.append(f"  {k}: {v}")
            else:
                new_fm_lines.append(f"{key}: {value}")
        new_fm_lines.append("---")

        new_content = "\n".join(new_fm_lines) + "\n" + body
        skill_md.write_text(new_content, encoding="utf-8")

    except Exception as e:
        logger.warning(f"Failed to save skill metadata for {skill_dir}: {e}")
