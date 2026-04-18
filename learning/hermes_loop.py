"""
learning/hermes_loop.py

Self-Improving Trade Intelligence Loop (Hermes-inspired)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Autonomous self-improvement cycle inspired by NousResearch/hermes-agent:

  Trade Closes → Reflect → Score → Create/Update Skill → Feed ADI → Repeat

After each trade outcome, the system:
  1. REFLECT — analyze why it won or lost (microstructure, timing, trend, flow)
  2. SCORE — quantify pattern quality and reliability
  3. SKILL — create or update a "winner template" skill document
  4. FEED — updated skills feed back into ADI and confidence modifiers
  5. EVOLVE — periodic meta-analysis discovers new strategy patterns

Skills accumulate in data/runtime/skills/ as structured JSON.
The system gets smarter every trade, every day, without human intervention.

Architecture: Hermes "task → evaluate → create skill → reuse" loop
adapted for quantitative trading with full observability.
"""

import json
import logging
import sqlite3
import threading
import time
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_SKILLS_DIR = "data/runtime/skills"
_REFLECTIONS_DIR = "data/runtime/reflections"
_LOOP_STATE_FILE = "data/runtime/hermes_loop_state.json"


# ═══════════════════════════════════════════════════════════════════════════════
#  TRADE REFLECTOR — Post-trade analysis engine
# ═══════════════════════════════════════════════════════════════════════════════

class TradeReflector:
    """Analyzes closed trades to extract learnable patterns.

    For each trade, produces a reflection report with:
    - Pattern fingerprint (source + direction + session + hour + pattern)
    - Why it won/lost (microstructure alignment, trend, timing)
    - Confidence vs actual outcome correlation
    - Entry quality metrics (if available from raw_scores)
    """

    def reflect(self, trade: dict) -> dict:
        """Produce a reflection report for a single closed trade."""
        outcome = str(trade.get("outcome", "")).strip().lower()
        if outcome not in {"win", "loss"}:
            return {}

        source = str(trade.get("source", "")).strip().lower()
        direction = str(trade.get("direction", "")).strip().lower()
        symbol = str(trade.get("symbol", "")).strip().upper()
        pnl = float(trade.get("pnl_usd", 0) or 0)

        # Parse execution metadata
        meta = {}
        try:
            raw = trade.get("execution_meta_json", "") or ""
            if isinstance(raw, str) and raw.strip():
                meta = json.loads(raw)
        except Exception:
            meta = {}

        raw_scores = meta.get("raw_scores", {}) if isinstance(meta, dict) else {}

        # Extract timing
        exec_utc = str(trade.get("execution_utc", "") or "")
        hour_utc = self._parse_hour(exec_utc)
        session = self._hour_to_session(hour_utc) if hour_utc >= 0 else "unknown"
        dow = self._parse_dow(exec_utc)

        # Extract entry quality
        confidence = float(raw_scores.get("signal_confidence", 0) or
                          raw_scores.get("confidence_post_neural", 0) or 0)
        pattern = str(raw_scores.get("pattern", "") or
                     meta.get("pattern", "") or "").strip()
        entry = float(trade.get("entry", 0) or 0)
        sl = float(trade.get("stop_loss", 0) or 0)
        risk = abs(entry - sl) if entry and sl else 0

        # Microstructure features at entry time
        adi_data = raw_scores.get("adi_dimensions", {})
        sharpness = raw_scores.get("entry_sharpness_score", 0)
        delta_proxy = float(raw_scores.get("delta_proxy", 0) or 0)
        bar_volume = float(raw_scores.get("bar_volume_proxy", 0) or 0)
        trend_d1 = str(raw_scores.get("signal_d1_trend", "") or "")
        trend_h4 = str(raw_scores.get("signal_h4_trend", "") or "")

        # R-multiple achieved
        r_achieved = 0.0
        if risk > 0 and pnl != 0:
            r_achieved = round(pnl / max(0.01, risk), 2)

        # Build fingerprint — unique pattern identifier
        source_base = source.split(":")[0]
        fingerprint = f"{source_base}|{direction}|{session}|h{hour_utc:02d}|{pattern[:20]}"

        # Determine lesson
        lesson = self._extract_lesson(outcome, pnl, confidence, delta_proxy,
                                       bar_volume, trend_d1, direction, r_achieved)

        return {
            "fingerprint": fingerprint,
            "source": source,
            "source_base": source_base,
            "direction": direction,
            "symbol": symbol,
            "outcome": outcome,
            "pnl_usd": round(pnl, 2),
            "r_achieved": r_achieved,
            "confidence_at_entry": round(confidence, 1),
            "pattern": pattern,
            "session": session,
            "hour_utc": hour_utc,
            "day_of_week": dow,
            "trend_d1": trend_d1,
            "trend_h4": trend_h4,
            "delta_proxy_at_entry": round(delta_proxy, 4),
            "bar_volume_at_entry": round(bar_volume, 4),
            "entry_sharpness": int(sharpness) if sharpness else 0,
            "adi_modifier": float(raw_scores.get("adi_modifier", 0) or 0),
            "lesson": lesson,
            "reflected_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _extract_lesson(self, outcome, pnl, confidence, delta, volume,
                        trend_d1, direction, r_achieved) -> str:
        """Generate a concise lesson from the trade outcome."""
        parts = []
        if outcome == "win":
            if r_achieved >= 2.0:
                parts.append("high_r_winner")
            elif r_achieved >= 1.0:
                parts.append("solid_winner")
            else:
                parts.append("small_winner")
            if confidence >= 78:
                parts.append("high_conf_correct")
            if abs(delta) >= 0.15:
                parts.append("flow_confirmed")
        else:
            if abs(pnl) >= 3.0:
                parts.append("large_loss")
            else:
                parts.append("small_loss")
            if confidence >= 78:
                parts.append("high_conf_wrong:overconfident")
            trend_aligned = (
                (direction == "long" and "bull" in str(trend_d1).lower()) or
                (direction == "short" and "bear" in str(trend_d1).lower())
            )
            if not trend_aligned and trend_d1:
                parts.append("counter_trend_loss")
            if volume < 0.1:
                parts.append("low_volume_entry")
        return "|".join(parts) if parts else outcome

    @staticmethod
    def _parse_hour(utc_str: str) -> int:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(str(utc_str).strip()[:26], fmt).hour
            except ValueError:
                continue
        return -1

    @staticmethod
    def _parse_dow(utc_str: str) -> int:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(str(utc_str).strip()[:26], fmt).weekday()
            except ValueError:
                continue
        return -1

    @staticmethod
    def _hour_to_session(hour: int) -> str:
        if 22 <= hour or hour < 7:
            return "asian"
        if 7 <= hour < 13:
            return "london"
        return "new_york"


# ═══════════════════════════════════════════════════════════════════════════════
#  SKILL LIBRARY — Winner template storage and retrieval
# ═══════════════════════════════════════════════════════════════════════════════

class SkillLibrary:
    """Stores and retrieves winner template skills.

    Each skill represents a learnable pattern — a combination of conditions
    (source, direction, session, hour, pattern) and its historical performance.

    Skills accumulate over time. Good patterns get stronger confidence boosts.
    Bad patterns get stronger penalties. The system LEARNS from every trade.
    """

    def __init__(self, skills_dir: Optional[str] = None):
        self._dir = Path(skills_dir or _SKILLS_DIR)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._cache: dict[str, dict] = {}
        self._cache_loaded = False

    def _ensure_cache(self):
        if self._cache_loaded:
            return
        self._cache_loaded = True
        for f in self._dir.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                fp = str(data.get("fingerprint", f.stem))
                self._cache[fp] = data
            except Exception:
                pass

    def get_skill(self, fingerprint: str) -> Optional[dict]:
        """Retrieve a skill by fingerprint."""
        with self._lock:
            self._ensure_cache()
            return self._cache.get(fingerprint)

    def update_skill(self, reflection: dict):
        """Create or update a skill from a trade reflection.

        This is the CORE learning mechanism — every trade teaches the system.
        """
        fp = reflection.get("fingerprint", "")
        if not fp:
            return

        with self._lock:
            self._ensure_cache()
            existing = self._cache.get(fp)

            if existing:
                skill = self._merge_reflection(existing, reflection)
            else:
                skill = self._create_skill(reflection)

            self._cache[fp] = skill
            self._save_skill(fp, skill)

    def get_all_skills(self) -> dict[str, dict]:
        """Return all skills for meta-analysis."""
        with self._lock:
            self._ensure_cache()
            return dict(self._cache)

    def get_active_skills(self, *, source_base: str = "",
                          direction: str = "", session: str = "") -> list[dict]:
        """Find skills matching given criteria."""
        with self._lock:
            self._ensure_cache()
            results = []
            for skill in self._cache.values():
                if source_base and skill.get("source_base") != source_base:
                    continue
                if direction and skill.get("direction") != direction:
                    continue
                if session and skill.get("session") != session:
                    continue
                if skill.get("sample_count", 0) >= 3:  # minimum samples
                    results.append(skill)
            return results

    def _create_skill(self, r: dict) -> dict:
        """Create a new skill from first reflection."""
        is_win = r.get("outcome") == "win"
        return {
            "fingerprint": r["fingerprint"],
            "source_base": r.get("source_base", ""),
            "direction": r.get("direction", ""),
            "symbol": r.get("symbol", ""),
            "session": r.get("session", ""),
            "hour_utc": r.get("hour_utc", -1),
            "pattern": r.get("pattern", ""),
            # Performance stats
            "sample_count": 1,
            "wins": 1 if is_win else 0,
            "losses": 0 if is_win else 1,
            "win_rate": 1.0 if is_win else 0.0,
            "total_pnl": r.get("pnl_usd", 0.0),
            "avg_pnl": r.get("pnl_usd", 0.0),
            "avg_r": r.get("r_achieved", 0.0),
            "best_r": r.get("r_achieved", 0.0) if is_win else 0.0,
            "worst_loss": r.get("pnl_usd", 0.0) if not is_win else 0.0,
            # Confidence modifier — starts conservative
            "confidence_modifier": 0.0,
            "skill_grade": "novice",  # novice → apprentice → skilled → master
            # Learning history
            "lessons": [r.get("lesson", "")],
            "last_outcome": r.get("outcome", ""),
            "consecutive_wins": 1 if is_win else 0,
            "consecutive_losses": 0 if is_win else 1,
            # Context snapshots (for pattern recognition)
            "avg_confidence_at_entry": r.get("confidence_at_entry", 0.0),
            "avg_delta_at_entry": r.get("delta_proxy_at_entry", 0.0),
            "avg_volume_at_entry": r.get("bar_volume_at_entry", 0.0),
            "dominant_trend_d1": r.get("trend_d1", ""),
            # Metadata
            "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "version": 1,
        }

    def _merge_reflection(self, skill: dict, r: dict) -> dict:
        """Merge a new reflection into existing skill — the learning step."""
        is_win = r.get("outcome") == "win"
        n = skill.get("sample_count", 0)
        wins = skill.get("wins", 0) + (1 if is_win else 0)
        losses = skill.get("losses", 0) + (0 if is_win else 1)
        n_new = n + 1

        total_pnl = skill.get("total_pnl", 0.0) + r.get("pnl_usd", 0.0)
        wr = wins / n_new if n_new > 0 else 0.0

        # Running averages with decay (recent trades weighted more)
        decay = 0.85
        avg_conf = skill.get("avg_confidence_at_entry", 0.0)
        avg_delta = skill.get("avg_delta_at_entry", 0.0)
        avg_vol = skill.get("avg_volume_at_entry", 0.0)
        avg_r = skill.get("avg_r", 0.0)

        skill["sample_count"] = n_new
        skill["wins"] = wins
        skill["losses"] = losses
        skill["win_rate"] = round(wr, 4)
        skill["total_pnl"] = round(total_pnl, 2)
        skill["avg_pnl"] = round(total_pnl / n_new, 2)
        skill["avg_r"] = round(avg_r * decay + r.get("r_achieved", 0.0) * (1 - decay), 3)
        skill["avg_confidence_at_entry"] = round(avg_conf * decay + r.get("confidence_at_entry", 0.0) * (1 - decay), 1)
        skill["avg_delta_at_entry"] = round(avg_delta * decay + r.get("delta_proxy_at_entry", 0.0) * (1 - decay), 4)
        skill["avg_volume_at_entry"] = round(avg_vol * decay + r.get("bar_volume_at_entry", 0.0) * (1 - decay), 4)

        if is_win:
            skill["best_r"] = max(skill.get("best_r", 0.0), r.get("r_achieved", 0.0))
            skill["consecutive_wins"] = skill.get("consecutive_wins", 0) + 1
            skill["consecutive_losses"] = 0
        else:
            skill["worst_loss"] = min(skill.get("worst_loss", 0.0), r.get("pnl_usd", 0.0))
            skill["consecutive_losses"] = skill.get("consecutive_losses", 0) + 1
            skill["consecutive_wins"] = 0

        # Update dominant trend
        if r.get("trend_d1"):
            skill["dominant_trend_d1"] = r["trend_d1"]

        skill["last_outcome"] = r.get("outcome", "")

        # Append lesson (keep last 20)
        lessons = skill.get("lessons", [])
        if r.get("lesson"):
            lessons.append(r["lesson"])
        skill["lessons"] = lessons[-20:]

        # ── SKILL GRADING — autonomous improvement ──
        skill["skill_grade"] = self._compute_grade(skill)
        skill["confidence_modifier"] = self._compute_modifier(skill)

        skill["updated_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        skill["version"] = skill.get("version", 0) + 1

        return skill

    @staticmethod
    def _compute_grade(skill: dict) -> str:
        """Grade skill based on sample size and performance.

        novice → apprentice → skilled → master → elite
        """
        n = skill.get("sample_count", 0)
        wr = skill.get("win_rate", 0.0)
        avg_pnl = skill.get("avg_pnl", 0.0)

        if n < 5:
            return "novice"
        if n < 10:
            return "apprentice" if wr >= 0.45 else "novice"
        if n < 20:
            if wr >= 0.60 and avg_pnl > 0:
                return "skilled"
            return "apprentice" if wr >= 0.40 else "novice"
        # n >= 20
        if wr >= 0.65 and avg_pnl > 0.5:
            return "elite"
        if wr >= 0.55 and avg_pnl > 0:
            return "master"
        if wr >= 0.45:
            return "skilled"
        return "apprentice"

    @staticmethod
    def _compute_modifier(skill: dict) -> float:
        """Compute confidence modifier from skill performance.

        This is what feeds back into the trading pipeline.
        Positive = boost, negative = penalty.
        Range: [-15, +10]
        """
        n = skill.get("sample_count", 0)
        wr = skill.get("win_rate", 0.0)
        avg_pnl = skill.get("avg_pnl", 0.0)
        grade = skill.get("skill_grade", "novice")

        if n < 3:
            return 0.0  # not enough data

        # Base modifier from win rate
        if wr >= 0.70:
            base = 8.0
        elif wr >= 0.60:
            base = 4.0
        elif wr >= 0.55:
            base = 2.0
        elif wr >= 0.45:
            base = 0.0
        elif wr >= 0.35:
            base = -4.0
        elif wr >= 0.25:
            base = -8.0
        else:
            base = -12.0

        # Sample size dampening (less confident with fewer samples)
        if n < 5:
            base *= 0.3
        elif n < 10:
            base *= 0.6
        elif n < 20:
            base *= 0.85

        # P&L reality check
        if avg_pnl < -2.0 and base > 0:
            base = min(base, 1.0)  # can't boost if losing money
        if avg_pnl > 1.0 and base < 0:
            base = max(base, -2.0)  # don't penalize too hard if making money

        # Grade bonus
        grade_bonus = {"elite": 3.0, "master": 1.5, "skilled": 0.5}.get(grade, 0.0)

        return round(max(-15.0, min(10.0, base + grade_bonus)), 1)

    def _save_skill(self, fingerprint: str, skill: dict):
        """Persist skill to disk."""
        safe_name = fingerprint.replace("|", "_").replace("/", "_").replace(":", "_")
        path = self._dir / f"{safe_name}.json"
        try:
            path.write_text(json.dumps(skill, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            logger.debug("[SkillLibrary] save error: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  STRATEGY EVOLVER — Periodic meta-analysis
# ═══════════════════════════════════════════════════════════════════════════════

class StrategyEvolver:
    """Periodic meta-analysis of all skills to discover patterns.

    Runs every N hours (default: 4) and:
    - Identifies top-performing skills (for promotion)
    - Identifies toxic skills (for demotion)
    - Discovers cross-skill patterns (e.g. "london shorts always lose")
    - Generates a strategy evolution report
    """

    def evolve(self, skills: dict[str, dict]) -> dict:
        """Analyze all skills and produce evolution report."""
        if not skills:
            return {"status": "no_skills"}

        # Classify skills by grade
        by_grade = {}
        for fp, skill in skills.items():
            grade = skill.get("skill_grade", "novice")
            by_grade.setdefault(grade, []).append(skill)

        # Top performers
        sorted_skills = sorted(
            [s for s in skills.values() if s.get("sample_count", 0) >= 5],
            key=lambda s: (s.get("win_rate", 0), s.get("avg_pnl", 0)),
            reverse=True,
        )
        top_5 = sorted_skills[:5]
        bottom_5 = sorted_skills[-5:] if len(sorted_skills) >= 5 else []

        # Cross-skill pattern discovery
        patterns = self._discover_patterns(skills)

        # Session analysis
        session_stats = {}
        for skill in skills.values():
            sess = skill.get("session", "unknown")
            if sess not in session_stats:
                session_stats[sess] = {"wins": 0, "losses": 0, "pnl": 0.0, "n": 0}
            session_stats[sess]["wins"] += skill.get("wins", 0)
            session_stats[sess]["losses"] += skill.get("losses", 0)
            session_stats[sess]["pnl"] += skill.get("total_pnl", 0.0)
            session_stats[sess]["n"] += skill.get("sample_count", 0)

        for sess, stats in session_stats.items():
            total = stats["wins"] + stats["losses"]
            stats["wr"] = round(stats["wins"] / total, 3) if total > 0 else 0.0

        return {
            "status": "evolved",
            "total_skills": len(skills),
            "grade_distribution": {g: len(s) for g, s in by_grade.items()},
            "top_performers": [
                {"fingerprint": s["fingerprint"], "wr": s.get("win_rate", 0),
                 "n": s.get("sample_count", 0), "pnl": s.get("total_pnl", 0),
                 "grade": s.get("skill_grade", "")}
                for s in top_5
            ],
            "worst_performers": [
                {"fingerprint": s["fingerprint"], "wr": s.get("win_rate", 0),
                 "n": s.get("sample_count", 0), "pnl": s.get("total_pnl", 0),
                 "grade": s.get("skill_grade", "")}
                for s in bottom_5
            ],
            "session_stats": session_stats,
            "discovered_patterns": patterns,
            "evolved_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _discover_patterns(self, skills: dict[str, dict]) -> list[str]:
        """Auto-discover cross-skill patterns."""
        patterns = []

        # Direction bias per source
        source_dir = {}
        for skill in skills.values():
            sb = skill.get("source_base", "")
            d = skill.get("direction", "")
            if sb and d and skill.get("sample_count", 0) >= 5:
                key = f"{sb}:{d}"
                source_dir.setdefault(key, []).append(skill)

        for key, group in source_dir.items():
            avg_wr = sum(s.get("win_rate", 0) for s in group) / len(group)
            total_pnl = sum(s.get("total_pnl", 0) for s in group)
            total_n = sum(s.get("sample_count", 0) for s in group)
            if avg_wr <= 0.25 and total_n >= 10:
                patterns.append(f"TOXIC:{key}:wr={avg_wr:.0%}:pnl=${total_pnl:.1f}:n={total_n}")
            elif avg_wr >= 0.65 and total_n >= 10:
                patterns.append(f"STRONG:{key}:wr={avg_wr:.0%}:pnl=${total_pnl:.1f}:n={total_n}")

        # Hour patterns
        hour_stats = {}
        for skill in skills.values():
            h = skill.get("hour_utc", -1)
            if h >= 0 and skill.get("sample_count", 0) >= 3:
                if h not in hour_stats:
                    hour_stats[h] = {"wins": 0, "losses": 0, "pnl": 0.0}
                hour_stats[h]["wins"] += skill.get("wins", 0)
                hour_stats[h]["losses"] += skill.get("losses", 0)
                hour_stats[h]["pnl"] += skill.get("total_pnl", 0.0)

        for h, stats in hour_stats.items():
            total = stats["wins"] + stats["losses"]
            if total >= 10:
                wr = stats["wins"] / total
                if wr <= 0.25:
                    patterns.append(f"TOXIC_HOUR:UTC{h:02d}:wr={wr:.0%}:pnl=${stats['pnl']:.1f}")
                elif wr >= 0.65:
                    patterns.append(f"STRONG_HOUR:UTC{h:02d}:wr={wr:.0%}:pnl=${stats['pnl']:.1f}")

        return patterns


# ═══════════════════════════════════════════════════════════════════════════════
#  IMPROVEMENT LOOP — The self-improving cycle
# ═══════════════════════════════════════════════════════════════════════════════

class ImprovementLoop:
    """Autonomous self-improvement loop.

    Runs as a background thread, processing new trade outcomes and
    evolving the skill library continuously.

    cycle:  check DB → reflect → update skills → (periodic) evolve
    """

    def __init__(self, db_path: Optional[str] = None,
                 skills_dir: Optional[str] = None):
        self._db_path = db_path
        self._reflector = TradeReflector()
        self._library = SkillLibrary(skills_dir)
        self._evolver = StrategyEvolver()
        self._last_processed_utc = ""
        self._last_evolution_utc = ""
        self._cycle_count = 0
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._state_path = Path(_LOOP_STATE_FILE)
        self._load_state()

    def _get_db_path(self) -> Path:
        if self._db_path:
            return Path(self._db_path)
        try:
            from config import config as _cfg
            return Path(getattr(_cfg, "CTRADER_OPENAPI_DB_PATH", "data/ctrader_openapi.db"))
        except Exception:
            return Path("data/ctrader_openapi.db")

    def _load_state(self):
        """Restore last-processed marker from disk."""
        if self._state_path.exists():
            try:
                state = json.loads(self._state_path.read_text(encoding="utf-8"))
                self._last_processed_utc = str(state.get("last_processed_utc", "") or "")
                self._last_evolution_utc = str(state.get("last_evolution_utc", "") or "")
                self._cycle_count = int(state.get("cycle_count", 0) or 0)
            except Exception:
                pass

    def _save_state(self):
        """Persist loop state."""
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            state = {
                "last_processed_utc": self._last_processed_utc,
                "last_evolution_utc": self._last_evolution_utc,
                "cycle_count": self._cycle_count,
                "saved_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
            self._state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        except Exception:
            pass

    def start(self, interval_sec: int = 300):
        """Start the background improvement loop."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, args=(interval_sec,),
            daemon=True, name="hermes-loop",
        )
        self._thread.start()
        logger.info("[HermesLoop] Started — interval=%ds", interval_sec)

    def stop(self):
        self._running = False

    def _run_loop(self, interval: int):
        """Main loop — runs forever in background."""
        # Initial delay to let system warm up
        time.sleep(30)
        while self._running:
            try:
                self._cycle()
            except Exception as e:
                logger.debug("[HermesLoop] cycle error: %s", e)
            time.sleep(interval)

    def _cycle(self):
        """One improvement cycle: reflect → learn → (evolve)."""
        db_path = self._get_db_path()
        if not db_path.exists():
            return

        new_trades = self._fetch_new_trades(db_path)
        if not new_trades:
            return

        reflected = 0
        for trade in new_trades:
            reflection = self._reflector.reflect(trade)
            if reflection:
                self._library.update_skill(reflection)
                reflected += 1

        if reflected > 0:
            self._cycle_count += 1
            logger.info(
                "[HermesLoop] Cycle #%d — reflected %d trades, %d skills total",
                self._cycle_count, reflected, len(self._library.get_all_skills()),
            )

        # Periodic evolution (every 4 hours)
        now_utc = datetime.now(timezone.utc)
        last_evo = self._last_evolution_utc
        should_evolve = False
        if not last_evo:
            should_evolve = True
        else:
            try:
                last_dt = datetime.strptime(last_evo, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if (now_utc - last_dt).total_seconds() >= 14400:  # 4 hours
                    should_evolve = True
            except Exception:
                should_evolve = True

        if should_evolve:
            report = self._evolver.evolve(self._library.get_all_skills())
            self._last_evolution_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S")
            self._save_evolution_report(report)
            logger.info(
                "[HermesLoop] Evolution complete — %d skills, %d patterns discovered",
                report.get("total_skills", 0),
                len(report.get("discovered_patterns", [])),
            )

        self._save_state()

    def _fetch_new_trades(self, db_path: Path) -> list[dict]:
        """Fetch trades closed since last processed."""
        since = self._last_processed_utc or (
            datetime.now(timezone.utc) - timedelta(days=14)
        ).strftime("%Y-%m-%d %H:%M:%S")

        try:
            with closing(sqlite3.connect(str(db_path), timeout=5)) as conn:
                conn.row_factory = sqlite3.Row
                cols = {r[1] for r in conn.execute("PRAGMA table_info(execution_journal)").fetchall()}
                if "outcome" not in cols:
                    return []

                select = ["source", "direction", "symbol", "outcome", "pnl_usd", "execution_utc"]
                for c in ("entry", "stop_loss", "execution_meta_json"):
                    if c in cols:
                        select.append(c)

                rows = conn.execute(
                    f"SELECT {', '.join(select)} FROM execution_journal "
                    f"WHERE outcome IN ('win', 'loss') AND execution_utc > ? "
                    f"ORDER BY execution_utc ASC LIMIT 100",
                    (since,),
                ).fetchall()

                trades = [dict(r) for r in rows]
                if trades:
                    self._last_processed_utc = str(trades[-1].get("execution_utc", since))
                return trades
        except Exception as e:
            logger.debug("[HermesLoop] DB fetch error: %s", e)
            return []

    def _save_evolution_report(self, report: dict):
        """Save evolution report for observability."""
        try:
            report_dir = Path("data/reports")
            report_dir.mkdir(parents=True, exist_ok=True)
            path = report_dir / "hermes_evolution_latest.json"
            path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    # ── Public API for scheduler integration ──

    def get_skill_modifier(self, *, source: str, direction: str,
                           session: str = "") -> tuple[float, dict]:
        """Get the skill-based confidence modifier for a signal.

        Called by scheduler/ADI to apply learned patterns.
        Returns (modifier, details).
        """
        source_base = str(source or "").split(":")[0].strip().lower()
        skills = self._library.get_active_skills(
            source_base=source_base, direction=direction, session=session,
        )
        if not skills:
            return 0.0, {"reason": "no_matching_skills"}

        # Weight by sample count (more samples = more trust)
        total_weight = 0.0
        weighted_mod = 0.0
        for skill in skills:
            n = skill.get("sample_count", 0)
            mod = skill.get("confidence_modifier", 0.0)
            weight = min(n, 30)  # cap influence at 30 samples
            weighted_mod += mod * weight
            total_weight += weight

        if total_weight <= 0:
            return 0.0, {"reason": "insufficient_weight"}

        modifier = round(weighted_mod / total_weight, 1)
        return modifier, {
            "matching_skills": len(skills),
            "total_samples": sum(s.get("sample_count", 0) for s in skills),
            "avg_wr": round(sum(s.get("win_rate", 0) for s in skills) / len(skills), 3),
            "modifier": modifier,
        }


# ── Singleton ───────────────────────────────────────────────────────────────
improvement_loop = ImprovementLoop()
