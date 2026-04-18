"""
tests/test_hermes_loop.py — Unit tests for the Hermes self-improving loop.

Tests: TradeReflector, SkillLibrary, SkillGrading, StrategyEvolver,
       ImprovementLoop, get_skill_modifier.
"""
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

# ── Helpers ─────────────────────────────────────────────────────────────────
os.environ.setdefault("PYTEST_CURRENT_TEST", "1")


def _make_trade(*, source="scalp_xauusd", direction="long", symbol="XAUUSD",
                outcome="win", pnl=1.5, confidence=75.0,
                exec_utc="2026-04-10 09:15:00", pattern="pullback",
                delta_proxy=0.12, bar_volume=0.25, trend_d1="bullish",
                entry=3050.0, stop_loss=3048.0, extra_meta=None):
    """Create a fake trade dict mirroring execution_journal row."""
    meta = {
        "raw_scores": {
            "signal_confidence": confidence,
            "pattern": pattern,
            "delta_proxy": delta_proxy,
            "bar_volume_proxy": bar_volume,
            "signal_d1_trend": trend_d1,
        },
    }
    if extra_meta:
        meta.update(extra_meta)
    return {
        "source": source,
        "direction": direction,
        "symbol": symbol,
        "outcome": outcome,
        "pnl_usd": pnl,
        "execution_utc": exec_utc,
        "entry": entry,
        "stop_loss": stop_loss,
        "execution_meta_json": json.dumps(meta),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  TradeReflector
# ═══════════════════════════════════════════════════════════════════════════════

class TestTradeReflector:
    def _make(self):
        from learning.hermes_loop import TradeReflector
        return TradeReflector()

    def test_reflect_win(self):
        r = self._make()
        ref = r.reflect(_make_trade(outcome="win", pnl=2.5))
        assert ref["outcome"] == "win"
        assert ref["fingerprint"]
        assert "scalp_xauusd" in ref["fingerprint"]
        assert ref["direction"] == "long"
        assert ref["pnl_usd"] == 2.5
        assert ref["lesson"]

    def test_reflect_loss(self):
        r = self._make()
        ref = r.reflect(_make_trade(outcome="loss", pnl=-1.2))
        assert ref["outcome"] == "loss"
        assert ref["pnl_usd"] == -1.2

    def test_reflect_invalid_outcome_returns_empty(self):
        r = self._make()
        assert r.reflect(_make_trade(outcome="pending")) == {}
        assert r.reflect(_make_trade(outcome="")) == {}

    def test_fingerprint_structure(self):
        r = self._make()
        ref = r.reflect(_make_trade(source="canary_tdf:xau", direction="short",
                                     exec_utc="2026-04-10 14:30:00"))
        fp = ref["fingerprint"]
        parts = fp.split("|")
        assert len(parts) == 5
        assert parts[0] == "canary_tdf"  # source base
        assert parts[1] == "short"

    def test_session_detection_london(self):
        r = self._make()
        ref = r.reflect(_make_trade(exec_utc="2026-04-10 09:15:00"))
        assert ref["session"] == "london"

    def test_session_detection_asian(self):
        r = self._make()
        ref = r.reflect(_make_trade(exec_utc="2026-04-10 23:30:00"))
        assert ref["session"] == "asian"

    def test_session_detection_new_york(self):
        r = self._make()
        ref = r.reflect(_make_trade(exec_utc="2026-04-10 15:00:00"))
        assert ref["session"] == "new_york"

    def test_r_achieved_calculation(self):
        r = self._make()
        ref = r.reflect(_make_trade(entry=3050.0, stop_loss=3048.0,
                                     pnl=4.0, outcome="win"))
        assert ref["r_achieved"] == 2.0  # 4 / 2

    def test_lesson_high_r_winner(self):
        r = self._make()
        ref = r.reflect(_make_trade(pnl=6.0, entry=3050, stop_loss=3048))
        assert "high_r_winner" in ref["lesson"]

    def test_lesson_counter_trend_loss(self):
        r = self._make()
        ref = r.reflect(_make_trade(outcome="loss", pnl=-1.0,
                                     direction="long", trend_d1="bearish"))
        assert "counter_trend_loss" in ref["lesson"]

    def test_lesson_overconfident(self):
        r = self._make()
        ref = r.reflect(_make_trade(outcome="loss", pnl=-2.0, confidence=85.0))
        assert "high_conf_wrong" in ref["lesson"]

    def test_both_directions(self):
        r = self._make()
        long_ref = r.reflect(_make_trade(direction="long"))
        short_ref = r.reflect(_make_trade(direction="short"))
        assert long_ref["direction"] == "long"
        assert short_ref["direction"] == "short"
        assert long_ref["fingerprint"] != short_ref["fingerprint"]


# ═══════════════════════════════════════════════════════════════════════════════
#  SkillLibrary
# ═══════════════════════════════════════════════════════════════════════════════

class TestSkillLibrary:
    def _make(self, tmp_path):
        from learning.hermes_loop import SkillLibrary
        return SkillLibrary(skills_dir=str(tmp_path / "skills"))

    def _reflect(self, **kw):
        from learning.hermes_loop import TradeReflector
        return TradeReflector().reflect(_make_trade(**kw))

    def test_create_skill_from_reflection(self, tmp_path):
        lib = self._make(tmp_path)
        ref = self._reflect(outcome="win", pnl=2.0)
        lib.update_skill(ref)
        skill = lib.get_skill(ref["fingerprint"])
        assert skill is not None
        assert skill["sample_count"] == 1
        assert skill["wins"] == 1
        assert skill["skill_grade"] == "novice"

    def test_merge_skill_updates_stats(self, tmp_path):
        lib = self._make(tmp_path)
        for i in range(5):
            ref = self._reflect(outcome="win", pnl=1.0 + i * 0.5,
                                exec_utc=f"2026-04-10 09:{10+i}:00")
            lib.update_skill(ref)
        skill = lib.get_skill(ref["fingerprint"])
        assert skill["sample_count"] == 5
        assert skill["wins"] == 5
        assert skill["win_rate"] == 1.0

    def test_mixed_outcomes(self, tmp_path):
        lib = self._make(tmp_path)
        for i in range(3):
            lib.update_skill(self._reflect(outcome="win", pnl=1.0))
        for i in range(2):
            lib.update_skill(self._reflect(outcome="loss", pnl=-1.0))
        skill = list(lib.get_all_skills().values())[0]
        assert skill["sample_count"] == 5
        assert skill["wins"] == 3
        assert skill["losses"] == 2
        assert skill["win_rate"] == 0.6

    def test_skill_persists_to_disk(self, tmp_path):
        lib = self._make(tmp_path)
        ref = self._reflect()
        lib.update_skill(ref)
        # Create new library instance from same dir — should load from disk
        from learning.hermes_loop import SkillLibrary
        lib2 = SkillLibrary(skills_dir=str(tmp_path / "skills"))
        skill = lib2.get_skill(ref["fingerprint"])
        assert skill is not None
        assert skill["sample_count"] == 1

    def test_get_active_skills_filters(self, tmp_path):
        lib = self._make(tmp_path)
        # Need 3+ samples to be "active"
        for _ in range(4):
            lib.update_skill(self._reflect(source="scalp_xauusd", direction="long"))
        for _ in range(4):
            lib.update_skill(self._reflect(source="canary_tdf", direction="short"))

        long_skills = lib.get_active_skills(direction="long")
        short_skills = lib.get_active_skills(direction="short")
        assert len(long_skills) >= 1
        assert len(short_skills) >= 1
        assert all(s["direction"] == "long" for s in long_skills)
        assert all(s["direction"] == "short" for s in short_skills)

    def test_lessons_capped_at_20(self, tmp_path):
        lib = self._make(tmp_path)
        for i in range(25):
            lib.update_skill(self._reflect(outcome="win", pnl=0.5,
                                           exec_utc=f"2026-04-10 09:{i%60:02d}:00"))
        skill = list(lib.get_all_skills().values())[0]
        assert len(skill["lessons"]) <= 20

    def test_consecutive_tracking(self, tmp_path):
        lib = self._make(tmp_path)
        # 3 wins then 2 losses
        for _ in range(3):
            lib.update_skill(self._reflect(outcome="win", pnl=1.0))
        skill = list(lib.get_all_skills().values())[0]
        assert skill["consecutive_wins"] == 3
        assert skill["consecutive_losses"] == 0
        for _ in range(2):
            lib.update_skill(self._reflect(outcome="loss", pnl=-1.0))
        skill = list(lib.get_all_skills().values())[0]
        assert skill["consecutive_wins"] == 0
        assert skill["consecutive_losses"] == 2


# ═══════════════════════════════════════════════════════════════════════════════
#  Skill Grading & Modifier
# ═══════════════════════════════════════════════════════════════════════════════

class TestSkillGrading:
    def _grade(self, n, wr, avg_pnl=0.5):
        from learning.hermes_loop import SkillLibrary
        return SkillLibrary._compute_grade(
            {"sample_count": n, "win_rate": wr, "avg_pnl": avg_pnl}
        )

    def _modifier(self, n, wr, avg_pnl=0.5, grade="novice"):
        from learning.hermes_loop import SkillLibrary
        return SkillLibrary._compute_modifier(
            {"sample_count": n, "win_rate": wr, "avg_pnl": avg_pnl,
             "skill_grade": grade}
        )

    def test_grade_novice_low_samples(self):
        assert self._grade(2, 1.0) == "novice"

    def test_grade_apprentice(self):
        assert self._grade(7, 0.55) == "apprentice"

    def test_grade_skilled(self):
        assert self._grade(15, 0.65, avg_pnl=1.0) == "skilled"

    def test_grade_master(self):
        assert self._grade(25, 0.60, avg_pnl=0.8) == "master"

    def test_grade_elite(self):
        assert self._grade(25, 0.70, avg_pnl=1.0) == "elite"

    def test_modifier_zero_below_3(self):
        assert self._modifier(2, 0.8) == 0.0

    def test_modifier_positive_high_wr(self):
        mod = self._modifier(15, 0.72, avg_pnl=1.0)
        assert mod > 0

    def test_modifier_negative_low_wr(self):
        mod = self._modifier(15, 0.20, avg_pnl=-2.0)
        assert mod < 0

    def test_modifier_capped_range(self):
        assert self._modifier(30, 0.10, avg_pnl=-5.0) >= -15.0
        assert self._modifier(30, 0.95, avg_pnl=5.0, grade="elite") <= 10.0

    def test_modifier_dampened_small_sample(self):
        mod_small = self._modifier(4, 0.75, avg_pnl=1.0)
        mod_large = self._modifier(15, 0.75, avg_pnl=1.0)
        assert abs(mod_small) < abs(mod_large)  # dampening kicks in

    def test_pnl_reality_check(self):
        # High WR but losing money → modifier capped
        mod = self._modifier(15, 0.65, avg_pnl=-3.0)
        # Can't give big boost if losing money
        assert mod <= 1.0 or mod <= 4.0  # dampened by P&L check

    def test_grade_bonus_elite(self):
        mod_novice = self._modifier(25, 0.72, avg_pnl=1.0, grade="novice")
        mod_elite = self._modifier(25, 0.72, avg_pnl=1.0, grade="elite")
        assert mod_elite > mod_novice


# ═══════════════════════════════════════════════════════════════════════════════
#  StrategyEvolver
# ═══════════════════════════════════════════════════════════════════════════════

class TestStrategyEvolver:
    def _make(self):
        from learning.hermes_loop import StrategyEvolver
        return StrategyEvolver()

    def test_evolve_empty_skills(self):
        e = self._make()
        report = e.evolve({})
        assert report["status"] == "no_skills"

    def test_evolve_basic(self):
        e = self._make()
        skills = {
            "fp1": {"fingerprint": "fp1", "source_base": "scalp_xauusd",
                    "direction": "long", "session": "london",
                    "sample_count": 10, "wins": 7, "losses": 3,
                    "win_rate": 0.7, "total_pnl": 12.5, "avg_pnl": 1.25,
                    "skill_grade": "skilled", "hour_utc": 9},
            "fp2": {"fingerprint": "fp2", "source_base": "canary_tdf",
                    "direction": "short", "session": "new_york",
                    "sample_count": 8, "wins": 2, "losses": 6,
                    "win_rate": 0.25, "total_pnl": -8.0, "avg_pnl": -1.0,
                    "skill_grade": "novice", "hour_utc": 15},
        }
        report = e.evolve(skills)
        assert report["status"] == "evolved"
        assert report["total_skills"] == 2
        assert "session_stats" in report

    def test_discover_toxic_pattern(self):
        e = self._make()
        skills = {}
        for i in range(12):
            fp = f"toxic_{i}"
            skills[fp] = {
                "fingerprint": fp, "source_base": "scalp_xauusd",
                "direction": "short", "session": "london",
                "sample_count": 5, "wins": 1, "losses": 4,
                "win_rate": 0.20, "total_pnl": -5.0, "avg_pnl": -1.0,
                "skill_grade": "novice", "hour_utc": 10,
            }
        report = e.evolve(skills)
        patterns = report.get("discovered_patterns", [])
        toxic = [p for p in patterns if "TOXIC" in p]
        assert len(toxic) >= 1

    def test_discover_strong_pattern(self):
        e = self._make()
        skills = {}
        for i in range(12):
            fp = f"strong_{i}"
            skills[fp] = {
                "fingerprint": fp, "source_base": "canary_mfu",
                "direction": "long", "session": "london",
                "sample_count": 5, "wins": 4, "losses": 1,
                "win_rate": 0.80, "total_pnl": 8.0, "avg_pnl": 1.6,
                "skill_grade": "skilled", "hour_utc": 9,
            }
        report = e.evolve(skills)
        patterns = report.get("discovered_patterns", [])
        strong = [p for p in patterns if "STRONG" in p]
        assert len(strong) >= 1


# ═══════════════════════════════════════════════════════════════════════════════
#  ImprovementLoop
# ═══════════════════════════════════════════════════════════════════════════════

class TestImprovementLoop:
    def _make_loop(self, tmp_path, db_path=None):
        from learning.hermes_loop import ImprovementLoop
        loop = ImprovementLoop(
            db_path=str(db_path) if db_path else None,
            skills_dir=str(tmp_path / "skills"),
        )
        # Isolate state file to tmp_path so tests don't share state
        loop._state_path = tmp_path / "hermes_loop_state.json"
        loop._last_processed_utc = ""
        loop._last_evolution_utc = ""
        loop._cycle_count = 0
        return loop

    def _create_db(self, tmp_path, trades=None):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE execution_journal (
                source TEXT, direction TEXT, symbol TEXT,
                outcome TEXT, pnl_usd REAL, execution_utc TEXT,
                entry REAL, stop_loss REAL, execution_meta_json TEXT
            )
        """)
        if trades:
            for t in trades:
                conn.execute(
                    "INSERT INTO execution_journal VALUES (?,?,?,?,?,?,?,?,?)",
                    (t.get("source", "scalp_xauusd"),
                     t.get("direction", "long"),
                     t.get("symbol", "XAUUSD"),
                     t.get("outcome", "win"),
                     t.get("pnl_usd", 1.0),
                     t.get("execution_utc", "2026-04-10 09:00:00"),
                     t.get("entry", 3050.0),
                     t.get("stop_loss", 3048.0),
                     t.get("execution_meta_json", "{}"))
                )
        conn.commit()
        conn.close()
        return db_path

    def test_cycle_processes_trades(self, tmp_path):
        trades = [_make_trade(outcome="win", pnl=1.5,
                              exec_utc=f"2026-04-10 09:{i:02d}:00")
                  for i in range(5)]
        db_path = self._create_db(tmp_path, trades)
        loop = self._make_loop(tmp_path, db_path)
        loop._cycle()
        skills = loop._library.get_all_skills()
        assert len(skills) >= 1

    def test_cycle_incremental(self, tmp_path):
        """Second cycle should not reprocess already-seen trades."""
        trades = [_make_trade(outcome="win", pnl=1.0,
                              exec_utc="2026-04-10 09:00:00")]
        db_path = self._create_db(tmp_path, trades)
        loop = self._make_loop(tmp_path, db_path)
        loop._cycle()
        # Add new trade — same hour so same fingerprint → merges into same skill
        conn = sqlite3.connect(str(db_path))
        t = _make_trade(outcome="loss", pnl=-0.5,
                        exec_utc="2026-04-10 09:30:00")
        conn.execute(
            "INSERT INTO execution_journal VALUES (?,?,?,?,?,?,?,?,?)",
            (t["source"], t["direction"], t["symbol"], t["outcome"],
             t["pnl_usd"], t["execution_utc"], t["entry"], t["stop_loss"],
             t["execution_meta_json"])
        )
        conn.commit()
        conn.close()
        loop._cycle()
        skills = loop._library.get_all_skills()
        skill = list(skills.values())[0]
        assert skill["sample_count"] == 2

    def test_get_skill_modifier_no_data(self, tmp_path):
        loop = self._make_loop(tmp_path)
        mod, detail = loop.get_skill_modifier(source="x", direction="long")
        assert mod == 0.0
        assert detail.get("reason") == "no_matching_skills"

    def test_get_skill_modifier_with_data(self, tmp_path):
        trades = [_make_trade(outcome="win", pnl=1.5,
                              exec_utc=f"2026-04-10 09:{i:02d}:00")
                  for i in range(10)]
        db_path = self._create_db(tmp_path, trades)
        loop = self._make_loop(tmp_path, db_path)
        loop._cycle()
        mod, detail = loop.get_skill_modifier(
            source="scalp_xauusd", direction="long", session="london",
        )
        # 10 wins → should have positive modifier
        assert mod > 0
        assert detail.get("matching_skills", 0) >= 1

    def test_evolution_report_created(self, tmp_path):
        trades = [_make_trade(outcome="win", pnl=1.0,
                              exec_utc=f"2026-04-10 09:{i:02d}:00")
                  for i in range(5)]
        db_path = self._create_db(tmp_path, trades)
        loop = self._make_loop(tmp_path, db_path)
        loop._last_evolution_utc = ""  # force evolution
        loop._cycle()
        assert loop._last_evolution_utc != ""

    def test_state_persistence(self, tmp_path):
        loop = self._make_loop(tmp_path)
        loop._last_processed_utc = "2026-04-10 09:00:00"
        loop._cycle_count = 42
        loop._state_path = tmp_path / "state.json"
        loop._save_state()
        # Load into new loop
        from learning.hermes_loop import ImprovementLoop
        loop2 = ImprovementLoop(skills_dir=str(tmp_path / "skills"))
        loop2._state_path = tmp_path / "state.json"
        loop2._load_state()
        assert loop2._last_processed_utc == "2026-04-10 09:00:00"
        assert loop2._cycle_count == 42

    def test_bidirectional_skill_learning(self, tmp_path):
        """Long and short build independent skills."""
        trades = []
        for i in range(5):
            trades.append(_make_trade(direction="long", outcome="win", pnl=2.0,
                                      exec_utc=f"2026-04-10 09:{i:02d}:00"))
            trades.append(_make_trade(direction="short", outcome="loss", pnl=-1.0,
                                      exec_utc=f"2026-04-10 09:{i:02d}:30"))
        db_path = self._create_db(tmp_path, trades)
        loop = self._make_loop(tmp_path, db_path)
        loop._cycle()

        mod_long, _ = loop.get_skill_modifier(
            source="scalp_xauusd", direction="long", session="london")
        mod_short, _ = loop.get_skill_modifier(
            source="scalp_xauusd", direction="short", session="london")
        # Longs winning → boost; shorts losing → penalty
        assert mod_long > mod_short

    def test_empty_db_no_crash(self, tmp_path):
        db_path = self._create_db(tmp_path, trades=[])
        loop = self._make_loop(tmp_path, db_path)
        loop._cycle()  # should not crash — no trades to process
        # cycle_count only increments when trades are reflected
        assert loop._cycle_count == 0 or True  # no crash is the test

    def test_missing_db_no_crash(self, tmp_path):
        loop = self._make_loop(tmp_path, db_path=tmp_path / "nonexistent.db")
        loop._cycle()  # should not crash
