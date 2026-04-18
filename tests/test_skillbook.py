from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from trading_ai.core.self_improvement import SelfImprovementEngine
from trading_ai.core.skillbook import SkillBook


class SkillBookTests(unittest.TestCase):
    def test_upsert_and_recall_skill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            book = SkillBook(
                root_dir=root / "skills",
                index_path=root / "skillbook_index.json",
                max_evidence=4,
            )

            skill = book.upsert_from_review(
                review={
                    "skill_key": "UP*HIGH*NY_trend_follow",
                    "title": "Trend-follow continuation",
                    "summary": "Prefer aligned directional continuation in NY when volatility is high.",
                    "use_when": ["Trend and action align."],
                    "avoid_when": ["Stand aside in range conditions."],
                    "guardrails": ["Keep exposure capped."],
                    "confidence_rules": ["Require >=0.65 confidence for full size."],
                },
                evidence={
                    "score": 1,
                    "pnl": 1.25,
                    "confidence": 0.50,
                    "symbol": "XAUUSD",
                    "session": "NY",
                    "setup_tag": "trend_follow",
                    "strategy_key": "UP*HIGH*NY_trend_follow",
                    "room": "UP*HIGH*NY_trend_follow",
                    "trend_direction": "UP",
                    "volatility": "HIGH",
                    "action": "BUY",
                    "reason": "clean continuation",
                    "created_ts": 1.0,
                    "outcome_label": "win",
                },
            )

            self.assertTrue(Path(skill["file_path"]).is_file())
            self.assertEqual(skill["stats"]["wins"], 1)
            self.assertEqual(skill["stats"]["underconfident_wins"], 1)

            skill = book.upsert_from_review(
                review={
                    "skill_key": "UP*HIGH*NY_trend_follow",
                    "summary": "Second sample adds caution after a bad high-confidence entry.",
                    "avoid_when": ["Avoid forcing entries after extended move."],
                    "confidence_rules": ["Demand stronger confirmation after an overconfident loss."],
                },
                evidence={
                    "score": -1,
                    "pnl": -0.75,
                    "confidence": 0.91,
                    "symbol": "XAUUSD",
                    "session": "NY",
                    "setup_tag": "trend_follow",
                    "strategy_key": "UP*HIGH*NY_trend_follow",
                    "room": "UP*HIGH*NY_trend_follow",
                    "trend_direction": "UP",
                    "volatility": "HIGH",
                    "action": "BUY",
                    "reason": "late entry",
                    "created_ts": 2.0,
                    "outcome_label": "loss",
                },
            )

            self.assertEqual(skill["stats"]["trades_seen"], 2)
            self.assertEqual(skill["stats"]["losses"], 1)
            self.assertEqual(skill["stats"]["overconfident_losses"], 1)
            self.assertIn("Trend and action align.", skill["use_when"])
            self.assertIn("Avoid forcing entries after extended move.", skill["avoid_when"])

            matches = book.recall(
                symbol="XAUUSD",
                session="NY",
                setup_tag="trend_follow",
                strategy_key="UP*HIGH*NY_trend_follow",
                room="UP*HIGH*NY_trend_follow",
                trend_direction="UP",
                volatility="HIGH",
                action="BUY",
                top_k=3,
            )

            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0].skill_key, "UP*HIGH*NY_trend_follow")
            self.assertIn("Trend-follow continuation", book.render_prompt_context(matches))


class SelfImprovementTests(unittest.TestCase):
    def test_heuristic_self_improvement_updates_skillbook(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            book = SkillBook(
                root_dir=root / "skills",
                index_path=root / "skillbook_index.json",
                max_evidence=4,
            )
            improver = SelfImprovementEngine(
                skillbook=book,
                memory=None,
                llm=None,
                enabled=True,
                store_notes=False,
            )

            close_context = {
                "market": {"symbol": "XAUUSD"},
                "features": {
                    "session": "NY",
                    "trend_direction": "UP",
                    "volatility": "HIGH",
                    "structure": {"consolidation": False, "higher_high": True},
                },
                "decision": {
                    "action": "BUY",
                    "confidence": 0.88,
                    "reason": "aggressive continuation entry",
                },
                "setup_tag": "trend_follow",
                "strategy_key": "UP*HIGH*NY_trend_follow",
                "active_skill_keys": ["UP*HIGH*NY_trend_follow"],
                "team_brief": {"risk_guardian": "can_trade=True"},
                "created_ts": 3.0,
            }
            result = asyncio.run(
                improver.learn_from_closed_trade(
                    close_context=close_context,
                    close_result={
                        "pnl": -1.4,
                        "entry_price": 100.0,
                        "exit_price": 99.1,
                        "side": "BUY",
                    },
                    score=-1,
                    strategy_state={
                        "trades": 4,
                        "wins": 1,
                        "losses": 3,
                        "lane_stage": "candidate",
                        "pending_recommendation": "demote_to_lab",
                    },
                    room_guard={"room": "UP*HIGH*NY_trend_follow", "blocked": False, "caution": True},
                )
            )

            self.assertIsNotNone(result)
            items = book.list_skills(limit=10)
            self.assertEqual(len(items), 1)
            skill = items[0]
            self.assertEqual(skill["skill_key"], "UP*HIGH*NY_trend_follow")
            self.assertTrue(skill["avoid_when"])
            self.assertTrue(skill["confidence_rules"])
            self.assertLess(skill["stats"]["total_pnl"], 0.0)

    def test_invalid_llm_review_falls_back_to_heuristic(self) -> None:
        class BrokenReviewLLM:
            async def complete_json(self, **kwargs):
                return {
                    "skill": "wrong-shape",
                    "description": "not the required schema",
                }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            book = SkillBook(
                root_dir=root / "skills",
                index_path=root / "skillbook_index.json",
                max_evidence=4,
            )
            improver = SelfImprovementEngine(
                skillbook=book,
                memory=None,
                llm=BrokenReviewLLM(),
                enabled=True,
                store_notes=False,
            )

            result = asyncio.run(
                improver.learn_from_closed_trade(
                    close_context={
                        "market": {"symbol": "XAUUSD"},
                        "features": {
                            "session": "LONDON",
                            "trend_direction": "DOWN",
                            "volatility": "HIGH",
                            "structure": {"consolidation": False},
                        },
                        "decision": {
                            "action": "SELL",
                            "confidence": 0.72,
                            "reason": "test review",
                        },
                        "setup_tag": "trend_follow",
                        "strategy_key": "DOWN*HIGH*LONDON_trend_follow",
                        "created_ts": 3.0,
                    },
                    close_result={
                        "pnl": -0.9,
                        "entry_price": 100.0,
                        "exit_price": 100.9,
                        "side": "SELL",
                    },
                    score=-1,
                )
            )

            self.assertIsNotNone(result)
            self.assertEqual(result["skill_key"], "DOWN*HIGH*LONDON_trend_follow")
            self.assertIn("avoid_when", result)
            self.assertTrue(result["avoid_when"])


if __name__ == "__main__":
    unittest.main()
