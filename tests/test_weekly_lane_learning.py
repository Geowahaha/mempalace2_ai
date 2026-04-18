from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from trading_ai.core.weekly_lane_learning import (
    _simulate_blocked_monitor_outcomes,
    build_weekly_lane_profile,
)


class _FakeMemory:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = list(rows)

    def list_all_structured_experiences(self) -> List[Dict[str, Any]]:
        return list(self._rows)


def _memory_row(
    *,
    created_ts: float,
    strategy_key: str,
    score: int,
    pnl: float,
    memory_type: str = "trade_journal",
    blocker_bucket: str = "",
) -> Dict[str, Any]:
    body = {
        "market": {"symbol": "XAUUSD"},
        "features": {
            "symbol": "XAUUSD",
            "session": "LONDON",
            "trend_direction": "UP",
            "volatility": "HIGH",
        },
        "decision": {"action": "BUY", "confidence": 0.7, "reason": "test"},
        "result": {"pnl": pnl},
        "score": score,
        "setup_tag": "trend_follow",
        "strategy_key": strategy_key,
    }
    meta = {
        "symbol": "XAUUSD",
        "session": "LONDON",
        "trend_direction": "UP",
        "volatility": "HIGH",
        "setup_tag": "trend_follow",
        "strategy_key": strategy_key,
        "created_ts": created_ts,
        "memory_type": memory_type,
        "outcome_score": float(score),
        "pnl": float(pnl),
    }
    if blocker_bucket:
        meta["probe_blocker_bucket"] = blocker_bucket
    return {"document": json.dumps(body), "metadata": meta}


class WeeklyLaneLearningTests(unittest.TestCase):
    def test_simulated_blocked_monitor_outcomes(self) -> None:
        rows = [
            {
                "updated_utc": "2026-04-17T10:00:00Z",
                "mid": 100.0,
                "anticipated_action": "BUY",
                "anticipated_strategy_key": "lane_buy",
                "final_action": "HOLD",
                "final_reason": "foo|pattern_block:sample",
            },
            {
                "updated_utc": "2026-04-17T10:01:00Z",
                "mid": 101.0,
                "anticipated_action": "BUY",
                "anticipated_strategy_key": "lane_buy",
                "final_action": "HOLD",
                "final_reason": "noop",
            },
            {
                "updated_utc": "2026-04-17T10:02:00Z",
                "mid": 102.0,
                "anticipated_action": "BUY",
                "anticipated_strategy_key": "lane_buy",
                "final_action": "HOLD",
                "final_reason": "noop",
            },
            {
                "updated_utc": "2026-04-17T10:03:00Z",
                "mid": 100.0,
                "anticipated_action": "SELL",
                "anticipated_strategy_key": "lane_sell",
                "final_action": "HOLD",
                "final_reason": "bar|strategy_disabled:sample",
            },
            {
                "updated_utc": "2026-04-17T10:04:00Z",
                "mid": 102.0,
                "anticipated_action": "SELL",
                "anticipated_strategy_key": "lane_sell",
                "final_action": "HOLD",
                "final_reason": "noop",
            },
            {
                "updated_utc": "2026-04-17T10:05:00Z",
                "mid": 101.0,
                "anticipated_action": "SELL",
                "anticipated_strategy_key": "lane_sell",
                "final_action": "HOLD",
                "final_reason": "noop",
            },
        ]
        stats = _simulate_blocked_monitor_outcomes(rows, lookahead_steps=2, move_threshold_pct=0.005)

        self.assertEqual(stats["lane_buy"]["blocked_events"], 1)
        self.assertEqual(stats["lane_buy"]["missed_opportunities"], 1)
        self.assertEqual(stats["lane_buy"]["prevented_bad"], 0)
        self.assertEqual(stats["lane_sell"]["blocked_events"], 1)
        self.assertEqual(stats["lane_sell"]["missed_opportunities"], 0)
        self.assertEqual(stats["lane_sell"]["prevented_bad"], 1)

    def test_build_profile_classifies_good_bad_and_opportunity_lanes(self) -> None:
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
        now_ts = now.timestamp()
        rows = [
            _memory_row(created_ts=now_ts - 3600, strategy_key="lane_good", score=1, pnl=1.0),
            _memory_row(created_ts=now_ts - 3500, strategy_key="lane_good", score=1, pnl=0.8),
            _memory_row(created_ts=now_ts - 3400, strategy_key="lane_good", score=-1, pnl=-0.2),
            _memory_row(created_ts=now_ts - 3300, strategy_key="lane_bad", score=1, pnl=0.2),
            _memory_row(created_ts=now_ts - 3200, strategy_key="lane_bad", score=-1, pnl=-0.9),
            _memory_row(created_ts=now_ts - 3100, strategy_key="lane_bad", score=-1, pnl=-0.7),
            _memory_row(
                created_ts=now_ts - 3000,
                strategy_key="lane_probe",
                score=1,
                pnl=0.1,
                memory_type="shadow_probe",
                blocker_bucket="pattern_block",
            ),
            _memory_row(
                created_ts=now_ts - 2900,
                strategy_key="lane_probe",
                score=1,
                pnl=0.2,
                memory_type="shadow_probe",
                blocker_bucket="strategy_disabled",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            monitor_path = Path(tmp) / "monitor.ndjson"
            monitor_path.write_text("", encoding="utf-8")
            profile = build_weekly_lane_profile(
                memory=_FakeMemory(rows),
                symbol="XAUUSD",
                monitor_history_path=monitor_path,
                dexter_db_path=None,
                min_trades=3,
                good_win_rate=0.58,
                bad_loss_rate=0.6,
                bad_pnl_threshold=0.0,
                lookahead_steps=5,
                move_threshold_pct=0.001,
                now_utc=now,
            )

        lanes = profile["mempalace_strategy_lanes"]
        self.assertEqual(lanes["lane_good"]["classification"], "good")
        self.assertEqual(lanes["lane_bad"]["classification"], "bad")
        self.assertEqual(lanes["lane_probe"]["classification"], "opportunity")
        self.assertIn("lane_good", profile["promote_lanes"])
        self.assertIn("lane_bad", profile["block_lanes"])
        self.assertIn("lane_probe", profile["probe_lanes"])

    def test_build_profile_reads_dexter_deals_this_week(self) -> None:
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
        in_week_a = "2026-04-16T01:00:00Z"
        in_week_b = "2026-04-17T02:00:00Z"
        out_week = "2026-04-10T02:00:00Z"

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ctrader_openapi.db"
            con = sqlite3.connect(db_path)
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE ctrader_deals (
                    execution_utc TEXT,
                    source TEXT,
                    lane TEXT,
                    symbol TEXT,
                    direction TEXT,
                    pnl_usd REAL
                )
                """
            )
            cur.executemany(
                "INSERT INTO ctrader_deals VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (in_week_a, "scalp_xauusd:winner", "winner", "XAUUSD", "long", 2.5),
                    (in_week_b, "scalp_xauusd:winner", "winner", "XAUUSD", "short", -1.0),
                    (out_week, "scalp_xauusd:winner", "winner", "XAUUSD", "long", 3.0),
                    (in_week_b, "scalp_btcusd:canary", "canary", "BTCUSD", "long", 5.0),
                ],
            )
            con.commit()
            con.close()

            monitor_path = Path(tmp) / "monitor.ndjson"
            monitor_path.write_text("", encoding="utf-8")
            profile = build_weekly_lane_profile(
                memory=_FakeMemory([]),
                symbol="XAUUSD",
                monitor_history_path=monitor_path,
                dexter_db_path=db_path,
                now_utc=now,
            )

        summary = profile["summary"]
        self.assertEqual(summary["dexter_deal_count"], 2)
        self.assertEqual(summary["dexter_family_lanes"], 1)
        self.assertIn("scalp_xauusd:winner::winner", profile["dexter_family_lanes"])


if __name__ == "__main__":
    unittest.main()

