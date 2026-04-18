import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from trading_ai.backtest import (
    GapSummary,
    HistoricalBar,
    _apply_skill_feedback,
    _apply_loss_streak_soft_gate,
    _aggregate_quote_rows,
    _date_window_to_utc,
    _loss_streak_override_payload,
    _reason_bucket,
    _requested_trade_volume,
    merge_bars,
    summarize_gaps,
)
from trading_ai.core.agent import Decision
from trading_ai.core.skillbook import SkillMatch


UTC = timezone.utc


class BacktestHelpersTest(unittest.TestCase):
    def _override_settings(self) -> SimpleNamespace:
        return SimpleNamespace(
            loss_streak_override_enabled=True,
            loss_streak_override_min_shadow_trades=3,
            loss_streak_override_min_shadow_win_rate=0.54,
            loss_streak_override_min_skill_trades=3,
            loss_streak_override_min_skill_edge=0.1,
            loss_streak_override_confidence_penalty=0.06,
            soft_gate_min_confidence=0.58,
        )

    def test_date_window_uses_local_timezone(self) -> None:
        start_utc, end_utc, meta = _date_window_to_utc("2026-04-06", "2026-04-11", "Asia/Bangkok")
        self.assertEqual(meta["start_utc"], "2026-04-05T17:00:00Z")
        self.assertEqual(meta["end_utc_exclusive"], "2026-04-11T17:00:00Z")
        self.assertEqual(start_utc.tzinfo, UTC)
        self.assertEqual(end_utc.tzinfo, UTC)

    def test_aggregate_quote_rows_buckets_5m(self) -> None:
        rows = [
            (datetime(2026, 4, 6, 0, 0, 10, tzinfo=UTC), 3000.0, 3000.2),
            (datetime(2026, 4, 6, 0, 3, 0, tzinfo=UTC), 3001.0, 3001.2),
            (datetime(2026, 4, 6, 0, 7, 0, tzinfo=UTC), 2999.5, 2999.7),
        ]
        bars = _aggregate_quote_rows(rows, timeframe_sec=300, source="spot_ticks")
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[0].ts_utc, "2026-04-06T00:00:00Z")
        self.assertAlmostEqual(bars[0].open, 3000.1)
        self.assertAlmostEqual(bars[0].close, 3001.1)
        self.assertEqual(bars[1].ts_utc, "2026-04-06T00:05:00Z")
        self.assertAlmostEqual(bars[1].open, 2999.6)

    def test_merge_prefers_higher_priority_source(self) -> None:
        candle = HistoricalBar(
            ts_unix=datetime(2026, 4, 6, 0, 0, tzinfo=UTC).timestamp(),
            ts_utc="2026-04-06T00:00:00Z",
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            bid=1.45,
            ask=1.55,
            spread=0.1,
            volume=10.0,
            source="candle_db",
        )
        spot = HistoricalBar(
            ts_unix=candle.ts_unix,
            ts_utc=candle.ts_utc,
            open=2.0,
            high=3.0,
            low=1.5,
            close=2.5,
            bid=2.45,
            ask=2.55,
            spread=0.1,
            volume=20.0,
            source="spot_ticks",
        )
        merged = merge_bars([candle], [spot])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].source, "spot_ticks")
        self.assertAlmostEqual(merged[0].close, 2.5)

    def test_summarize_gaps_finds_internal_gap(self) -> None:
        bars = [
            HistoricalBar(
                ts_unix=datetime(2026, 4, 6, 0, 0, tzinfo=UTC).timestamp(),
                ts_utc="2026-04-06T00:00:00Z",
                open=1.0,
                high=1.0,
                low=1.0,
                close=1.0,
                bid=0.95,
                ask=1.05,
                spread=0.1,
                volume=1.0,
                source="spot_ticks",
            ),
            HistoricalBar(
                ts_unix=datetime(2026, 4, 6, 0, 15, tzinfo=UTC).timestamp(),
                ts_utc="2026-04-06T00:15:00Z",
                open=1.0,
                high=1.0,
                low=1.0,
                close=1.0,
                bid=0.95,
                ask=1.05,
                spread=0.1,
                volume=1.0,
                source="spot_ticks",
            ),
        ]
        gaps = summarize_gaps(
            bars,
            start_utc=datetime(2026, 4, 6, 0, 0, tzinfo=UTC),
            end_utc=datetime(2026, 4, 6, 0, 20, tzinfo=UTC),
            timeframe_sec=300,
        )
        self.assertTrue(gaps)
        self.assertIsInstance(gaps[0], GapSummary)
        self.assertEqual(gaps[0].start_utc, "2026-04-06T00:05:00Z")
        self.assertEqual(gaps[0].missing_bars, 2)

    def test_reason_bucket_collapses_pattern_block(self) -> None:
        bucket = _reason_bucket("heuristic_fallback:trend=UP|pattern_block:thin_sample")
        self.assertEqual(bucket, "pattern_block")

    def test_skill_feedback_blocks_negative_lane(self) -> None:
        decision = Decision(
            action="BUY",
            confidence=0.74,
            reason="heuristic_fallback:trend=UP",
            raw={},
        )
        match = SkillMatch(
            skill_key="UP*HIGH*ASIA_trend_follow",
            score=6.2,
            title="Bad lane",
            summary="Avoid this lane.",
            use_when=[],
            avoid_when=["Repeated losses."],
            guardrails=[],
            confidence_rules=[],
            stats={"trades_seen": 4, "wins": 0, "losses": 4, "win_rate": 0.0, "risk_adjusted_score": -0.8},
            triggers={},
            file_path="",
            fit_reasons=["strategy_key"],
        )
        adjusted, feedback = _apply_skill_feedback(
            decision,
            anticipated_action="BUY",
            matches=[match],
            min_trade_confidence=0.65,
        )
        self.assertEqual(adjusted.action, "HOLD")
        self.assertTrue(feedback["applied"])
        self.assertEqual(feedback["type"], "block")

    def test_skill_feedback_can_promote_low_conf_hold(self) -> None:
        decision = Decision(
            action="HOLD",
            confidence=0.63,
            reason="low_confidence_floor(0.630<0.65)",
            raw={},
        )
        match = SkillMatch(
            skill_key="UP*HIGH*LONDON_trend_follow",
            score=6.8,
            title="Good lane",
            summary="Promote this lane when it repeats.",
            use_when=["Trend aligns."],
            avoid_when=[],
            guardrails=[],
            confidence_rules=[],
            stats={"trades_seen": 3, "wins": 2, "losses": 1, "win_rate": 0.6667, "risk_adjusted_score": 0.35},
            triggers={},
            file_path="",
            fit_reasons=["strategy_key"],
        )
        adjusted, feedback = _apply_skill_feedback(
            decision,
            anticipated_action="BUY",
            matches=[match],
            min_trade_confidence=0.65,
        )
        self.assertEqual(adjusted.action, "BUY")
        self.assertGreaterEqual(adjusted.confidence, 0.65)
        self.assertTrue(feedback["applied"])
        self.assertEqual(feedback["type"], "promotion")

    def test_loss_streak_override_uses_promoted_shadow_lane(self) -> None:
        match = SkillMatch(
            skill_key="DOWN*HIGH*NY_trend_follow",
            score=6.1,
            title="Promoted lane",
            summary="Shadow evidence is positive.",
            use_when=[],
            avoid_when=[],
            guardrails=[],
            confidence_rules=[],
            stats={"trades_seen": 6, "wins": 4, "losses": 2, "win_rate": 0.6667, "risk_adjusted_score": 0.12},
            triggers={},
            file_path="",
            fit_reasons=["strategy_key"],
        )
        override = _loss_streak_override_payload(
            veto="loss_streak_3>=3",
            anticipated_action="SELL",
            strategy_key="DOWN*HIGH*NY_trend_follow",
            strategy_state={
                "pending_recommendation": "promote_from_shadow",
                "shadow_trades": 4,
                "shadow_wins": 3,
                "shadow_losses": 1,
                "shadow_total_profit": 0.08,
            },
            matches=[match],
            settings=self._override_settings(),
        )
        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "promote_from_shadow")

        softened = _apply_loss_streak_soft_gate(
            Decision(action="SELL", confidence=0.74, reason="heuristic_fallback:trend=DOWN", raw={}),
            override=override,
            settings=self._override_settings(),
            min_trade_confidence=0.58,
        )
        self.assertEqual(softened.action, "SELL")
        self.assertIn("loss_streak_soft_gate", softened.reason)
        self.assertGreaterEqual(softened.confidence, 0.58)

    def test_requested_trade_volume_respects_min_lot_floor(self) -> None:
        settings = SimpleNamespace(default_volume=0.01, probation_trade_volume_fraction=0.35, risk_min_order_lot=0.01)
        requested = _requested_trade_volume(
            Decision(action="SELL", confidence=0.7, reason="heuristic|loss_streak_soft_gate:test", raw={}),
            settings,
        )
        self.assertEqual(requested, 0.01)


if __name__ == "__main__":
    unittest.main()
