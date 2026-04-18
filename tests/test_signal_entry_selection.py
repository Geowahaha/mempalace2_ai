import unittest

from analysis.signals import SignalGenerator
from analysis.smc import FairValueGap, OrderBlock, SMCContext


class SignalEntrySelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gen = SignalGenerator(min_confidence=0)

    def test_long_uses_bullish_ob_retest_entry(self):
        ctx = SMCContext(
            nearest_ob=OrderBlock(
                direction="bullish",
                high=99.0,
                low=97.5,
                open=98.9,
                close=97.8,
                index=1,
                bar_time=None,
                strength=0.8,
                tested=False,
                broken=False,
            )
        )
        entry, note = self.gen._select_advantaged_entry(
            direction="long",
            close=100.0,
            atr=2.0,
            smc_ctx=ctx,
        )

        self.assertAlmostEqual(entry, 98.475, places=6)
        self.assertIsNotNone(note)
        self.assertIn("Bullish OB retest", note)

    def test_short_uses_bearish_fvg_fill_entry(self):
        ctx = SMCContext(
            nearest_fvg=FairValueGap(
                direction="bearish",
                upper=101.6,
                lower=100.8,
                index=2,
                bar_time=None,
                filled=False,
            )
        )
        entry, note = self.gen._select_advantaged_entry(
            direction="short",
            close=100.0,
            atr=2.0,
            smc_ctx=ctx,
        )

        self.assertAlmostEqual(entry, 101.2, places=6)
        self.assertIsNotNone(note)
        self.assertIn("Bearish FVG fill", note)

    def test_fallback_to_close_when_zone_too_far(self):
        ctx = SMCContext(
            nearest_ob=OrderBlock(
                direction="bullish",
                high=92.0,
                low=90.0,
                open=91.5,
                close=90.5,
                index=3,
                bar_time=None,
                strength=0.9,
                tested=False,
                broken=False,
            )
        )
        entry, note = self.gen._select_advantaged_entry(
            direction="long",
            close=100.0,
            atr=2.0,
            smc_ctx=ctx,
        )

        self.assertEqual(entry, 100.0)
        self.assertIsNone(note)


if __name__ == "__main__":
    unittest.main()
