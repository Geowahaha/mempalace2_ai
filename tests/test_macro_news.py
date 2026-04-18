import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import scheduler as scheduler_module
from market.macro_news import MacroHeadline, MacroNewsMonitor


class MacroNewsTests(unittest.TestCase):
    def test_score_star_mapping_and_filter_parsing(self):
        mon = MacroNewsMonitor()
        self.assertEqual(mon.score_to_stars(2), "*")
        self.assertEqual(mon.score_to_stars(5), "**")
        self.assertEqual(mon.score_to_stars(9), "***")
        self.assertEqual(mon.stars_to_min_score("*"), 1)
        self.assertEqual(mon.stars_to_min_score("**"), 5)
        self.assertEqual(mon.stars_to_min_score("***"), 8)
        self.assertEqual(mon.stars_to_min_score("high"), 8)
        self.assertIsNone(mon.stars_to_min_score("invalid"))

    def test_macro_feed_parses_trump_tariff_headline(self):
        mon = MacroNewsMonitor()
        now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        xml = f"""
<rss><channel>
  <item>
    <title>Trump says new tariff plan may expand</title>
    <link>https://example.com/trump-tariff</link>
    <pubDate>{now}</pubDate>
    <source>Reuters</source>
  </item>
</channel></rss>
""".strip()
        with patch.object(mon, "_download", return_value=xml):
            heads = mon.fetch_headlines(force=True)
        self.assertEqual(len(heads), 1)
        self.assertGreaterEqual(heads[0].score, 6)
        self.assertTrue(any(t in heads[0].themes for t in ("trump_policy", "tariff_trade")))
        self.assertIn("Gold", heads[0].impact_hint)

    def test_dynamic_theme_weights_adjust_scoring_but_remain_bounded(self):
        mon = MacroNewsMonitor()
        base_score, base_themes = mon._score_themes("Trump tariff plan expands")
        self.assertTrue(any(t in base_themes for t in ("trump_policy", "tariff_trade")))
        mon.set_dynamic_theme_weights({"trump_policy": 0.5, "tariff_trade": 2.0})
        adj_score, _ = mon._score_themes("Trump tariff plan expands")
        self.assertNotEqual(adj_score, base_score)
        snap = mon.dynamic_theme_weights_snapshot()
        self.assertAlmostEqual(float(snap["tariff_trade"]["weight_mult"]), 2.0, places=2)
        self.assertGreaterEqual(adj_score, 1)

    def test_macro_feed_assigns_source_quality_and_verified_state(self):
        mon = MacroNewsMonitor()
        now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        xml = f"""
<rss><channel>
  <item>
    <title>Fed announces emergency liquidity action in official statement</title>
    <link>https://example.com/fed</link>
    <pubDate>{now}</pubDate>
    <source>Reuters</source>
  </item>
</channel></rss>
""".strip()
        with patch.object(mon, "_download", return_value=xml):
            heads = mon.fetch_headlines(force=True)
        self.assertEqual(len(heads), 1)
        h = heads[0]
        self.assertGreaterEqual(float(h.source_quality), 0.9)
        self.assertEqual(str(h.source_tier), "trusted")
        self.assertEqual(str(h.verification), "confirmed")

    def test_macro_feed_marks_rumor_and_penalizes_score(self):
        mon = MacroNewsMonitor()
        now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        title = "Unconfirmed reports say missile strike may expand conflict"
        xml = f"""
<rss><channel>
  <item>
    <title>{title}</title>
    <link>https://example.com/rumor</link>
    <pubDate>{now}</pubDate>
    <source>randomblog</source>
  </item>
</channel></rss>
""".strip()
        base_score, _ = mon._score_themes(title)
        with patch.object(mon, "_download", return_value=xml):
            heads = mon.fetch_headlines(force=True)
        self.assertEqual(len(heads), 1)
        h = heads[0]
        self.assertIn(str(h.verification), {"rumor", "mixed"})
        self.assertLessEqual(int(h.score), int(base_score))

    def test_scheduler_dedupes_macro_alerts(self):
        dexter = scheduler_module.DexterScheduler()
        h = MacroHeadline(
            headline_id="h123",
            title="Trump tariff headline",
            link="https://example.com/a",
            source="Reuters",
            published_utc=datetime.now(timezone.utc) - timedelta(minutes=10),
            score=8,
            themes=["trump_policy", "tariff_trade"],
            impact_hint="Policy/trade shock risk",
        )
        with patch.object(scheduler_module.config, "MACRO_NEWS_ENABLED", True), \
             patch.object(scheduler_module.config, "MACRO_NEWS_LOOKBACK_HOURS", 24), \
             patch.object(scheduler_module.config, "MACRO_NEWS_MIN_SCORE", 6), \
             patch.object(scheduler_module.macro_news, "high_impact_headlines", return_value=[h]), \
             patch.object(scheduler_module.notifier, "send_macro_news_alert", return_value=True) as send_alert:
            dexter._run_macro_news_watch()
            dexter._run_macro_news_watch()
        self.assertEqual(send_alert.call_count, 1)


if __name__ == "__main__":
    unittest.main()
