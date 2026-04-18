import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from market.macro_impact_tracker import MacroImpactTracker
from market.macro_news import MacroHeadline


class MacroImpactTrackerTests(unittest.TestCase):
    def test_classify_asset_and_headline_detects_confirmed_vs_priced_in(self):
        age_sec = 2 * 3600
        cls_xau, m_xau = MacroImpactTracker._classify_asset(
            "XAUUSD",
            {-15: 100.0, 0: 100.0, 5: 100.25, 15: 100.30, 60: 100.35},
            age_sec,
        )
        cls_btc, m_btc = MacroImpactTracker._classify_asset(
            "BTCUSD",
            {-15: 100.0, 0: 100.0, 5: 101.2, 15: 101.0, 60: 100.9},
            age_sec,
        )
        headline_label, summary = MacroImpactTracker._classify_headline(
            {
                "XAUUSD": {"classification": cls_xau, **m_xau},
                "BTCUSD": {"classification": cls_btc, **m_btc},
            },
            age_sec,
        )
        self.assertIn(headline_label, {"impact_confirmed", "impact_developing"})
        self.assertIn("XAUUSD", summary)
        self.assertIn("BTCUSD", summary)

        cls_pi, metrics_pi = MacroImpactTracker._classify_asset(
            "XAUUSD",
            {-15: 99.75, 0: 100.0, 5: 100.02, 15: 100.01, 60: 100.00},
            age_sec,
        )
        self.assertEqual(cls_pi, "priced_in")
        self.assertIsNotNone(metrics_pi["pre15_pct"])

    def test_build_report_uses_stored_headlines_and_samples(self):
        with tempfile.TemporaryDirectory() as td:
            tracker = MacroImpactTracker(db_path=f"{td}\\macro_impact_test.db")
            now = datetime.now(timezone.utc)
            h = MacroHeadline(
                headline_id="h123",
                title="Trump tariff headline hits markets",
                link="https://example.com/h123",
                source="Reuters",
                published_utc=now - timedelta(hours=2),
                score=9,
                themes=["tariff_trade", "trump_policy"],
                impact_hint="Policy/trade shock risk",
            )
            tracker.ingest_headlines([h])
            base = h.published_utc
            for asset, prices in {
                "XAUUSD": {-15: 100.0, 0: 100.0, 5: 100.2, 15: 100.25, 60: 100.3},
                "BTCUSD": {-15: 100.0, 0: 100.0, 5: 99.0, 15: 98.8, 60: 98.5},
                "ETHUSD": {-15: 100.0, 0: 100.0, 5: 98.7, 15: 98.5, 60: 98.2},
                "US500": {-15: 100.0, 0: 100.0, 5: 99.7, 15: 99.5, 60: 99.4},
            }.items():
                for hz, px in prices.items():
                    tracker._upsert_sample("h123", asset, hz, px, base + timedelta(minutes=hz), "test")

            report = tracker.build_report(hours=24, min_score=8, min_risk_stars="***", limit=5)
            self.assertTrue(report["ok"])
            self.assertEqual(len(report["entries"]), 1)
            entry = report["entries"][0]
            self.assertEqual(entry["risk_stars"], "***")
            self.assertIn(entry["classification"], {"impact_confirmed", "impact_developing"})
            self.assertIn("US500", entry["reaction_summary"])
            self.assertIn("tariff_trade", [ts["theme"] for ts in report["theme_stats"]])

    def test_refresh_adaptive_weights_persists_and_applies_to_macro_news(self):
        with tempfile.TemporaryDirectory() as td:
            tracker = MacroImpactTracker(db_path=f"{td}\\macro_impact_test2.db")
            now = datetime.now(timezone.utc)
            for i in range(4):
                h = MacroHeadline(
                    headline_id=f"h{i}",
                    title=f"Trump tariff headline {i}",
                    link=f"https://example.com/{i}",
                    source="Reuters",
                    published_utc=now - timedelta(hours=2 + i),
                    score=9,
                    themes=["tariff_trade", "trump_policy"],
                    impact_hint="Policy/trade shock risk",
                )
                tracker.ingest_headlines([h])
                base = h.published_utc
                # Make tariff/trump themes look impactful (confirmed)
                for asset, prices in {
                    "XAUUSD": {-15: 100.0, 0: 100.0, 5: 100.25, 15: 100.3, 60: 100.35},
                    "BTCUSD": {-15: 100.0, 0: 100.0, 5: 98.8, 15: 98.6, 60: 98.4},
                    "ETHUSD": {-15: 100.0, 0: 100.0, 5: 98.7, 15: 98.5, 60: 98.1},
                    "US500": {-15: 100.0, 0: 100.0, 5: 99.7, 15: 99.5, 60: 99.4},
                }.items():
                    for hz, px in prices.items():
                        tracker._upsert_sample(h.headline_id, asset, hz, px, base + timedelta(minutes=hz), "test")

            res = tracker.refresh_adaptive_weights()
            self.assertTrue(res["ok"])
            self.assertTrue(res["applied"])
            stored = tracker.load_theme_weights()
            self.assertIn("tariff_trade", stored)
            w = float(stored["tariff_trade"]["weight_mult"])
            self.assertGreaterEqual(w, 0.8)
            self.assertLessEqual(w, 1.25)

            from market.macro_news import macro_news
            snap = macro_news.dynamic_theme_weights_snapshot()
            self.assertIn("tariff_trade", snap)
            self.assertAlmostEqual(float(snap["tariff_trade"]["weight_mult"]), w, places=4)

    def test_build_weights_report_returns_sorted_rows_and_thresholds(self):
        with tempfile.TemporaryDirectory() as td:
            tracker = MacroImpactTracker(db_path=f"{td}\\macro_impact_test3.db")
            now = datetime.now(timezone.utc)
            for i in range(3):
                h = MacroHeadline(
                    headline_id=f"w{i}",
                    title=f"Trump tariff shock {i}",
                    link=f"https://example.com/w{i}",
                    source="Reuters",
                    published_utc=now - timedelta(hours=2 + i),
                    score=9,
                    themes=["tariff_trade"],
                    impact_hint="Policy/trade shock risk",
                )
                tracker.ingest_headlines([h])
                base = h.published_utc
                for hz, px in {-15: 100.0, 0: 100.0, 5: 100.2, 15: 100.3, 60: 100.35}.items():
                    tracker._upsert_sample(h.headline_id, "XAUUSD", hz, px, base + timedelta(minutes=hz), "test")
            tracker.refresh_adaptive_weights()
            report = tracker.build_weights_report(limit=5)
            self.assertTrue(report["ok"])
            self.assertIn("rows", report)
            self.assertGreaterEqual(report["runtime_count"], 1)
            self.assertIn("thresholds", report)
            if report["rows"]:
                self.assertIn("theme", report["rows"][0])
                self.assertIn("weight_mult", report["rows"][0])


if __name__ == "__main__":
    unittest.main()
