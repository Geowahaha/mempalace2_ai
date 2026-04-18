import unittest
from unittest.mock import patch
import pandas as pd

from analysis.signals import TradeSignal
from config import config
import scanners.stock_scanner as stock_scanner_module
from scanners.stock_scanner import GlobalStockScanner, StockOpportunity


def make_signal(
    symbol: str = "TEST",
    confidence: float = 72.0,
    direction: str = "long",
    rsi: float = 56.0,
    trend: str = "bullish",
    edge: float = 20.0,
) -> TradeSignal:
    return TradeSignal(
        symbol=symbol,
        direction=direction,
        confidence=confidence,
        entry=100.0,
        stop_loss=99.0,
        take_profit_1=101.0,
        take_profit_2=102.0,
        take_profit_3=103.0,
        risk_reward=2.0,
        timeframe="1h",
        session="new_york",
        trend=trend,
        rsi=rsi,
        atr=1.0,
        pattern="TEST",
        reasons=[],
        warnings=[],
        raw_scores={"edge": edge},
    )


def make_opp(symbol: str, vol: float, quality_score: int, confidence: float = 72.0) -> StockOpportunity:
    return StockOpportunity(
        signal=make_signal(symbol=symbol, confidence=confidence),
        market="US",
        setup_type="BULLISH_OB_BOUNCE",
        base_setup_type="OB_BOUNCE",
        vol_vs_avg=vol,
        quality_score=quality_score,
        quality_tag="LOW",
    )


class StockQualityFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scanner = GlobalStockScanner(max_workers=1)

    def test_quality_tag_high_requires_volume(self):
        # Lower min vol threshold temporarily so score can still reach 3 with vol<1.0.
        with patch.object(config, "STOCK_MIN_VOL_RATIO", 0.5), patch.object(config, "STOCK_MIN_EDGE", 15), patch.object(config, "STOCK_MIN_MOMENTUM_RSI", 53):
            sig = make_signal(confidence=75.0, direction="long", rsi=58.0, trend="bullish", edge=22.0)
            score, tag = self.scanner._stock_quality_score(sig, vol_ratio=0.84)

        self.assertGreaterEqual(score, 3)
        self.assertNotEqual(tag, "HIGH")
        self.assertEqual(tag, "LOW")

    def test_filter_quality_respects_balanced_thresholds(self):
        with patch.object(config, "STOCK_MIN_VOL_RATIO", 0.9):
            opps = [
                make_opp("PASS", vol=0.95, quality_score=2),
                make_opp("FAIL_VOL", vol=0.89, quality_score=3),
                make_opp("FAIL_SCORE", vol=1.2, quality_score=1),
            ]
            passed = self.scanner.filter_quality(opps, min_score=2)

        self.assertEqual([o.signal.symbol for o in passed], ["PASS"])

    def test_filter_watchlist_excludes_ultra_low_volume(self):
        with patch.object(config, "WATCHLIST_MIN_VOL_RATIO", 0.5), patch.object(config, "WATCHLIST_MIN_CONFIDENCE", 70):
            opps = [
                make_opp("ULTRA_LOW_1", vol=0.03, quality_score=2, confidence=75),
                make_opp("ULTRA_LOW_2", vol=0.10, quality_score=2, confidence=75),
                make_opp("PASS", vol=0.55, quality_score=1, confidence=75),
            ]
            passed = self.scanner.filter_watchlist(opps)

        self.assertEqual([o.signal.symbol for o in passed], ["PASS"])

    def test_watchlist_confidence_floor(self):
        with patch.object(config, "WATCHLIST_MIN_VOL_RATIO", 0.5), patch.object(config, "WATCHLIST_MIN_CONFIDENCE", 70):
            opps = [
                make_opp("LOW_CONF", vol=0.8, quality_score=2, confidence=69),
                make_opp("PASS", vol=0.8, quality_score=2, confidence=71),
            ]
            passed = self.scanner.filter_watchlist(opps)

        self.assertEqual([o.signal.symbol for o in passed], ["PASS"])

    def test_scan_us_value_trend_ranks_better_value_higher(self):
        opp_a = make_opp("AAA", vol=1.2, quality_score=2, confidence=78)
        opp_b = make_opp("BBB", vol=1.2, quality_score=2, confidence=78)
        opp_a.signal.direction = "long"
        opp_b.signal.direction = "long"
        opp_a.signal.trend = "bullish"
        opp_b.signal.trend = "bullish"
        opp_a.dollar_volume = 30_000_000
        opp_b.dollar_volume = 30_000_000
        opp_a.setup_win_rate = 0.60
        opp_b.setup_win_rate = 0.60

        info_map = {
            "AAA": {"pe_ratio": 12.0, "52w_high": 120.0, "52w_low": 80.0, "sector": "Tech"},
            "BBB": {"pe_ratio": 45.0, "52w_high": 120.0, "52w_low": 80.0, "sector": "Tech"},
        }

        with patch.object(self.scanner, "scan_us", return_value=[opp_b, opp_a]), \
             patch.object(self.scanner, "_get_stock_info_cached", side_effect=lambda s: info_map[s]), \
             patch.object(config, "VI_MIN_CONFIDENCE", 70), \
             patch.object(config, "VI_MIN_VOL_RATIO", 0.7), \
             patch.object(config, "VI_MIN_DOLLAR_VOLUME", 8_000_000), \
             patch.object(config, "VI_MAX_PE_RATIO", 35), \
             patch.object(config, "VI_MAX_CANDIDATES", 10):
            out = self.scanner.scan_us_value_trend(top_n=2)

        self.assertEqual(len(out), 2)
        self.assertEqual(out[0].signal.symbol, "AAA")
        self.assertIn("vi_total_score", out[0].signal.raw_scores)
        self.assertIn("vi_primary_profile", out[0].signal.raw_scores)
        self.assertIn("vi_reasons_detailed", out[0].signal.raw_scores)

    def test_vi_profile_scores_classifies_buffett_quality(self):
        opp = make_opp("QUAL", vol=1.4, quality_score=3, confidence=80)
        opp.signal.direction = "long"
        opp.signal.trend = "bullish"
        opp.signal.rsi = 60.0
        opp.dollar_volume = 60_000_000
        opp.setup_win_rate = 0.62
        opp.base_setup_type = "OB_BOUNCE"
        opp.pe_ratio = 16.0
        opp.week_52_low = 80.0
        opp.week_52_high = 120.0
        opp.signal.entry = 95.0
        info = {
            "pe_ratio": 16.0,
            "forward_pe": 14.5,
            "price_to_book": 2.8,
            "enterprise_to_ebitda": 10.0,
            "return_on_equity": 0.24,
            "return_on_assets": 0.10,
            "gross_margin": 0.55,
            "operating_margin": 0.24,
            "profit_margin": 0.18,
            "revenue_growth": 0.10,
            "earnings_growth": 0.14,
            "debt_to_equity": 45.0,
            "current_ratio": 1.7,
            "quick_ratio": 1.4,
            "free_cashflow": 1_000_000_000,
            "operating_cashflow": 1_500_000_000,
            "total_revenue": 10_000_000_000,
            "market_cap": 120_000_000_000,
        }
        meta = self.scanner._vi_profile_scores(opp, info)
        self.assertEqual(meta["primary_profile"], "BUFFETT")
        self.assertGreater(meta["compounder_score"], meta["turnaround_score"])
        self.assertTrue(any("Buffett-inspired" in r for r in meta["detailed_reasons"]))

    def test_vi_profile_scores_classifies_turnaround(self):
        opp = make_opp("TURN", vol=1.6, quality_score=2, confidence=77)
        opp.signal.direction = "long"
        opp.signal.trend = "bullish"
        opp.signal.rsi = 58.0
        opp.dollar_volume = 35_000_000
        opp.setup_win_rate = 0.62
        opp.base_setup_type = "CHOCH"
        opp.pe_ratio = None
        opp.week_52_low = 10.0
        opp.week_52_high = 40.0
        opp.signal.entry = 18.0
        info = {
            "pe_ratio": None,
            "price_to_book": 1.2,
            "return_on_equity": 0.04,
            "operating_margin": 0.03,
            "profit_margin": 0.01,
            "revenue_growth": 0.22,
            "earnings_growth": 0.35,
            "debt_to_equity": 110.0,
            "current_ratio": 1.2,
            "free_cashflow": 50_000_000,
            "operating_cashflow": 120_000_000,
            "total_revenue": 1_200_000_000,
            "market_cap": 4_500_000_000,
        }
        meta = self.scanner._vi_profile_scores(opp, info)
        self.assertEqual(meta["primary_profile"], "TURNAROUND")
        self.assertGreater(meta["turnaround_score"], meta["compounder_score"])
        self.assertTrue(any("Turnaround" in r for r in meta["detailed_reasons"]))

    def test_yahoo_bad_symbol_blacklist_cache_skips_repeated_failures(self):
        stock_scanner_module._YF_BAD_SYMBOL_UNTIL.clear()
        stock_scanner_module._YF_BAD_SYMBOL_REASON.clear()
        stock_scanner_module._YF_FAIL_COUNTS.clear()

        fake_ticker = type("FakeTicker", (), {"history": lambda self, **kwargs: pd.DataFrame()})
        with patch.object(config, "STOCK_YF_EMPTY_FAILS_TO_BLACKLIST", 2), \
             patch.object(config, "STOCK_YF_BAD_SYMBOL_CACHE_TTL_SEC", 3600), \
             patch.object(stock_scanner_module.yf, "Ticker", return_value=fake_ticker()) as ticker_ctor:
            self.assertIsNone(stock_scanner_module.fetch_stock_ohlcv("DELISTED.BK", "1h", 50))
            self.assertIsNone(stock_scanner_module.fetch_stock_ohlcv("DELISTED.BK", "1h", 50))
            # Third call should short-circuit from blacklist cache and not call yfinance again.
            self.assertIsNone(stock_scanner_module.fetch_stock_ohlcv("DELISTED.BK", "1h", 50))

        self.assertEqual(ticker_ctor.call_count, 2)
        self.assertIn("DELISTED.BK", stock_scanner_module._YF_BAD_SYMBOL_UNTIL)


if __name__ == "__main__":
    unittest.main()
