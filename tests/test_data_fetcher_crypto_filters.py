import unittest
from unittest.mock import MagicMock, patch

from market.data_fetcher import CryptoProvider


class CryptoUniverseFilterTests(unittest.TestCase):
    def test_get_top_volume_pairs_excludes_fiat_and_stable_bases(self):
        provider = CryptoProvider("binance")
        fake_spot = MagicMock()
        fake_spot.load_markets.return_value = {}
        fake_spot.fetch_tickers.return_value = {
            "BTC/USDT": {"quoteVolume": 1000},
            "ETH/USDT": {"quoteVolume": 900},
            "EUR/USDT": {"quoteVolume": 950},
            "FDUSD/USDT": {"quoteVolume": 980},
            "DOGE/USDT": {"quoteVolume": 700},
        }
        provider._spot = fake_spot
        provider._markets_cache = {}
        provider._markets_ts = 9e9

        with patch("market.data_fetcher.config.PRIORITY_PAIRS", ["BTC/USDT", "ETH/USDT"]), \
             patch("market.data_fetcher.config.CRYPTO_SNIPER_EXCLUDE_FIAT_BASES", True), \
             patch("market.data_fetcher.config.CRYPTO_SNIPER_EXCLUDE_STABLE_BASES", True), \
             patch("market.data_fetcher.config.CRYPTO_SNIPER_MT5_TRADABLE_ONLY", False), \
             patch("market.data_fetcher.config.get_crypto_sniper_exclude_bases", return_value={"EUR", "FDUSD"}):
            pairs = provider.get_top_volume_pairs(n=5)

        self.assertIn("BTC/USDT", pairs)
        self.assertIn("ETH/USDT", pairs)
        self.assertIn("DOGE/USDT", pairs)
        self.assertNotIn("EUR/USDT", pairs)
        self.assertNotIn("FDUSD/USDT", pairs)

    def test_get_top_volume_pairs_mt5_tradable_only_filter(self):
        provider = CryptoProvider("binance")
        fake_spot = MagicMock()
        fake_spot.load_markets.return_value = {}
        fake_spot.fetch_tickers.return_value = {
            "BTC/USDT": {"quoteVolume": 1000},
            "ETH/USDT": {"quoteVolume": 900},
            "DOGE/USDT": {"quoteVolume": 800},
            "SOL/USDT": {"quoteVolume": 700},
        }
        provider._spot = fake_spot
        provider._markets_cache = {}
        provider._markets_ts = 9e9

        with patch("market.data_fetcher.config.PRIORITY_PAIRS", ["BTC/USDT", "ETH/USDT"]), \
             patch("market.data_fetcher.config.CRYPTO_SNIPER_EXCLUDE_FIAT_BASES", True), \
             patch("market.data_fetcher.config.CRYPTO_SNIPER_EXCLUDE_STABLE_BASES", True), \
             patch("market.data_fetcher.config.CRYPTO_SNIPER_MT5_TRADABLE_ONLY", True), \
             patch("market.data_fetcher.config.MT5_ENABLED", True), \
             patch("market.data_fetcher.config.get_crypto_sniper_exclude_bases", return_value=set()), \
             patch("execution.mt5_executor.mt5_executor.filter_tradable_signal_symbols", return_value={
                 "ok": True,
                 "connected": True,
                 "tradable": ["BTC/USDT", "SOL/USDT"],
                 "unmapped": ["ETH/USDT", "DOGE/USDT"],
             }), \
             patch("execution.mt5_executor.mt5_executor.resolve_symbol", side_effect=lambda s: {"BTC/USDT": "BTCUSD", "ETH/USDT": None}.get(s, None)):
            pairs = provider.get_top_volume_pairs(n=5)

        self.assertIn("BTC/USDT", pairs)
        self.assertIn("SOL/USDT", pairs)
        self.assertNotIn("DOGE/USDT", pairs)
        self.assertNotIn("ETH/USDT", pairs)  # priority pair blocked if not tradable


if __name__ == "__main__":
    unittest.main()
