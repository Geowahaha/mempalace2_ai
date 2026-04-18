"""
scanners/crypto_sniper.py - Professional Crypto Sniper Scanner
Scans top-N coins for high-probability sniper entry setups:
  - Bollinger Band Squeeze Breakouts
  - RSI Divergence Setups
  - Multi-TF Trend Confluence
  - Volume Profile Breakouts
  - SMC Order Block Entries
  - Funding Rate Extremes (contrarian)
"""
import logging
import time
from typing import Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from market.data_fetcher import crypto_provider, session_manager
from analysis.technical import TechnicalAnalysis
from analysis.smc import SMCAnalyzer
from analysis.signals import SignalGenerator, TradeSignal
from config import config

logger = logging.getLogger(__name__)
ta = TechnicalAnalysis()
smc = SMCAnalyzer()
sig = SignalGenerator(min_confidence=config.MIN_SIGNAL_CONFIDENCE)


@dataclass
class CryptoOpportunity:
    signal: TradeSignal
    volume_24h: float = 0.0
    market_cap_rank: int = 0
    funding_rate: Optional[float] = None
    volatility_score: float = 0.0
    setup_type: str = ""

    @property
    def composite_score(self) -> float:
        """Combined score for ranking opportunities."""
        base = self.signal.confidence
        # Boost for volume
        vol_boost = min(5.0, self.volume_24h / 1_000_000_000 * 2)
        # Boost for high-quality setups
        setup_boost = {
            "BB_SQUEEZE_BREAKOUT": 10,
            "OB_BOUNCE": 8,
            "DIVERGENCE": 7,
            "TREND_CONTINUATION": 5,
            "CHOCH_ENTRY": 12,
            "FVG_FILL": 6,
        }.get(self.setup_type, 0)
        # Penalty for extreme funding (can reverse)
        funding_penalty = 0
        if self.funding_rate is not None:
            if abs(self.funding_rate) > 0.001:
                funding_penalty = 5
        return base + vol_boost + setup_boost - funding_penalty


class CryptoSniper:
    """
    Professional crypto sniper that scans the top-N coins and
    returns ranked, high-probability trade opportunities.
    """

    def __init__(self, max_workers: int = 8):
        self.max_workers = max_workers
        self.scan_count = 0
        self.total_signals = 0
        self._last_pairs: list[str] = []

    def detect_setup_type(self, df: pd.DataFrame if False else object) -> str:
        """Determine the primary setup type for this chart."""
        import pandas as pd
        try:
            if len(df) < 30:
                return "UNKNOWN"
            df_ta = ta.add_all(df.copy())
            last = df_ta.iloc[-1]

            # BB Squeeze Breakout
            bb_width = float(last.get("bb_width", 1))
            bb_pct = float(last.get("bb_pct", 0.5))
            vol_ratio = float(last.get("vol_ratio", 1))
            hist_bb_width = float(df_ta["bb_width"].quantile(0.2)) if "bb_width" in df_ta else 1

            if bb_width < hist_bb_width and vol_ratio > 1.5:
                return "BB_SQUEEZE_BREAKOUT"

            # ChoCH (Change of Character) - highest priority
            ctx = smc.analyze(df_ta)
            if ctx.recent_bos and ctx.recent_bos.level_type == "ChoCH":
                return "CHOCH_ENTRY"

            # OB Bounce
            if ctx.nearest_ob and not ctx.nearest_ob.tested:
                return "OB_BOUNCE"

            # FVG Fill
            if ctx.nearest_fvg and not ctx.nearest_fvg.filled:
                return "FVG_FILL"

            # RSI Divergence
            div = ta.detect_rsi_divergence(df_ta)
            if div in ("bullish_div", "bearish_div"):
                return "DIVERGENCE"

            return "TREND_CONTINUATION"

        except Exception:
            return "UNKNOWN"

    def analyze_single(self, symbol: str) -> Optional[CryptoOpportunity]:
        """Analyze a single coin and return an opportunity if found."""
        try:
            # Fetch entry and trend timeframes
            df_entry = crypto_provider.fetch_ohlcv(symbol, config.CRYPTO_ENTRY_TF, bars=200)
            if df_entry is None or len(df_entry) < 50:
                return None

            df_trend = crypto_provider.fetch_ohlcv(symbol, config.CRYPTO_TREND_TF, bars=100)
            session_info = session_manager.get_session_info()

            signal = sig.score_signal(
                df_entry=df_entry,
                df_trend=df_trend,
                symbol=symbol,
                timeframe=config.CRYPTO_ENTRY_TF,
                session_info=session_info,
            )

            if signal is None:
                return None

            # Enrich with setup type
            setup = self.detect_setup_type(df_entry)
            signal.pattern = setup if setup != "UNKNOWN" else signal.pattern

            # Get funding rate
            funding = None
            try:
                funding = crypto_provider.get_funding_rate(symbol)
                if funding is not None:
                    funding_pct = funding * 100
                    if funding_pct > 0.05 and signal.direction == "long":
                        signal.warnings.append(
                            f"⚠️ Positive funding rate ({funding_pct:.4f}%) - longs paying, "
                            "risk of squeeze"
                        )
                    elif funding_pct < -0.05 and signal.direction == "short":
                        signal.warnings.append(
                            f"⚠️ Negative funding rate ({funding_pct:.4f}%) - shorts paying, "
                            "risk of short squeeze"
                        )
                    elif abs(funding_pct) > 0.1:
                        signal.reasons.append(
                            f"✅ Extreme funding ({funding_pct:.4f}%) → contrarian opportunity"
                        )
            except Exception:
                pass

            return CryptoOpportunity(
                signal=signal,
                funding_rate=funding,
                setup_type=setup,
            )

        except Exception as e:
            logger.debug(f"analyze_single({symbol}): {e}")
            return None

    def scan(self, symbols: Optional[list[str]] = None) -> list[CryptoOpportunity]:
        """
        Scan all top coins (or a provided list) and return
        ranked list of opportunities, best first.
        """
        self.scan_count += 1
        start_time = time.time()

        if symbols is None:
            symbols = crypto_provider.get_top_volume_pairs(config.TOP_COINS_COUNT)
        self._last_pairs = symbols

        session_info = session_manager.get_session_info()
        logger.info(f"[CRYPTO SNIPER] Scan #{self.scan_count} | "
                    f"Scanning {len(symbols)} pairs | "
                    f"Sessions: {session_info['active_sessions']}")

        opportunities = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.analyze_single, sym): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    opp = future.result(timeout=30)
                    if opp is not None:
                        opportunities.append(opp)
                except Exception as e:
                    logger.debug(f"Future error for {sym}: {e}")

        # Sort by composite score
        opportunities.sort(key=lambda x: x.composite_score, reverse=True)

        elapsed = time.time() - start_time
        self.total_signals += len(opportunities)

        logger.info(
            f"[CRYPTO SNIPER] ✅ Scan complete in {elapsed:.1f}s | "
            f"Found {len(opportunities)} opportunities from {len(symbols)} pairs"
        )

        return opportunities

    def quick_scan(self, symbols: Optional[list[str]] = None) -> list[CryptoOpportunity]:
        """Fast scan on fewer coins for rapid signal delivery."""
        if symbols is None:
            symbols = config.PRIORITY_PAIRS
        return self.scan(symbols)

    def get_top_n(self, n: int = 5) -> list[CryptoOpportunity]:
        """Run full scan and return top N opportunities."""
        all_opps = self.scan()
        return all_opps[:n]

    def get_stats(self) -> dict:
        return {
            "total_scans": self.scan_count,
            "total_signals": self.total_signals,
            "pairs_watched": len(self._last_pairs),
            "exchange": config.CRYPTO_EXCHANGE,
        }


# Import pandas for type hint fix
import pandas as pd

crypto_sniper = CryptoSniper()
