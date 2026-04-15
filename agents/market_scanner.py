"""
Market Scanner Agent — Continuously scans for trading opportunities.

Responsibilities:
  - Monitor XAUUSD price action across timeframes
  - Detect patterns: breakouts, reversals, trend continuations
  - Generate raw signals for the Analyst to deep-dive
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

import pandas as pd

from agents.base import BaseAgent, AgentMessage
from core.task import Task, TaskType
from tools.base import ToolResult

logger = logging.getLogger("mempalace2.agents.scanner")


class MarketScannerAgent(BaseAgent):
    """
    Watches the market continuously and identifies potential setups.

    Scans for:
      - EMA crossovers (fast/slow)
      - Support/resistance tests
      - Volume breakouts
      - RSI divergences
      - Supertrend direction changes
      - Bollinger Band squeezes
    """

    name = "market_scanner"
    role = "Scan markets for trading opportunities"

    def __init__(self, state):
        super().__init__(state)
        self._scan_task: Optional[asyncio.Task] = None
        self._last_signals: Dict[str, dict] = {}

    async def handle_message(self, message: AgentMessage):
        if message.action == "scan":
            await self.scan_symbol(message.data.get("symbol", "XAUUSD"))
        elif message.action == "start_scan_loop":
            await self.start_scan_loop()
        elif message.action == "stop_scan_loop":
            await self.stop_scan_loop()

    async def start_scan_loop(self):
        """Start continuous scanning loop."""
        if self._scan_task and not self._scan_task.done():
            return
        self._scan_task = asyncio.create_task(self._scan_loop())
        logger.info("Scanner loop started")

    async def stop_scan_loop(self):
        """Stop scanning loop."""
        if self._scan_task:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass
        logger.info("Scanner loop stopped")

    async def _scan_loop(self):
        """Continuous scan loop."""
        interval = self.state.config.agents.scan_interval_seconds
        while self.is_active:
            try:
                for symbol in self.state.config.symbols:
                    await self.scan_symbol(symbol)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scan loop error: {e}")
                await asyncio.sleep(5)

    async def scan_symbol(self, symbol: str) -> Optional[dict]:
        """
        Scan a single symbol for opportunities.

        Pipeline:
          1. Fetch OHLCV data (all timeframes)
          2. Run technical analysis
          3. Detect setups
          4. Forward to Analyst if promising
        """
        # Create tracking task
        task = self.state.task_manager.create_task(
            task_type=TaskType.MARKET_SCAN,
            name=f"scan_{symbol}",
            description=f"Scanning {symbol} for setups",
            symbol=symbol,
        )

        async def _do_scan():
            market_data_tool = self.get_tool("market_data")
            ta_tool = self.get_tool("technical_analysis")
            if not market_data_tool or not ta_tool:
                return None

            # 1. Fetch data on primary timeframe
            result = await market_data_tool.execute(
                symbol=symbol,
                timeframe=self.state.config.analysis.primary_timeframe,
                limit=self.state.config.analysis.lookback_candles,
            )
            if not result.success:
                logger.warning(f"Failed to fetch data for {symbol}: {result.error}")
                return None

            df = result.data

            # 2. Run technical analysis
            ta_result = await ta_tool.execute(data=df)
            if not ta_result.success:
                return None

            indicators = ta_result.data["indicators"]

            # 3. Detect setups
            setups = self._detect_setups(symbol, indicators, df)

            if setups:
                logger.info(f"🔍 {symbol}: {len(setups)} setup(s) detected")
                # Forward best setup to Analyst
                best = max(setups, key=lambda s: s["strength"])
                await self.send(
                    recipient="analyst",
                    action="analyze",
                    data={
                        "symbol": symbol,
                        "setup": best,
                        "indicators": indicators,
                        "ohlcv": df,
                    },
                    priority=int(best["strength"]),
                )

            return setups

        return await self.state.task_manager.run_task(task, _do_scan())

    def _detect_setups(
        self, symbol: str, indicators, df: pd.DataFrame
    ) -> List[dict]:
        """
        Detect trading setups from indicator readings.

        Returns list of setup dicts with type, direction, strength, reasoning.
        """
        setups = []
        close = float(df["close"].iloc[-1])
        prev_close = float(df["close"].iloc[-2])

        # ── Setup 1: EMA Crossover ──────────────────
        ema_fast_prev = float(df["ema_fast"].iloc[-2])
        ema_slow_prev = float(df["ema_slow"].iloc[-2])

        if (indicators.ema_fast > indicators.ema_slow and
                ema_fast_prev <= ema_slow_prev):
            setups.append({
                "type": "ema_crossover",
                "direction": "long",
                "strength": 70 + min(20, indicators.adx),
                "reasoning": f"EMA {self.state.config.analysis.ema_fast}/{self.state.config.analysis.ema_slow} bullish crossover",
            })
        elif (indicators.ema_fast < indicators.ema_slow and
              ema_fast_prev >= ema_slow_prev):
            setups.append({
                "type": "ema_crossover",
                "direction": "short",
                "strength": 70 + min(20, indicators.adx),
                "reasoning": f"EMA {self.state.config.analysis.ema_fast}/{self.state.config.analysis.ema_slow} bearish crossover",
            })

        # ── Setup 2: RSI Oversold/Overbought ────────
        if indicators.rsi < self.state.config.analysis.rsi_oversold:
            setups.append({
                "type": "rsi_oversold",
                "direction": "long",
                "strength": 60 + (self.state.config.analysis.rsi_oversold - indicators.rsi),
                "reasoning": f"RSI at {indicators.rsi:.1f} — oversold zone",
            })
        elif indicators.rsi > self.state.config.analysis.rsi_overbought:
            setups.append({
                "type": "rsi_overbought",
                "direction": "short",
                "strength": 60 + (indicators.rsi - self.state.config.analysis.rsi_overbought),
                "reasoning": f"RSI at {indicators.rsi:.1f} — overbought zone",
            })

        # ── Setup 3: MACD Momentum Shift ────────────
        macd_hist_prev = float(df["macd_histogram"].iloc[-2])
        if indicators.macd_histogram > 0 and macd_hist_prev <= 0:
            setups.append({
                "type": "macd_bullish_cross",
                "direction": "long",
                "strength": 65,
                "reasoning": "MACD histogram turning bullish",
            })
        elif indicators.macd_histogram < 0 and macd_hist_prev >= 0:
            setups.append({
                "type": "macd_bearish_cross",
                "direction": "short",
                "strength": 65,
                "reasoning": "MACD histogram turning bearish",
            })

        # ── Setup 4: Supertrend Direction Change ────
        st_prev = int(df["supertrend_direction"].iloc[-2])
        if indicators.supertrend_direction == 1 and st_prev == -1:
            setups.append({
                "type": "supertrend_flip_bullish",
                "direction": "long",
                "strength": 75,
                "reasoning": "Supertrend flipped bullish",
            })
        elif indicators.supertrend_direction == -1 and st_prev == 1:
            setups.append({
                "type": "supertrend_flip_bearish",
                "direction": "short",
                "strength": 75,
                "reasoning": "Supertrend flipped bearish",
            })

        # ── Setup 5: Bollinger Band Squeeze Breakout ─
        if indicators.bb_width < 1.5:
            bb_pct = (close - indicators.bb_lower) / (indicators.bb_upper - indicators.bb_lower) if indicators.bb_upper != indicators.bb_lower else 0.5
            if bb_pct > 0.8:
                setups.append({
                    "type": "bb_squeeze_breakout",
                    "direction": "long",
                    "strength": 72,
                    "reasoning": f"BB squeeze ({indicators.bb_width:.1f}%) — breaking upper",
                })
            elif bb_pct < 0.2:
                setups.append({
                    "type": "bb_squeeze_breakout",
                    "direction": "short",
                    "strength": 72,
                    "reasoning": f"BB squeeze ({indicators.bb_width:.1f}%) — breaking lower",
                })

        # ── Setup 6: Support/Resistance Test ────────
        atr = indicators.atr
        if atr > 0:
            # Price near support (bounce potential)
            if indicators.support_1 > 0:
                dist_to_support = abs(close - indicators.support_1) / atr
                if dist_to_support < 0.5:
                    setups.append({
                        "type": "support_test",
                        "direction": "long",
                        "strength": 68 + max(0, 10 - dist_to_support * 20),
                        "reasoning": f"Price {dist_to_support:.1f} ATR from support {indicators.support_1:.2f}",
                    })

            # Price near resistance (rejection potential)
            if indicators.resistance_1 > 0:
                dist_to_resistance = abs(close - indicators.resistance_1) / atr
                if dist_to_resistance < 0.5:
                    setups.append({
                        "type": "resistance_test",
                        "direction": "short",
                        "strength": 68 + max(0, 10 - dist_to_resistance * 20),
                        "reasoning": f"Price {dist_to_resistance:.1f} ATR from resistance {indicators.resistance_1:.2f}",
                    })

        # ── Setup 7: Trend Alignment (multi-factor) ─
        trend_aligned_long = (
            indicators.ema_fast > indicators.ema_slow > indicators.ema_trend
            and close > indicators.ema_trend
            and indicators.supertrend_direction == 1
            and indicators.macd_histogram > 0
        )
        trend_aligned_short = (
            indicators.ema_fast < indicators.ema_slow < indicators.ema_trend
            and close < indicators.ema_trend
            and indicators.supertrend_direction == -1
            and indicators.macd_histogram < 0
        )

        if trend_aligned_long:
            setups.append({
                "type": "trend_alignment",
                "direction": "long",
                "strength": 80,
                "reasoning": "Full trend alignment: EMAs + Supertrend + MACD all bullish",
            })
        elif trend_aligned_short:
            setups.append({
                "type": "trend_alignment",
                "direction": "short",
                "strength": 80,
                "reasoning": "Full trend alignment: EMAs + Supertrend + MACD all bearish",
            })

        return setups
