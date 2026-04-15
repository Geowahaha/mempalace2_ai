"""
Analyst Agent — Deep analysis of trading setups.

Responsibilities:
  - Take raw setups from Scanner
  - Multi-timeframe confirmation
  - Calculate optimal Entry, TP1/TP2/TP3, SL
  - Score confidence and expected value
  - Forward validated signals to Risk Manager
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from agents.base import BaseAgent, AgentMessage
from core.state import TradeSignal
from core.task import TaskType
from tools.base import ToolResult

logger = logging.getLogger("mempalace2.agents.analyst")


class AnalystAgent(BaseAgent):
    """
    Deep-dive analysis agent.

    Takes Scanner setups and produces high-quality TradeSignals with:
      - Optimal entry price (Sharpe-adjusted)
      - Dynamic TP/SL (ATR + market structure based)
      - Multi-timeframe confluence scoring
      - Expected value calculation
    """

    name = "analyst"
    role = "Deep analysis and signal generation"

    async def handle_message(self, message: AgentMessage):
        if message.action == "analyze":
            await self.analyze_setup(
                symbol=message.data.get("symbol", ""),
                setup=message.data.get("setup", {}),
                indicators=message.data.get("indicators"),
                ohlcv=message.data.get("ohlcv"),
            )

    async def analyze_setup(
        self,
        symbol: str,
        setup: dict,
        indicators=None,
        ohlcv: pd.DataFrame = None,
    ):
        """
        Full analysis pipeline for a detected setup.

        Steps:
          1. Multi-timeframe confirmation
          2. Optimal entry calculation
          3. ATR-based TP/SL optimization
          4. Confidence scoring
          5. Forward to Risk Manager
        """
        task = self.state.task_manager.create_task(
            task_type=TaskType.ANALYSIS,
            name=f"analyze_{symbol}_{setup.get('type', 'unknown')}",
            description=f"Deep analysis: {symbol} {setup.get('type')} {setup.get('direction')}",
            symbol=symbol,
        )

        async def _do_analysis():
            logger.info(
                f"📊 Analyzing: {symbol} {setup['direction']} "
                f"[{setup['type']}] strength={setup.get('strength', 0)}"
            )

            # 1. Multi-timeframe confirmation
            mtf_score = await self._multi_timeframe_check(symbol, setup["direction"])

            # 2. Get support/resistance from indicators
            supports = []
            resistances = []
            atr = 0.0
            if indicators:
                supports = [indicators.support_1, indicators.support_2]
                resistances = [indicators.resistance_1, indicators.resistance_2]
                atr = indicators.atr

            # 3. Calculate optimal Entry/TP/SL
            entry_price = float(ohlcv["close"].iloc[-1]) if ohlcv is not None else 0
            if entry_price <= 0:
                return None

            tpsl = self._calculate_optimal_levels(
                entry_price=entry_price,
                direction=setup["direction"],
                atr=atr,
                supports=[s for s in supports if s > 0],
                resistances=[r for r in resistances if r > 0],
            )

            # 4. Calculate confidence
            base_confidence = setup.get("strength", 50)
            mtf_bonus = mtf_score * 0.3
            atr_bonus = min(10, atr / entry_price * 1000) if atr > 0 else 0  # ATR quality
            confidence = min(100, base_confidence * 0.5 + mtf_bonus + atr_bonus)

            # 5. Build signal
            signal = TradeSignal(
                symbol=symbol,
                timeframe=self.state.config.analysis.primary_timeframe,
                direction=setup["direction"],
                entry_price=entry_price,
                stop_loss=tpsl["stop_loss"],
                take_profit_1=tpsl["take_profit_1"],
                take_profit_2=tpsl["take_profit_2"],
                take_profit_3=tpsl["take_profit_3"],
                confidence=confidence,
                risk_reward_ratio=tpsl["risk_reward_1"],
                strategy=setup["type"],
                reasoning=(
                    f"{setup['reasoning']}\n"
                    f"MTF confirmation: {mtf_score:.0f}/100\n"
                    f"Entry: {entry_price:.2f} | "
                    f"SL: {tpsl['stop_loss']:.2f} ({tpsl['risk_reward_1']:.1f}R)\n"
                    f"TP1: {tpsl['take_profit_1']:.2f} | "
                    f"TP2: {tpsl['take_profit_2']:.2f} | "
                    f"TP3: {tpsl['take_profit_3']:.2f}"
                ),
                atr=atr,
                trend_strength=indicators.trend_score if indicators else 0,
                volume_score=indicators.volume_ratio if indicators else 1,
                support_level=indicators.support_1 if indicators else 0,
                resistance_level=indicators.resistance_1 if indicators else 0,
            )

            # 6. Store and forward to Risk Manager
            self.state.signals.append(signal)
            self.state.total_analyses += 1

            logger.info(
                f"📈 Signal generated: {symbol} {signal.direction} "
                f"Conf={confidence:.0f}% R:R={signal.risk_reward_ratio:.1f} "
                f"Entry={signal.entry_price:.2f} SL={signal.stop_loss:.2f} "
                f"TP1={signal.take_profit_1:.2f}"
            )

            # Forward to risk manager
            await self.send(
                recipient="risk_manager",
                action="validate",
                data={"signal": signal},
                priority=int(confidence),
            )

            return signal

        return await self.state.task_manager.run_task(task, _do_analysis())

    async def _multi_timeframe_check(self, symbol: str, direction: str) -> float:
        """
        Confirm signal across multiple timeframes.
        Returns score 0-100 representing confluence.
        """
        market_data_tool = self.get_tool("market_data")
        ta_tool = self.get_tool("technical_analysis")
        if not market_data_tool or not ta_tool:
            return 50.0

        score = 0.0
        timeframes = self.state.config.analysis.timeframes
        weights = {"15m": 0.15, "1h": 0.30, "4h": 0.30, "1d": 0.25}

        for tf in timeframes:
            try:
                result = await market_data_tool.execute(symbol=symbol, timeframe=tf, limit=100)
                if not result.success:
                    continue

                ta_result = await ta_tool.execute(data=result.data)
                if not ta_result.success:
                    continue

                ind = ta_result.data["indicators"]
                weight = weights.get(tf, 0.2)

                if direction == "long":
                    if ind.ema_fast > ind.ema_slow:
                        score += 30 * weight
                    if ind.supertrend_direction == 1:
                        score += 30 * weight
                    if ind.rsi > 40:
                        score += 20 * weight
                    if ind.macd_histogram > 0:
                        score += 20 * weight
                else:
                    if ind.ema_fast < ind.ema_slow:
                        score += 30 * weight
                    if ind.supertrend_direction == -1:
                        score += 30 * weight
                    if ind.rsi < 60:
                        score += 20 * weight
                    if ind.macd_histogram < 0:
                        score += 20 * weight

            except Exception as e:
                logger.warning(f"MTF check failed for {tf}: {e}")

        return min(100, score)

    def _calculate_optimal_levels(
        self,
        entry_price: float,
        direction: str,
        atr: float,
        supports: List[float],
        resistances: List[float],
    ) -> Dict:
        """
        Calculate optimal TP/SL levels.

        Strategy:
          - SL: ATR-based + structure (support/resistance)
          - TP1: Conservative (1.5-2R)
          - TP2: Moderate (2-3R)
          - TP3: Aggressive (3-5R)
        """
        risk_tool = self.get_tool("risk_engine")
        if risk_tool:
            # Use risk engine's calculate_optimal_tpsl
            return risk_tool.calculate_optimal_tpsl(
                entry_price=entry_price,
                direction=direction,
                atr=atr,
                supports=supports,
                resistances=resistances,
            )

        # Fallback: ATR-only calculation
        if atr <= 0:
            atr = entry_price * 0.005  # 0.5% fallback

        sl_dist = atr * 1.5
        if direction == "long":
            sl = entry_price - sl_dist
            tp1 = entry_price + sl_dist * 2.0
            tp2 = entry_price + sl_dist * 3.0
            tp3 = entry_price + sl_dist * 5.0
        else:
            sl = entry_price + sl_dist
            tp1 = entry_price - sl_dist * 2.0
            tp2 = entry_price - sl_dist * 3.0
            tp3 = entry_price - sl_dist * 5.0

        return {
            "stop_loss": round(sl, 2),
            "take_profit_1": round(tp1, 2),
            "take_profit_2": round(tp2, 2),
            "take_profit_3": round(tp3, 2),
            "risk_reward_1": 2.0,
            "risk_reward_2": 3.0,
            "risk_reward_3": 5.0,
        }
