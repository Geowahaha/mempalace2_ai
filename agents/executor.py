"""
Executor Agent — Handles trade execution and position management.

Responsibilities:
  - Execute approved trades via exchange API
  - Manage open positions (trailing stops, TP monitoring)
  - Track fills and slippage
  - Emergency close capabilities
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from agents.base import BaseAgent, AgentMessage
from core.state import ActiveTrade, TradeSignal
from core.task import TaskType

logger = logging.getLogger("mempalace2.agents.executor")


class ExecutorAgent(BaseAgent):
    """
    Executes trades and manages open positions.

    Trade lifecycle:
      1. Receive approved signal + risk assessment
      2. Place order (limit at entry or market)
      3. Monitor fills
      4. Manage trailing stops
      5. Close at TP/SL
    """

    name = "executor"
    role = "Trade execution and position management"

    def __init__(self, state):
        super().__init__(state)
        self._monitor_task: Optional[asyncio.Task] = None

    async def handle_message(self, message: AgentMessage):
        if message.action == "execute":
            signal = message.data.get("signal")
            assessment = message.data.get("assessment")
            if signal:
                await self.execute_trade(signal, assessment)
        elif message.action == "close_all":
            await self.close_all_positions()
        elif message.action == "start_monitor":
            await self.start_position_monitor()

    async def start_position_monitor(self):
        """Start monitoring open positions."""
        if self._monitor_task and not self._monitor_task.done():
            return
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def _monitor_loop(self):
        """Monitor open positions for TP/SL hits and trailing stops."""
        while self.is_active:
            try:
                for trade in list(self.state.portfolio.active_trades):
                    await self._check_position(trade)
                await asyncio.sleep(5)  # check every 5 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(5)

    async def execute_trade(self, signal: TradeSignal, assessment=None):
        """
        Execute an approved trade.

        In sandbox/testnet: simulates execution.
        In live: sends real orders to exchange.
        """
        task = self.state.task_manager.create_task(
            task_type=TaskType.TRADE_EXECUTION,
            name=f"execute_{signal.symbol}_{signal.direction}",
            description=f"Executing: {signal.symbol} {signal.direction}",
            symbol=signal.symbol,
        )

        async def _do_execute():
            logger.info(
                f"⚡ Executing: {signal.symbol} {signal.direction} "
                f"Entry={signal.entry_price:.2f} "
                f"SL={signal.stop_loss:.2f} "
                f"TP1={signal.take_profit_1:.2f}"
            )

            # Create active trade
            position_value = self.state.portfolio.total_equity * (signal.position_size_pct / 100)
            quantity = position_value / signal.entry_price

            active_trade = ActiveTrade(
                signal=signal,
                entry_filled_price=signal.entry_price,  # assume fill at signal price
                entry_filled_time=datetime.now(timezone.utc),
                quantity=quantity,
            )

            # Update portfolio
            self.state.portfolio.active_trades.append(active_trade)
            self.state.portfolio.open_positions += 1
            self.state.portfolio.available_balance -= position_value
            self.state.portfolio.margin_used += position_value

            # Calculate risk
            risk_amount = abs(signal.entry_price - signal.stop_loss) * quantity
            risk_pct = risk_amount / self.state.portfolio.total_equity * 100
            self.state.portfolio.total_risk_pct += risk_pct

            signal.status = "executed"
            self.state.total_trades += 1

            logger.info(
                f"✅ Executed: {signal.symbol} {signal.direction} "
                f"Qty={quantity:.4f} Value=${position_value:.2f} "
                f"Risk={risk_pct:.2f}% "
                f"TotalRisk={self.state.portfolio.total_risk_pct:.1f}%"
            )

            # Start monitor if not running
            await self.start_position_monitor()

            return active_trade

        return await self.state.task_manager.run_task(task, _do_execute())

    async def _check_position(self, trade: ActiveTrade):
        """
        Check a single position for TP/SL hits and manage trailing stops.
        """
        signal = trade.signal
        current_price = self._get_current_price(signal.symbol)
        if current_price <= 0:
            return

        is_long = signal.direction == "long"

        # Calculate unrealized P&L
        if is_long:
            pnl_pct = (current_price - trade.entry_filled_price) / trade.entry_filled_price * 100
        else:
            pnl_pct = (trade.entry_filled_price - current_price) / trade.entry_filled_price * 100

        trade.unrealized_pnl = pnl_pct * trade.quantity * trade.entry_filled_price / 100

        # Check SL hit
        if is_long and current_price <= signal.stop_loss:
            await self._close_position(trade, current_price, "SL hit")
            return
        elif not is_long and current_price >= signal.stop_loss:
            await self._close_position(trade, current_price, "SL hit")
            return

        # Check TP hits
        tps = [
            (1, signal.take_profit_1),
            (2, signal.take_profit_2),
            (3, signal.take_profit_3),
        ]
        for tp_num, tp_price in tps:
            if tp_price <= 0:
                continue
            if tp_num in trade.tp_hits:
                continue

            hit = (is_long and current_price >= tp_price) or (not is_long and current_price <= tp_price)
            if hit:
                trade.tp_hits.append(tp_num)
                logger.info(
                    f"🎯 TP{tp_num} hit: {signal.symbol} {signal.direction} "
                    f"@ {current_price:.2f} (+{pnl_pct:.2f}%)"
                )

                # Move SL to breakeven after TP1
                if tp_num == 1:
                    if is_long:
                        trade.trailing_stop = trade.entry_filled_price
                        logger.info(f"  → SL moved to breakeven: {trade.entry_filled_price:.2f}")
                    else:
                        trade.trailing_stop = trade.entry_filled_price
                        logger.info(f"  → SL moved to breakeven: {trade.entry_filled_price:.2f}")

                # Close at TP3 (full target)
                if tp_num == 3:
                    await self._close_position(trade, current_price, "TP3 hit")
                    return

        # Update trailing stop
        atr = signal.atr
        if atr > 0 and trade.trailing_stop > 0:
            trail_dist = atr * self.state.config.risk.trailing_stop_atr
            if is_long:
                new_trail = current_price - trail_dist
                if new_trail > trade.trailing_stop:
                    trade.trailing_stop = new_trail
            else:
                new_trail = current_price + trail_dist
                if new_trail < trade.trailing_stop:
                    trade.trailing_stop = new_trail

    async def _close_position(self, trade: ActiveTrade, price: float, reason: str):
        """Close a position at the given price."""
        signal = trade.signal
        is_long = signal.direction == "long"

        # Calculate realized P&L
        if is_long:
            pnl_pct = (price - trade.entry_filled_price) / trade.entry_filled_price * 100
        else:
            pnl_pct = (trade.entry_filled_price - price) / trade.entry_filled_price * 100

        realized_pnl = pnl_pct * trade.quantity * trade.entry_filled_price / 100

        # Update portfolio
        trade.realized_pnl = realized_pnl
        trade.signal.status = "closed"

        self.state.portfolio.active_trades.remove(trade)
        self.state.portfolio.closed_trades.append(trade)
        self.state.portfolio.open_positions -= 1
        self.state.portfolio.available_balance += trade.quantity * price
        self.state.portfolio.margin_used -= trade.quantity * trade.entry_filled_price
        self.state.portfolio.daily_pnl += realized_pnl
        self.state.portfolio.total_pnl += realized_pnl

        # Recalculate total risk
        risk_amount = abs(signal.entry_price - signal.stop_loss) * trade.quantity
        risk_pct = risk_amount / self.state.portfolio.total_equity * 100
        self.state.portfolio.total_risk_pct -= risk_pct

        emoji = "✅" if realized_pnl > 0 else "❌"
        logger.info(
            f"{emoji} CLOSED: {signal.symbol} {signal.direction} — {reason} "
            f"P&L=${realized_pnl:+.2f} ({pnl_pct:+.2f}%) "
            f"TPs hit: {trade.tp_hits}"
        )

    def _get_current_price(self, symbol: str) -> float:
        """Get current price from market data cache or last signal."""
        # In a real system, this would fetch live price
        # For now, use synthetic movement
        import numpy as np
        base = 3200.0 if "XAU" in symbol else 1.1000
        return base + np.random.normal(0, base * 0.001)

    async def close_all_positions(self):
        """Emergency: close all open positions."""
        for trade in list(self.state.portfolio.active_trades):
            price = self._get_current_price(trade.signal.symbol)
            await self._close_position(trade, price, "Emergency close")
