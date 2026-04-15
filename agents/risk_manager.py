"""
Risk Manager Agent — Final gatekeeper before trade execution.

Responsibilities:
  - Validate every signal against risk parameters
  - Calculate optimal position size (Kelly Criterion)
  - Check portfolio heat limits
  - Enforce daily loss circuit breakers
  - Approve or reject trades
"""

from __future__ import annotations

import logging
from typing import Optional

from agents.base import BaseAgent, AgentMessage
from core.state import TradeSignal
from core.task import TaskType

logger = logging.getLogger("mempalace2.agents.risk_manager")


class RiskManagerAgent(BaseAgent):
    """
    The final authority on trade approval.

    Every trade must pass through this agent before execution.
    No exceptions. No overrides.
    """

    name = "risk_manager"
    role = "Risk validation and position sizing"

    def __init__(self, state):
        super().__init__(state)
        self._approved_count = 0
        self._rejected_count = 0
        self._rejection_reasons: dict[str, int] = {}

    async def handle_message(self, message: AgentMessage):
        if message.action == "validate":
            signal = message.data.get("signal")
            if signal:
                await self.validate_trade(signal)

    async def validate_trade(self, signal: TradeSignal) -> bool:
        """
        Full risk validation pipeline.

        Steps:
          1. Pre-checks (basic sanity)
          2. Risk/Reward ratio enforcement
          3. Kelly Criterion position sizing
          4. Portfolio heat calculation
          5. Correlation check
          6. Daily loss circuit breaker
          7. Final approval/rejection
        """
        task = self.state.task_manager.create_task(
            task_type=TaskType.RISK_CHECK,
            name=f"risk_check_{signal.symbol}_{signal.direction}",
            description=f"Risk validation: {signal.symbol} {signal.direction}",
            symbol=signal.symbol,
        )

        async def _do_validation():
            logger.info(
                f"🛡️ Risk check: {signal.symbol} {signal.direction} "
                f"R:R={signal.risk_reward_ratio:.2f} Conf={signal.confidence:.0f}%"
            )

            # Use risk engine tool
            risk_tool = self.get_tool("risk_engine")
            if not risk_tool:
                logger.error("Risk engine tool not available!")
                signal.status = "rejected"
                return False

            result = await risk_tool.execute(
                signal=signal,
                portfolio=self.state.portfolio,
            )

            if not result.success:
                logger.error(f"Risk engine error: {result.error}")
                signal.status = "rejected"
                return False

            assessment = result.data["assessment"]

            if assessment.approved:
                # Update signal with risk-calculated values
                signal.position_size_pct = assessment.position_size_pct
                signal.expected_value = assessment.expected_value
                signal.status = "approved"

                self._approved_count += 1
                self.state.total_signals += 1

                logger.info(
                    f"✅ APPROVED: {signal.symbol} {signal.direction} "
                    f"Size={assessment.position_size_pct:.1f}% "
                    f"R:R={assessment.risk_reward_ratio:.2f} "
                    f"EV={assessment.expected_value:.4f}% "
                    f"Heat={assessment.portfolio_heat_after:.1f}%"
                )

                # Forward to executor
                await self.send(
                    recipient="executor",
                    action="execute",
                    data={
                        "signal": signal,
                        "assessment": assessment,
                    },
                    priority=int(signal.confidence),
                )

                return True
            else:
                signal.status = "rejected"
                self._rejected_count += 1
                reason = assessment.rejection_reason
                self._rejection_reasons[reason] = self._rejection_reasons.get(reason, 0) + 1

                logger.info(
                    f"❌ REJECTED: {signal.symbol} {signal.direction} — {reason}"
                )
                return False

        return await self.state.task_manager.run_task(task, _do_validation())

    def get_stats(self) -> dict:
        """Get risk manager statistics."""
        total = self._approved_count + self._rejected_count
        return {
            "total_checked": total,
            "approved": self._approved_count,
            "rejected": self._rejected_count,
            "approval_rate": f"{self._approved_count / max(1, total) * 100:.1f}%",
            "top_rejection_reasons": dict(
                sorted(self._rejection_reasons.items(), key=lambda x: x[1], reverse=True)[:5]
            ),
        }
