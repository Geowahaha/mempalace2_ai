"""
Scheduled Reports & Periodic Tasks — adapted from hermes-agent's cron system.

Provides scheduled execution of periodic trading tasks:
  - Daily P&L report
  - Weekly performance review
  - Hourly market snapshots
  - Trade alert delivery (Telegram/Discord hooks)

Key hermes-agent concepts adapted:
  - Cron expressions for scheduling
  - Job management (add, remove, enable, disable)
  - Delivery modes (announce, webhook)
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("mempalace2.scheduler")


class ReportType(str, Enum):
    DAILY_PNL = "daily_pnl"
    WEEKLY_REVIEW = "weekly_review"
    HOURLY_SNAPSHOT = "hourly_snapshot"
    TRADE_ALERT = "trade_alert"
    SKILL_PERFORMANCE = "skill_performance"
    MEMORY_STATS = "memory_stats"


class DeliveryMode(str, Enum):
    LOG = "log"           # Just log it
    PRINT = "print"       # Print to console
    WEBHOOK = "webhook"   # POST to URL
    CALLBACK = "callback" # Call a Python function


@dataclass
class ScheduledReport:
    """A scheduled report job."""
    id: str = ""
    name: str = ""
    report_type: ReportType = ReportType.DAILY_PNL
    schedule_seconds: int = 3600  # Run every N seconds
    enabled: bool = True
    delivery: DeliveryMode = DeliveryMode.LOG
    webhook_url: Optional[str] = None
    callback: Optional[Callable] = None
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    run_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_due(self) -> bool:
        """Check if this report is due to run."""
        if not self.enabled:
            return False
        now = time.time()
        if self.next_run is None:
            return True
        return now >= self.next_run

    def mark_run(self):
        """Mark this report as having run."""
        self.last_run = time.time()
        self.next_run = self.last_run + self.schedule_seconds
        self.run_count += 1


class Scheduler:
    """
    Manages scheduled periodic tasks for the trading system.

    Runs as an async task that checks for due reports and executes them.
    """

    def __init__(self, state_store=None, memory=None, skills_manager=None):
        self.state_store = state_store
        self.memory = memory
        self.skills_manager = skills_manager
        self._reports: Dict[str, ScheduledReport] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._report_generators: Dict[ReportType, Callable] = {
            ReportType.DAILY_PNL: self._gen_daily_pnl,
            ReportType.WEEKLY_REVIEW: self._gen_weekly_review,
            ReportType.HOURLY_SNAPSHOT: self._gen_hourly_snapshot,
            ReportType.TRADE_ALERT: self._gen_trade_alert,
            ReportType.SKILL_PERFORMANCE: self._gen_skill_performance,
            ReportType.MEMORY_STATS: self._gen_memory_stats,
        }

    # ── Job Management ──────────────────────────────────

    def add_report(self, report: ScheduledReport) -> str:
        """Add a scheduled report. Returns report ID."""
        if not report.id:
            import uuid
            report.id = uuid.uuid4().hex[:8]
        if report.next_run is None:
            report.next_run = time.time() + report.schedule_seconds
        self._reports[report.id] = report
        logger.info(f"Report scheduled: {report.name} ({report.report_type.value}) "
                     f"every {report.schedule_seconds}s")
        return report.id

    def remove_report(self, report_id: str):
        """Remove a scheduled report."""
        self._reports.pop(report_id, None)

    def enable_report(self, report_id: str, enabled: bool = True):
        """Enable or disable a report."""
        if report_id in self._reports:
            self._reports[report_id].enabled = enabled

    def list_reports(self) -> List[Dict]:
        """List all scheduled reports."""
        return [
            {
                "id": r.id,
                "name": r.name,
                "type": r.report_type.value,
                "enabled": r.enabled,
                "schedule_s": r.schedule_seconds,
                "last_run": r.last_run,
                "next_run": r.next_run,
                "run_count": r.run_count,
            }
            for r in self._reports.values()
        ]

    # ── Default Reports ─────────────────────────────────

    def setup_defaults(self):
        """Register default periodic reports."""
        defaults = [
            ScheduledReport(
                name="Hourly Market Snapshot",
                report_type=ReportType.HOURLY_SNAPSHOT,
                schedule_seconds=3600,
            ),
            ScheduledReport(
                name="Daily P&L Report",
                report_type=ReportType.DAILY_PNL,
                schedule_seconds=86400,
            ),
            ScheduledReport(
                name="Weekly Performance Review",
                report_type=ReportType.WEEKLY_REVIEW,
                schedule_seconds=604800,
            ),
            ScheduledReport(
                name="Skill Performance Report",
                report_type=ReportType.SKILL_PERFORMANCE,
                schedule_seconds=43200,  # Every 12 hours
            ),
            ScheduledReport(
                name="Memory Stats Report",
                report_type=ReportType.MEMORY_STATS,
                schedule_seconds=21600,  # Every 6 hours
            ),
        ]
        for r in defaults:
            self.add_report(r)

    # ── Execution Loop ──────────────────────────────────

    async def start(self):
        """Start the scheduler loop."""
        self._running = True
        logger.info("Scheduler started")
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """Stop the scheduler loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Scheduler stopped")

    async def _run_loop(self):
        """Main scheduler loop — checks every 30 seconds."""
        while self._running:
            await self._check_and_run()
            await asyncio.sleep(30)

    async def _check_and_run(self):
        """Check for due reports and execute them."""
        for report in list(self._reports.values()):
            if report.is_due():
                try:
                    content = await self._generate_report(report)
                    await self._deliver(report, content)
                    report.mark_run()
                except Exception as e:
                    logger.error(f"Report '{report.name}' failed: {e}")

    # ── Report Generation ───────────────────────────────

    async def _generate_report(self, report: ScheduledReport) -> str:
        """Generate report content based on type."""
        generator = self._report_generators.get(report.report_type)
        if not generator:
            return f"[Unknown report type: {report.report_type}]"
        return generator()

    def _gen_daily_pnl(self) -> str:
        """Generate daily P&L report."""
        if not self.state_store:
            return "[Daily P&L] No state store configured"

        stats = self.state_store.get_trade_stats()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        lines = [
            f"📊 Daily P&L Report — {now}",
            f"  Total Trades: {stats.get('total_trades', 0)}",
            f"  Win Rate: {stats.get('win_rate', 0):.1%}",
            f"  Total P&L: ${stats.get('total_pnl', 0):,.2f}",
            f"  Avg P&L%: {stats.get('avg_pnl_pct', 0):.2f}%",
            f"  Avg R:R: {stats.get('avg_risk_reward', 0):.2f}",
            f"  Best Trade: ${stats.get('best_trade', 0):,.2f}",
            f"  Worst Trade: ${stats.get('worst_trade', 0):,.2f}",
            f"  Profit Factor: {stats.get('profit_factor', 0):.2f}",
        ]

        # Strategy breakdown
        strategies = self.state_store.get_strategy_performance()
        if strategies:
            lines.append("\n  By Strategy:")
            for s in strategies[:5]:
                lines.append(
                    f"    {s['strategy']}: {s['trades']} trades, "
                    f"{s['wins']}/{s['trades']} wins, "
                    f"${s['total_pnl']:,.2f}"
                )

        return "\n".join(lines)

    def _gen_weekly_review(self) -> str:
        """Generate weekly performance review."""
        if not self.state_store:
            return "[Weekly Review] No state store configured"

        stats = self.state_store.get_trade_stats()
        hourly = self.state_store.get_hourly_performance()
        patterns = self.state_store.get_best_patterns(min_samples=3)

        lines = [
            "📈 Weekly Performance Review",
            f"  Trades: {stats.get('total_trades', 0)} | "
            f"WR: {stats.get('win_rate', 0):.1%} | "
            f"P&L: ${stats.get('total_pnl', 0):,.2f}",
        ]

        if hourly:
            lines.append("\n  Best Trading Hours:")
            sorted_hours = sorted(hourly, key=lambda h: h.get("win_rate", 0), reverse=True)
            for h in sorted_hours[:3]:
                lines.append(
                    f"    {h['hour']:02d}:00 — {h['win_rate']:.0%} WR "
                    f"({h['trades']} trades)"
                )

        if patterns:
            lines.append("\n  Top Patterns:")
            for p in patterns[:3]:
                lines.append(
                    f"    {p['pattern_name']}: {p['win_rate']:.0%} WR, "
                    f"{p['sample_count']} samples"
                )

        return "\n".join(lines)

    def _gen_hourly_snapshot(self) -> str:
        """Generate hourly market snapshot log."""
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        return f"[{now}] Market snapshot taken"

    def _gen_trade_alert(self) -> str:
        """Generate trade alert (placeholder — actual alerts triggered by events)."""
        return "[Trade Alert] No pending alerts"

    def _gen_skill_performance(self) -> str:
        """Generate skill performance report."""
        if not self.skills_manager:
            return "[Skill Report] No skill manager configured"

        stats = self.skills_manager.get_stats()
        lines = [
            "🎯 Skill Performance Report",
            f"  Total Skills: {stats.get('total', 0)}",
            f"  Avg Win Rate: {stats.get('avg_win_rate', 0):.1%}",
            f"  High Confidence (>60% WR, ≥5 trades): {stats.get('high_confidence', 0)}",
        ]

        skills = stats.get("skills", [])
        if skills:
            lines.append("\n  Top Skills:")
            for s in sorted(skills, key=lambda x: x.get("win_rate", 0), reverse=True)[:5]:
                lines.append(
                    f"    {s['name']}: {s['win_rate']:.0%} WR, "
                    f"{s['sample_count']} samples, RR={s['avg_risk_reward']:.1f}"
                )

        return "\n".join(lines)

    def _gen_memory_stats(self) -> str:
        """Generate memory system stats."""
        if not self.memory:
            return "[Memory Stats] No memory system configured"

        stats = self.memory.get_memory_stats()
        return (
            f"🧠 Memory Stats\n"
            f"  Patterns: {stats.get('patterns_stored', 0)} "
            f"({stats.get('high_confidence_patterns', 0)} high confidence)\n"
            f"  Lessons: {stats.get('lessons_stored', 0)}\n"
            f"  Cache Age: {stats.get('cache_age_seconds', 0):.0f}s"
        )

    # ── Delivery ────────────────────────────────────────

    async def _deliver(self, report: ScheduledReport, content: str):
        """Deliver report content based on delivery mode."""
        if report.delivery == DeliveryMode.LOG:
            logger.info(f"[{report.name}]\n{content}")
        elif report.delivery == DeliveryMode.PRINT:
            print(f"\n{'='*50}\n{content}\n{'='*50}")
        elif report.delivery == DeliveryMode.WEBHOOK and report.webhook_url:
            # Placeholder — actual webhook delivery
            logger.info(f"[{report.name}] Would POST to {report.webhook_url}")
        elif report.delivery == DeliveryMode.CALLBACK and report.callback:
            report.callback(content)
        else:
            logger.info(f"[{report.name}]\n{content}")
