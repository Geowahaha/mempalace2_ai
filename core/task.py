"""
Task System — Manages concurrent, observable work units.

Inspired by Claude Code's task lifecycle:
  pending → running → completed | failed | killed

Each scan, analysis, and trade execution is a tracked task.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.state import GlobalState

logger = logging.getLogger("mempalace2.task")


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    KILLED = "killed"


class TaskType(str, Enum):
    MARKET_SCAN = "market_scan"
    ANALYSIS = "analysis"
    SIGNAL_GENERATION = "signal_generation"
    RISK_CHECK = "risk_check"
    TRADE_EXECUTION = "trade_execution"
    POSITION_MONITOR = "position_monitor"
    PORTFOLIO_REBALANCE = "portfolio_rebalance"


@dataclass
class Task:
    """A single unit of work in the system."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    type: TaskType = TaskType.MARKET_SCAN
    name: str = ""
    description: str = ""
    status: TaskStatus = TaskStatus.PENDING
    priority: int = 0  # higher = more important

    # Timing
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Result
    result: Optional[Any] = None
    error: Optional[str] = None

    # Lifecycle
    notified: bool = False
    callback: Optional[Callable] = None

    # Metadata
    symbol: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.KILLED)

    @property
    def elapsed_ms(self) -> float:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds() * 1000
        elif self.started_at:
            return (datetime.now(timezone.utc) - self.started_at).total_seconds() * 1000
        return 0.0


class TaskManager:
    """
    Manages task lifecycle, concurrency, and notifications.
    Runs a background loop for polling and cleanup.
    """

    def __init__(self, state: "GlobalState"):
        self.state = state
        self.tasks: Dict[str, Task] = {}
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def start(self):
        """Start the task manager background loop."""
        self._running = True
        logger.info("TaskManager started")

    async def stop(self):
        """Stop the task manager and kill all running tasks."""
        self._running = False
        for task in list(self.tasks.values()):
            if task.status == TaskStatus.RUNNING:
                await self.kill_task(task.id)
        logger.info("TaskManager stopped")

    def create_task(
        self,
        task_type: TaskType,
        name: str,
        description: str = "",
        symbol: str = "",
        priority: int = 0,
        callback: Optional[Callable] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Task:
        """Create and register a new task."""
        task = Task(
            type=task_type,
            name=name,
            description=description,
            symbol=symbol,
            priority=priority,
            callback=callback,
            metadata=metadata or {},
        )
        self.tasks[task.id] = task
        logger.debug(f"Task created: {task.id} [{task.type.value}] {task.name}")
        return task

    async def run_task(self, task: Task, coro) -> Task:
        """Execute a task with the given coroutine."""
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(timezone.utc)

        try:
            result = await coro
            task.result = result
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(timezone.utc)

            logger.info(
                f"Task completed: {task.id} [{task.type.value}] "
                f"{task.name} ({task.elapsed_ms:.0f}ms)"
            )

            if task.callback:
                await task.callback(task)

        except asyncio.CancelledError:
            task.status = TaskStatus.KILLED
            task.completed_at = datetime.now(timezone.utc)
            logger.warning(f"Task killed: {task.id} {task.name}")

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.completed_at = datetime.now(timezone.utc)
            logger.error(f"Task failed: {task.id} {task.name} — {e}")

        return task

    async def kill_task(self, task_id: str) -> bool:
        """Kill a running task."""
        task = self.tasks.get(task_id)
        if task and task.status == TaskStatus.RUNNING:
            task.status = TaskStatus.KILLED
            task.completed_at = datetime.now(timezone.utc)
            return True
        return False

    def get_active_tasks(self, task_type: Optional[TaskType] = None) -> List[Task]:
        """Get all running tasks, optionally filtered by type."""
        tasks = [t for t in self.tasks.values() if t.status == TaskStatus.RUNNING]
        if task_type:
            tasks = [t for t in tasks if t.type == task_type]
        return sorted(tasks, key=lambda t: t.priority, reverse=True)

    def get_stats(self) -> Dict[str, int]:
        """Get task statistics."""
        stats = {s.value: 0 for s in TaskStatus}
        for task in self.tasks.values():
            stats[task.status.value] += 1
        return stats

    def evict_terminal(self, max_age_seconds: int = 3600):
        """Remove old terminal tasks to free memory."""
        now = datetime.now(timezone.utc)
        to_remove = []
        for task_id, task in self.tasks.items():
            if task.is_terminal and task.notified:
                if task.completed_at:
                    age = (now - task.completed_at).total_seconds()
                    if age > max_age_seconds:
                        to_remove.append(task_id)
        for task_id in to_remove:
            del self.tasks[task_id]
        if to_remove:
            logger.debug(f"Evicted {len(to_remove)} terminal tasks")
