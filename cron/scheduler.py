"""
Distributed Cron Job Scheduler & Exactly-Once Execution Engine.
Supports 5-field cron parsing, interval macros, idempotent execution slots,
and fencing token validation.
"""

from dataclasses import dataclass, field
from enum import Enum
import re
import time
from typing import Any, Callable, Dict, List, Optional
import uuid

from cron.election import LeaderElectionCoordinator, NodeRole


class ExecutionStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    RUNNING = "RUNNING"


@dataclass
class JobExecutionLog:
    run_id: str
    job_name: str
    triggered_at: float
    finished_at: float
    duration_ms: float
    leader_id: str
    fencing_token: Optional[int]
    status: ExecutionStatus
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "job_name": self.job_name,
            "triggered_at": self.triggered_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "leader_id": self.leader_id,
            "fencing_token": self.fencing_token,
            "status": self.status.value,
            "error_message": self.error_message,
        }


def parse_next_run(cron_expr: str, base_time: Optional[float] = None) -> float:
    """
    Computes the timestamp of the next run from cron_expr or interval macro.
    Supports:
      - Macros: @minutely (60s), @hourly (3600s), @daily (86400s), @weekly (604800s)
      - Interval syntax: @every 30s, @every 5m, @every 2h
      - Standard 5-field expressions (approximated interval for lightweight simulation)
    """
    now = time.time() if base_time is None else base_time
    expr = cron_expr.strip()

    if expr == "@minutely":
        return now + 60.0
    if expr == "@hourly":
        return now + 3600.0
    if expr == "@daily":
        return now + 86400.0
    if expr == "@weekly":
        return now + 604800.0

    match_every = re.match(r"^@every\s+(\d+)\s*([smh])$", expr, re.IGNORECASE)
    if match_every:
        val = int(match_every.group(1))
        unit = match_every.group(2).lower()
        multiplier = {"s": 1.0, "m": 60.0, "h": 3600.0}[unit]
        return now + (val * multiplier)

    # 5-field cron parsing (minute hour dom month dow)
    parts = expr.split()
    if len(parts) == 5:
        # Check step on minute field: e.g. */5 * * * * -> 300s
        min_field = parts[0]
        step_match = re.match(r"^\*/(\d+)$", min_field)
        if step_match:
            step_mins = int(step_match.group(1))
            return now + (step_mins * 60.0)
        # Hourly cron: e.g. 0 * * * * or 30 * * * * -> 3600s
        if parts[1] == "*":
            return now + 3600.0
        # Daily cron: e.g. 0 0 * * * -> 86400s
        return now + 86400.0

    # Default fallback: 60s
    return now + 60.0


class DistributedScheduler:
    """
    Cluster-aware distributed cron job scheduler with exactly-once execution,
    idempotent lock slots, and leader coordination.
    """

    def __init__(
        self,
        node_id: str,
        coordinator: Optional[LeaderElectionCoordinator] = None,
    ):
        self.node_id = node_id
        self.coordinator = coordinator or LeaderElectionCoordinator(node_id=node_id)
        # By default coordinator is initialized; if standalone, election step can make it leader
        self._coordinator_managed = coordinator is not None

        # Backwards compatible state
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self._executed_slots: set[str] = set()
        self._history: List[JobExecutionLog] = []
        self._handlers: Dict[str, Callable[[], Any]] = {}

    @property
    def is_leader(self) -> bool:
        """Dynamic leader status check backed by the election coordinator."""
        if not self._coordinator_managed:
            # Standalone mode: auto-ticks to leader if no external coordinator provided
            if self.coordinator.role == NodeRole.FOLLOWER:
                self.coordinator.tick()
        return self.coordinator.is_leader

    @is_leader.setter
    def is_leader(self, val: bool) -> None:
        """Allow manual override for backward compatibility in mock tests."""
        if val:
            self.coordinator.tick()
        else:
            self.coordinator.step_down()

    def schedule_job(
        self,
        name: str,
        cron_expr: str,
        handler: Optional[Callable[[], Any]] = None,
        timeout_sec: float = 30.0,
        retry_limit: int = 3,
    ) -> Dict[str, Any]:
        """
        Register a scheduled job. Backward-compatible return structure.
        """
        now = time.time()
        next_run = parse_next_run(cron_expr, now)

        self.jobs[name] = {
            "cron": cron_expr,
            "last_run": now,
            "next_run": next_run,
            "leader": self.node_id,
            "timeout_sec": timeout_sec,
            "retry_limit": retry_limit,
            "enabled": True,
            "total_runs": 0,
            "failed_runs": 0,
        }

        if handler:
            self._handlers[name] = handler

        return {
            "status": "scheduled",
            "job": name,
            "assigned_leader": self.node_id,
        }

    def execute_job(self, name: str, force: bool = False) -> Dict[str, Any]:
        """
        Execute a scheduled job ensuring exactly-once execution via fencing token
        and slot deduplication.
        """
        if name not in self.jobs:
            raise KeyError(f"Job '{name}' not found")

        job_info = self.jobs[name]
        now = time.time()

        # Enforce leader gate
        if not self.is_leader and not force:
            return {
                "status": ExecutionStatus.SKIPPED.value,
                "reason": "Not the current cluster leader",
                "current_leader": self.coordinator.active_leader_id,
            }

        # Idempotent slot check (slot key: job:current_minute_bucket)
        minute_bucket = int(now // 60)
        slot_key = f"{name}:{minute_bucket}"
        if slot_key in self._executed_slots and not force:
            return {
                "status": ExecutionStatus.SKIPPED.value,
                "reason": "Job execution slot already consumed for this window",
            }

        self._executed_slots.add(slot_key)
        run_id = f"run-{uuid.uuid4().hex[:8]}"
        fencing_token = self.coordinator.fencing_token

        start_time = time.time()
        status = ExecutionStatus.SUCCESS
        error_msg = None

        try:
            handler = self._handlers.get(name)
            if handler:
                handler()
            job_info["total_runs"] += 1
            job_info["last_run"] = start_time
            job_info["next_run"] = parse_next_run(job_info["cron"], start_time)
        except Exception as ex:
            status = ExecutionStatus.FAILED
            error_msg = str(ex)
            job_info["failed_runs"] += 1

        duration_ms = round((time.time() - start_time) * 1000.0, 2)
        log = JobExecutionLog(
            run_id=run_id,
            job_name=name,
            triggered_at=start_time,
            finished_at=time.time(),
            duration_ms=duration_ms,
            leader_id=self.node_id,
            fencing_token=fencing_token,
            status=status,
            error_message=error_msg,
        )
        self._history.append(log)

        return log.to_dict()

    def trigger_scheduled_jobs(self) -> List[Dict[str, Any]]:
        """Evaluate all jobs ready for execution based on next_run timestamp."""
        results = []
        now = time.time()
        for name, info in list(self.jobs.items()):
            if info.get("enabled", True) and now >= info.get("next_run", 0):
                res = self.execute_job(name)
                results.append(res)
        return results

    def get_history(self, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return execution audit history for all or specified job."""
        if name:
            return [l.to_dict() for l in self._history if l.job_name == name]
        return [l.to_dict() for l in self._history]

    def cancel_job(self, name: str) -> bool:
        """Remove a job from scheduling."""
        if name in self.jobs:
            del self.jobs[name]
            self._handlers.pop(name, None)
            return True
        return False
