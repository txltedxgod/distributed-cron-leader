"""
Compatibility wrapper for cron.leader.
Re-exports DistributedScheduler, LeaderElectionCoordinator, and NodeRole.
"""

from cron.election import LeaderElectionCoordinator, NodeRole, LeaseRecord
from cron.scheduler import DistributedScheduler, ExecutionStatus, JobExecutionLog

__all__ = [
    "DistributedScheduler",
    "LeaderElectionCoordinator",
    "NodeRole",
    "LeaseRecord",
    "ExecutionStatus",
    "JobExecutionLog",
]
