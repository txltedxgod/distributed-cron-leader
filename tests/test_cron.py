import time
import pytest
from cron.leader import DistributedScheduler
from cron.scheduler import ExecutionStatus, parse_next_run
from cron.election import LeaderElectionCoordinator, InMemoryLeaseBackend, NodeRole


def test_scheduler():
    """Verify backward compatibility of test_scheduler."""
    s = DistributedScheduler("node-1")
    res = s.schedule_job("cleanup", "0 * * * *")
    assert res["status"] == "scheduled"
    assert res["job"] == "cleanup"
    assert res["assigned_leader"] == "node-1"
    assert "cleanup" in s.jobs


def test_cron_macro_parsing():
    now = 1000000.0
    assert parse_next_run("@minutely", now) == now + 60.0
    assert parse_next_run("@hourly", now) == now + 3600.0
    assert parse_next_run("@daily", now) == now + 86400.0
    assert parse_next_run("@weekly", now) == now + 604800.0
    assert parse_next_run("@every 15s", now) == now + 15.0
    assert parse_next_run("@every 10m", now) == now + 600.0
    assert parse_next_run("*/10 * * * *", now) == now + 600.0


def test_job_execution_as_leader():
    executed = []

    def mock_task():
        executed.append("done")

    s = DistributedScheduler("node-1")
    s.schedule_job("backup-db", "@every 10s", handler=mock_task)
    
    log = s.execute_job("backup-db")
    assert log["status"] == ExecutionStatus.SUCCESS.value
    assert log["job_name"] == "backup-db"
    assert log["leader_id"] == "node-1"
    assert log["duration_ms"] >= 0
    assert len(executed) == 1
    assert s.jobs["backup-db"]["total_runs"] == 1


def test_job_execution_as_follower_skipped():
    backend = InMemoryLeaseBackend()
    c1 = LeaderElectionCoordinator("leader-node", backend=backend)
    c1.tick()  # leader-node acquires leadership

    c2 = LeaderElectionCoordinator("follower-node", backend=backend)
    c2.tick()  # follower-node detects active leader

    s2 = DistributedScheduler("follower-node", coordinator=c2)
    s2.schedule_job("report-gen", "@hourly")

    res = s2.execute_job("report-gen")
    assert res["status"] == ExecutionStatus.SKIPPED.value
    assert "Not the current cluster leader" in res["reason"]


def test_job_slot_deduplication():
    s = DistributedScheduler("node-1")
    s.schedule_job("sync-job", "@hourly")

    # First run in current window succeeds
    res1 = s.execute_job("sync-job")
    assert res1["status"] == ExecutionStatus.SUCCESS.value

    # Immediate second run in same window is skipped to ensure exactly-once execution
    res2 = s.execute_job("sync-job")
    assert res2["status"] == ExecutionStatus.SKIPPED.value
    assert "already consumed" in res2["reason"]

    # Forced run bypasses slot lock
    res3 = s.execute_job("sync-job", force=True)
    assert res3["status"] == ExecutionStatus.SUCCESS.value


def test_job_failure_handling():
    def failing_task():
        raise RuntimeError("Database connection timeout")

    s = DistributedScheduler("node-1")
    s.schedule_job("failing-job", "@hourly", handler=failing_task)

    log = s.execute_job("failing-job")
    assert log["status"] == ExecutionStatus.FAILED.value
    assert "Database connection timeout" in log["error_message"]
    assert s.jobs["failing-job"]["failed_runs"] == 1


def test_job_cancellation():
    s = DistributedScheduler("node-1")
    s.schedule_job("temp-job", "@daily")
    assert "temp-job" in s.jobs

    assert s.cancel_job("temp-job") is True
    assert "temp-job" not in s.jobs
    assert s.cancel_job("temp-job") is False
