from cron.leader import DistributedScheduler

def test_scheduler():
    s = DistributedScheduler("node-1")
    res = s.schedule_job("cleanup", "0 * * * *")
    assert res["status"] == "scheduled"
