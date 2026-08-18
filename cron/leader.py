import time
from typing import Dict, Any

class DistributedScheduler:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.is_leader = True
        self.jobs = {}

    def schedule_job(self, name: str, cron_expr: str) -> Dict[str, Any]:
        self.jobs[name] = {"cron": cron_expr, "last_run": time.time(), "leader": self.node_id}
        return {"status": "scheduled", "job": name, "assigned_leader": self.node_id}
