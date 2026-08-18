from fastapi import FastAPI
from pydantic import BaseModel
from cron.leader import DistributedScheduler

app = FastAPI(title="Distributed Cron Leader", version="0.1.0")
sched = DistributedScheduler(node_id="worker-01")

class JobReq(BaseModel):
    name: str
    cron_expr: str

@app.post("/api/v1/jobs")
def add_job(req: JobReq):
    return sched.schedule_job(req.name, req.cron_expr)
