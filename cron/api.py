from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from cron.election import LeaderElectionCoordinator, NodeRole
from cron.scheduler import DistributedScheduler

app = FastAPI(
    title="Distributed Cron Leader",
    version="0.2.0",
    description="Distributed Cron Scheduler with Atomic Lease Leader Election & Exactly-Once Execution",
)

coordinator = LeaderElectionCoordinator(node_id="worker-01")
sched = DistributedScheduler(node_id="worker-01", coordinator=coordinator)


class JobReq(BaseModel):
    name: str = Field(..., min_length=1, description="Unique job identifier")
    cron_expr: str = Field(..., min_length=1, description="5-field cron or interval macro (@hourly, @every 10s)")
    timeout_sec: Optional[float] = Field(default=30.0, ge=1.0)
    retry_limit: Optional[int] = Field(default=3, ge=0)


class JobTriggerReq(BaseModel):
    force: Optional[bool] = Field(default=False, description="Bypass leader check and slot deduplication")


@app.get("/healthz", tags=["Health"])
def health_check() -> Dict[str, Any]:
    """Kubernetes liveness and readiness health probe."""
    return {
        "status": "healthy",
        "node_id": sched.node_id,
        "role": coordinator.role.value,
        "is_leader": sched.is_leader,
        "term": coordinator.term,
        "fencing_token": coordinator.fencing_token,
    }


@app.get("/api/v1/cluster/status", tags=["Cluster"])
def cluster_status() -> Dict[str, Any]:
    """Retrieve cluster leader election lease and node status."""
    return {
        "node_id": sched.node_id,
        "role": coordinator.role.value,
        "term": coordinator.term,
        "fencing_token": coordinator.fencing_token,
        "active_leader_id": coordinator.active_leader_id,
        "lease_ttl_seconds": coordinator.lease_ttl_seconds,
        "active_jobs_count": len(sched.jobs),
    }


@app.post("/api/v1/jobs", tags=["Jobs"], status_code=status.HTTP_201_CREATED)
def add_job(req: JobReq) -> Dict[str, Any]:
    """Schedule a new distributed cron job."""
    return sched.schedule_job(
        name=req.name,
        cron_expr=req.cron_expr,
        timeout_sec=req.timeout_sec or 30.0,
        retry_limit=req.retry_limit or 3,
    )


@app.get("/api/v1/jobs", tags=["Jobs"])
def list_jobs() -> Dict[str, Any]:
    """List all registered jobs and their next scheduled execution times."""
    return {"jobs": sched.jobs, "count": len(sched.jobs)}


@app.post("/api/v1/jobs/{name}/trigger", tags=["Jobs"])
def trigger_job(name: str, req: Optional[JobTriggerReq] = None) -> Dict[str, Any]:
    """Manually trigger a job execution with exactly-once deduplication."""
    force = req.force if req else False
    try:
        return sched.execute_job(name=name, force=force)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Job '{name}' not found")


@app.get("/api/v1/jobs/{name}/history", tags=["Jobs"])
def get_job_history(name: str) -> Dict[str, Any]:
    """Retrieve execution history logs for a specific job."""
    if name not in sched.jobs and not sched.get_history(name):
        raise HTTPException(status_code=404, detail=f"Job '{name}' not found")
    history = sched.get_history(name)
    return {"job": name, "history": history, "total_records": len(history)}


@app.delete("/api/v1/jobs/{name}", tags=["Jobs"])
def delete_job(name: str) -> Dict[str, Any]:
    """Cancel and remove a job from the distributed scheduler."""
    if not sched.cancel_job(name):
        raise HTTPException(status_code=404, detail=f"Job '{name}' not found")
    return {"status": "deleted", "job": name}
