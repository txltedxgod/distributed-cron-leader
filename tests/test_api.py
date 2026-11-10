import pytest
from fastapi.testclient import TestClient
from cron.api import app, sched, coordinator

client = TestClient(app)


def test_healthz_endpoint():
    response = client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "node_id" in data
    assert "is_leader" in data
    assert "fencing_token" in data


def test_cluster_status_endpoint():
    response = client.get("/api/v1/cluster/status")
    assert response.status_code == 200
    data = response.json()
    assert "node_id" in data
    assert "active_leader_id" in data
    assert "active_jobs_count" in data


def test_job_crud_lifecycle():
    # 1. Add job
    payload = {
        "name": "etl-pipeline-hourly",
        "cron_expr": "@hourly",
        "timeout_sec": 45.0,
        "retry_limit": 2,
    }
    create_resp = client.post("/api/v1/jobs", json=payload)
    assert create_resp.status_code == 201
    assert create_resp.json()["status"] == "scheduled"
    assert create_resp.json()["job"] == "etl-pipeline-hourly"

    # 2. List jobs
    list_resp = client.get("/api/v1/jobs")
    assert list_resp.status_code == 200
    assert "etl-pipeline-hourly" in list_resp.json()["jobs"]

    # 3. Trigger job
    trigger_resp = client.post(
        "/api/v1/jobs/etl-pipeline-hourly/trigger",
        json={"force": True},
    )
    assert trigger_resp.status_code == 200
    assert trigger_resp.json()["status"] == "SUCCESS"
    assert trigger_resp.json()["job_name"] == "etl-pipeline-hourly"

    # 4. Get history
    hist_resp = client.get("/api/v1/jobs/etl-pipeline-hourly/history")
    assert hist_resp.status_code == 200
    assert hist_resp.json()["total_records"] >= 1

    # 5. Delete job
    del_resp = client.delete("/api/v1/jobs/etl-pipeline-hourly")
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "deleted"

    # 6. Verify 404 on trigger of deleted job
    not_found_resp = client.post("/api/v1/jobs/etl-pipeline-hourly/trigger")
    assert not_found_resp.status_code == 404
