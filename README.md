# ⏰ Distributed Cron Leader

[![CI](https://github.com/txltedxgod/distributed-cron-leader/actions/workflows/ci.yml/badge.svg)](https://github.com/txltedxgod/distributed-cron-leader/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Architecture: Distributed](https://img.shields.io/badge/Architecture-Distributed%20Consensus-orange.svg)](#architecture)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)

Production-grade distributed cron scheduler with atomic lease-based leader election, split-brain fencing tokens, and exactly-once job execution guarantees in Python & Docker.

---

## 🏛️ Architecture & Leader Election Protocol

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 01 (Candidate)
    participant Backend as Distributed Lease Store (CAS)
    participant W2 as Worker 02 (Follower)
    participant Exec as Target Execution Engine

    Note over W1,Backend: Lease Acquisition & Leader Election
    W1->>Backend: try_acquire("cron_leader", candidate="W1", TTL=5s, term=1)
    Backend-->>W1: (True, LeaseRecord{token=1001, expires=T+5})
    Note over W1: W1 becomes LEADER (Token 1001)

    W2->>Backend: get_lease("cron_leader")
    Backend-->>W2: LeaseRecord{leader="W1", active=True}
    Note over W2: W2 remains FOLLOWER

    Note over W1,Exec: Scheduled Cron Trigger (Exactly-Once Slot)
    W1->>Exec: execute_job("backup-db", slot="backup:17942", token=1001)
    Exec-->>W1: SUCCESS (Recorded in Audit Ledger)

    Note over W1,Backend: Heartbeat Renewal
    W1->>Backend: try_renew("cron_leader", token=1001, TTL=5s)
    Backend-->>W1: (True, LeaseRecord{expires=T+10})

    Note over W1,W2: Failover on Leader Crash
    Note over W1: W1 crashes / network partition
    Note over Backend: TTL expires (T+10)
    W2->>Backend: try_acquire("cron_leader", candidate="W2", TTL=5s, term=2)
    Backend-->>W2: (True, LeaseRecord{token=1002, expires=T+15})
    Note over W2: W2 elected NEW LEADER (Token 1002)
```

---

## ✨ Key Features

- **Atomic Leader Election**: Distributed compare-and-swap (CAS) lease lock with configurable TTL and proactive heartbeat renewals.
- **Split-Brain & Zombie Defense**: Monotonically increasing fencing tokens (epoch tokens) guarantee that partitioned or delayed nodes cannot perform duplicate execution.
- **Exactly-Once Execution Slots**: Time-window slot deduplication (`job:window_epoch`) ensures cron tasks fire exactly once across the cluster, even during failover transitions.
- **Flexible Schedule Expressions**:
  - Standard 5-field cron syntax (`*/5 * * * *`, `0 0 * * *`).
  - Human-friendly interval macros (`@hourly`, `@daily`, `@weekly`, `@minutely`).
  - Relative duration intervals (`@every 15s`, `@every 5m`, `@every 2h`).
- **Audit History & Execution Telemetry**: Tracks start time, execution duration (ms), assigned leader, fencing token, retry attempts, and detailed error traces.
- **REST API & Cloud-Native**: Production FastAPI interface with `/healthz`, `/api/v1/cluster/status`, job management CRUD, and manual ad-hoc triggers.

---

## 🚀 Quickstart

### Local Setup

```bash
git clone https://github.com/txltedxgod/distributed-cron-leader.git
cd distributed-cron-leader

python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run Tests

```bash
pytest -v tests/
```

### Run Server

```bash
uvicorn cron.api:app --host 0.0.0.0 --port 8000 --reload
```

---

## 📡 REST API Reference

### 1. Cluster Status
`GET /api/v1/cluster/status`

**Response (200 OK):**
```json
{
  "node_id": "worker-01",
  "role": "LEADER",
  "term": 1,
  "fencing_token": 1001,
  "active_leader_id": "worker-01",
  "lease_ttl_seconds": 5.0,
  "active_jobs_count": 3
}
```

### 2. Schedule a New Cron Job
`POST /api/v1/jobs`

**Request Payload:**
```json
{
  "name": "nightly-database-backup",
  "cron_expr": "@daily",
  "timeout_sec": 60.0,
  "retry_limit": 3
}
```

**Response (201 Created):**
```json
{
  "status": "scheduled",
  "job": "nightly-database-backup",
  "assigned_leader": "worker-01"
}
```

### 3. Trigger Job Execution (Ad-Hoc)
`POST /api/v1/jobs/{name}/trigger`

Triggers immediate job execution verified against the current cluster leader lease and slot lock.

---

## 🛡️ License

MIT License. See [LICENSE](LICENSE) for details.
