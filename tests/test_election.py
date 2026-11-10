import time
import pytest
from cron.election import (
    InMemoryLeaseBackend,
    LeaderElectionCoordinator,
    LeaseRecord,
    NodeRole,
)


def test_single_node_election():
    backend = InMemoryLeaseBackend()
    coord = LeaderElectionCoordinator("node-A", lease_ttl_seconds=10.0, backend=backend)
    assert coord.role == NodeRole.FOLLOWER
    assert coord.is_leader is False

    role = coord.tick()
    assert role == NodeRole.LEADER
    assert coord.is_leader is True
    assert coord.active_leader_id == "node-A"
    assert coord.fencing_token is not None


def test_multi_node_leader_contention():
    backend = InMemoryLeaseBackend()
    node1 = LeaderElectionCoordinator("node-1", backend=backend, lease_ttl_seconds=5.0)
    node2 = LeaderElectionCoordinator("node-2", backend=backend, lease_ttl_seconds=5.0)

    # Node 1 acquires leadership
    assert node1.tick() == NodeRole.LEADER
    assert node1.is_leader is True

    # Node 2 tries and becomes FOLLOWER since lease is occupied
    assert node2.tick() == NodeRole.FOLLOWER
    assert node2.is_leader is False
    assert node2.active_leader_id == "node-1"


def test_leader_heartbeat_renewal():
    backend = InMemoryLeaseBackend()
    node = LeaderElectionCoordinator("node-alpha", backend=backend, lease_ttl_seconds=5.0)
    node.tick()
    assert node.role == NodeRole.LEADER

    initial_token = node.fencing_token
    # Second tick renews lease
    assert node.tick() == NodeRole.LEADER
    # Fencing token remains stable during renewal within same term
    assert node.fencing_token == initial_token


def test_leader_step_down_and_failover():
    backend = InMemoryLeaseBackend()
    node1 = LeaderElectionCoordinator("node-primary", backend=backend, lease_ttl_seconds=10.0)
    node2 = LeaderElectionCoordinator("node-secondary", backend=backend, lease_ttl_seconds=10.0)

    node1.tick()
    assert node1.is_leader is True

    # Primary voluntarily abdicates
    node1.step_down()
    assert node1.role == NodeRole.FOLLOWER
    assert node1.is_leader is False

    # Secondary immediately acquires vacant lease
    assert node2.tick() == NodeRole.LEADER
    assert node2.is_leader is True
    assert node2.active_leader_id == "node-secondary"


def test_lease_expiry_failover():
    backend = InMemoryLeaseBackend()
    # Very short TTL for test
    node1 = LeaderElectionCoordinator("node-short", backend=backend, lease_ttl_seconds=0.05)
    node2 = LeaderElectionCoordinator("node-backup", backend=backend, lease_ttl_seconds=5.0)

    node1.tick()
    assert node1.is_leader is True

    # Wait for node1's lease to expire
    time.sleep(0.06)

    # Node 2 detects expired lease and acquires leadership with higher fencing token
    assert node2.tick() == NodeRole.LEADER
    assert node2.is_leader is True
    assert node2.active_leader_id == "node-backup"
    assert node2.fencing_token > node1.fencing_token


def test_fencing_token_strictly_monotonic():
    backend = InMemoryLeaseBackend()
    tokens = []
    for i in range(5):
        coord = LeaderElectionCoordinator(f"worker-{i}", backend=backend, lease_ttl_seconds=0.01)
        coord.tick()
        tokens.append(coord.fencing_token)
        coord.step_down()

    # Each new term acquisition yields a strictly greater fencing token
    for a, b in zip(tokens[:-1], tokens[1:]):
        assert b > a
