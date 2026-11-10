"""
Distributed Leader Election & Lease Lock Engine.
Implements atomic lease acquisition, heartbeat renewal, and monotonically increasing
fencing tokens for split-brain prevention.
"""

from dataclasses import dataclass
from enum import Enum
import threading
import time
from typing import Dict, Optional, Tuple


class NodeRole(str, Enum):
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"


@dataclass(frozen=True)
class LeaseRecord:
    """Immutable record of the current distributed leader lease."""
    leader_id: str
    term: int
    fencing_token: int
    expires_at: float
    created_at: float

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.expires_at


class InMemoryLeaseBackend:
    """
    Thread-safe in-memory lease coordination store with atomic Compare-And-Swap (CAS).
    Simulates etcd/Consul/Redis distributed lock primitives for distributed leader election.
    """

    def __init__(self):
        self._leases: Dict[str, LeaseRecord] = {}
        self._lock = threading.Lock()
        self._global_fencing_counter = 1000

    def get_lease(self, key: str) -> Optional[LeaseRecord]:
        with self._lock:
            lease = self._leases.get(key)
            if lease and lease.is_expired:
                del self._leases[key]
                return None
            return lease

    def try_acquire(
        self, key: str, candidate_id: str, ttl_seconds: float, term: int
    ) -> Tuple[bool, LeaseRecord]:
        """
        Atomically attempt to acquire the leader lease if vacant or expired.
        Returns (True, new_lease) if acquired, (False, current_lease) otherwise.
        """
        now = time.time()
        with self._lock:
            current = self._leases.get(key)
            if current is not None and not current.is_expired and current.leader_id != candidate_id:
                return False, current

            self._global_fencing_counter += 1
            new_record = LeaseRecord(
                leader_id=candidate_id,
                term=term,
                fencing_token=self._global_fencing_counter,
                expires_at=now + ttl_seconds,
                created_at=now,
            )
            self._leases[key] = new_record
            return True, new_record

    def try_renew(
        self, key: str, leader_id: str, ttl_seconds: float, expected_token: int
    ) -> Tuple[bool, Optional[LeaseRecord]]:
        """
        Renew an active lease. Requires matching leader_id and fencing_token.
        """
        now = time.time()
        with self._lock:
            current = self._leases.get(key)
            if (
                current is None
                or current.leader_id != leader_id
                or current.fencing_token != expected_token
                or current.is_expired
            ):
                return False, current

            renewed = LeaseRecord(
                leader_id=leader_id,
                term=current.term,
                fencing_token=current.fencing_token,
                expires_at=now + ttl_seconds,
                created_at=current.created_at,
            )
            self._leases[key] = renewed
            return True, renewed

    def release(self, key: str, leader_id: str, expected_token: int) -> bool:
        """Explicitly step down and release the leader lease."""
        with self._lock:
            current = self._leases.get(key)
            if current and current.leader_id == leader_id and current.fencing_token == expected_token:
                del self._leases[key]
                return True
            return False


class LeaderElectionCoordinator:
    """
    Coordinates leader election state transitions for a local node within a distributed cluster.
    """

    def __init__(
        self,
        node_id: str,
        lease_key: str = "cron_scheduler_leader",
        lease_ttl_seconds: float = 5.0,
        backend: Optional[InMemoryLeaseBackend] = None,
    ):
        self.node_id = node_id
        self.lease_key = lease_key
        self.lease_ttl_seconds = lease_ttl_seconds
        self.backend = backend or InMemoryLeaseBackend()

        self._role = NodeRole.FOLLOWER
        self._term = 1
        self._current_lease: Optional[LeaseRecord] = None
        self._lock = threading.Lock()

    @property
    def role(self) -> NodeRole:
        with self._lock:
            return self._role

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self._role == NodeRole.LEADER

    @property
    def term(self) -> int:
        with self._lock:
            return self._term

    @property
    def fencing_token(self) -> Optional[int]:
        with self._lock:
            return self._current_lease.fencing_token if self._current_lease else None

    @property
    def active_leader_id(self) -> Optional[str]:
        lease = self.backend.get_lease(self.lease_key)
        return lease.leader_id if lease and not lease.is_expired else None

    def tick(self) -> NodeRole:
        """
        Evaluate heartbeats and state machine step:
        - If LEADER: renew lease. If renewal fails, step down to FOLLOWER.
        - If FOLLOWER: inspect lease. If expired/absent, transition to CANDIDATE and attempt acquisition.
        - If CANDIDATE: attempt to acquire lease. If successful, become LEADER.
        """
        with self._lock:
            if self._role == NodeRole.LEADER:
                if self._current_lease is None:
                    self._role = NodeRole.FOLLOWER
                    return self._role

                success, renewed = self.backend.try_renew(
                    self.lease_key,
                    self.node_id,
                    self.lease_ttl_seconds,
                    self._current_lease.fencing_token,
                )
                if success and renewed:
                    self._current_lease = renewed
                    return NodeRole.LEADER
                else:
                    self._role = NodeRole.FOLLOWER
                    self._current_lease = None
                    return NodeRole.FOLLOWER

            # Non-leader check
            lease = self.backend.get_lease(self.lease_key)
            if lease is None or lease.is_expired:
                self._role = NodeRole.CANDIDATE
                self._term += 1
                acquired, new_lease = self.backend.try_acquire(
                    self.lease_key, self.node_id, self.lease_ttl_seconds, self._term
                )
                if acquired:
                    self._role = NodeRole.LEADER
                    self._current_lease = new_lease
                    return NodeRole.LEADER
                else:
                    self._role = NodeRole.FOLLOWER
                    self._current_lease = new_lease
                    return NodeRole.FOLLOWER
            else:
                self._role = NodeRole.FOLLOWER
                self._current_lease = lease
                return NodeRole.FOLLOWER

    def step_down(self) -> None:
        """Voluntarily abdicate leadership."""
        with self._lock:
            if self._role == NodeRole.LEADER and self._current_lease:
                self.backend.release(
                    self.lease_key, self.node_id, self._current_lease.fencing_token
                )
            self._role = NodeRole.FOLLOWER
            self._current_lease = None
