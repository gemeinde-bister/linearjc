"""
ExecutorRegistry - Heartbeat-based executor tracking.

Tracks live executors via periodic heartbeats. Replaces poll-based
capability discovery with a non-blocking push model.

Key features:
- No locks needed (single-threaded async)
- TTL-based staleness detection
- Fast executor lookup by job_id/version
"""

import logging
import time
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class ExecutorInfo(BaseModel):
    """Information about a registered executor."""

    executor_id: str
    hostname: str
    capabilities: dict[str, str] = Field(default_factory=dict)  # job_id -> version
    capability_types: list[str] = Field(default_factory=list)  # ["pool", "dedicated"]
    supported_formats: list[str] = Field(default_factory=list)
    last_seen: float

    model_config = ConfigDict(frozen=False)  # Allow mutation of last_seen


class ExecutorRegistry:
    """
    Tracks live executors via heartbeats.

    Single-threaded async design - no locks needed.
    All operations are O(n) where n is number of executors.
    """

    def __init__(self, ttl: float = 90.0) -> None:
        """
        Initialize executor registry.

        Args:
            ttl: Time-to-live in seconds (miss 2-3 heartbeats = stale)
        """
        self._executors: dict[str, ExecutorInfo] = {}
        self._ttl = ttl
        logger.info(f"ExecutorRegistry initialized with TTL={ttl}s")

    def on_heartbeat(self, executor_id: str, heartbeat: dict) -> None:
        """
        Handle incoming heartbeat from executor.

        Updates or creates executor entry in registry.
        Called when executor heartbeat received on linearjc/heartbeat/{executor_id}.

        Args:
            executor_id: ID of the executor (from topic)
            heartbeat: Heartbeat message payload containing:
                - hostname: Executor hostname
                - capabilities: List of {job_id, version} dicts
                - capability_types: List of capability types
                - supported_formats: List of supported archive formats
        """
        now = time.time()

        # Parse capabilities list into dict
        capabilities = {}
        for cap in heartbeat.get("capabilities", []):
            job_id = cap.get("job_id")
            version = cap.get("version")
            if job_id and version:
                capabilities[job_id] = version

        self._executors[executor_id] = ExecutorInfo(
            executor_id=executor_id,
            hostname=heartbeat.get("hostname", "unknown"),
            capabilities=capabilities,
            capability_types=heartbeat.get("capability_types", []),
            supported_formats=heartbeat.get("supported_formats", ["tar.gz"]),
            last_seen=now,
        )

        logger.debug(
            f"Heartbeat from {executor_id}: {len(capabilities)} capabilities"
        )

    def find_executor(self, job_id: str, version: str) -> str | None:
        """
        Find executor capable of running job.

        Non-blocking dict lookup - no queries, no waiting.

        Args:
            job_id: Job identifier
            version: Job version

        Returns:
            Executor ID if found, None otherwise
        """
        now = time.time()
        for executor_id, info in self._executors.items():
            # Skip stale executors
            if now - info.last_seen > self._ttl:
                continue
            # Check for exact version match
            if info.capabilities.get(job_id) == version:
                logger.debug(f"Found executor {executor_id} for {job_id} v{version}")
                return executor_id

        logger.debug(f"No executor found for {job_id} v{version}")
        return None

    def get_pool_executors(self) -> list[str]:
        """
        Get all live pool executors.

        Returns:
            List of executor IDs with "pool" capability type
        """
        now = time.time()
        return [
            executor_id
            for executor_id, info in self._executors.items()
            if now - info.last_seen <= self._ttl
            and "pool" in info.capability_types
        ]

    def get_all_live(self) -> list[ExecutorInfo]:
        """
        Get all live (non-stale) executors.

        Returns:
            List of ExecutorInfo for live executors
        """
        now = time.time()
        return [
            info
            for info in self._executors.values()
            if now - info.last_seen <= self._ttl
        ]

    def prune_stale(self) -> list[str]:
        """
        Remove executors that haven't sent heartbeats recently.

        Should be called periodically (e.g., every 30s).
        Executors are presumed dead after 3 missed heartbeats (TTL = 90s).

        Returns:
            List of executor IDs that were removed (for cleanup of their jobs)
        """
        now = time.time()
        stale_ids = [
            executor_id
            for executor_id, info in self._executors.items()
            if now - info.last_seen > self._ttl
        ]

        for executor_id in stale_ids:
            del self._executors[executor_id]
            logger.warning(f"Executor presumed dead (no heartbeat for {self._ttl}s): {executor_id}")

        if stale_ids:
            logger.info(f"Pruned {len(stale_ids)} stale executor(s)")

        return stale_ids

    def get(self, executor_id: str) -> ExecutorInfo | None:
        """Get executor info by ID."""
        return self._executors.get(executor_id)

    def has_job(self, executor_id: str, job_id: str, version: str) -> bool:
        """Check if executor has specific job version cached."""
        info = self._executors.get(executor_id)
        if not info:
            return False
        return info.capabilities.get(job_id) == version

    def __len__(self) -> int:
        """Return number of registered executors (including stale)."""
        return len(self._executors)

    def live_count(self) -> int:
        """Return number of live (non-stale) executors."""
        now = time.time()
        return sum(
            1 for info in self._executors.values()
            if now - info.last_seen <= self._ttl
        )
