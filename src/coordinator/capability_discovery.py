"""
Capability discovery using poll-based query/response pattern.

Coordinator actively queries executors for capabilities.
Executors respond on-demand with current state.
"""
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExecutorInfo:
    """Information about an executor and its capabilities."""
    executor_id: str
    hostname: str
    capabilities: List[Dict[str, str]]
    supported_formats: List[str]
    last_seen: float
    last_query_id: str


@dataclass
class QueryState:
    """State of a pending capability query."""
    request_id: str
    started_at: float
    responses: Dict[str, Dict] = field(default_factory=dict)


class ExecutorRegistry:
    """
    Registry of available executors and their capabilities.

    Manages query/response cycle for capability discovery.
    """

    def __init__(self, mqtt_client, query_timeout: float = 5.0, registry_ttl: float = 60.0):
        """
        Initialize executor registry.

        Args:
            mqtt_client: MQTT client for publishing queries
            query_timeout: How long to wait for responses (seconds)
            registry_ttl: How long registry is considered fresh (seconds)
        """
        self.mqtt_client = mqtt_client
        self.query_timeout = query_timeout
        self.registry_ttl = registry_ttl

        self._executors: Dict[str, ExecutorInfo] = {}
        self._last_query_time: Optional[float] = None
        self._pending_queries: Dict[str, QueryState] = {}
        self._lock = threading.Lock()

        logger.info(f"Executor registry initialized (timeout={query_timeout}s, ttl={registry_ttl}s)")

    def is_stale(self) -> bool:
        """Check if registry needs refresh."""
        if self._last_query_time is None:
            return True
        return time.time() - self._last_query_time > self.registry_ttl

    def query_capabilities(self) -> Dict[str, ExecutorInfo]:
        """
        Query all executors for capabilities.

        Publishes query to MQTT, waits for responses, updates registry.

        Returns:
            Dictionary of executor_id -> ExecutorInfo
        """
        request_id = str(uuid.uuid4())

        logger.info(f"Querying executors for capabilities (request_id={request_id})")

        # Create query state
        query_state = QueryState(
            request_id=request_id,
            started_at=time.time()
        )

        with self._lock:
            self._pending_queries[request_id] = query_state

        # Publish query
        query_msg = {
            "request_id": request_id,
        }
        self.mqtt_client.publish_message(
            "linearjc/query/capabilities",
            query_msg,
            qos=1
        )

        # Wait for responses
        deadline = time.time() + self.query_timeout
        while time.time() < deadline:
            time.sleep(0.1)

        # Collect responses
        with self._lock:
            responses = query_state.responses.copy()
            del self._pending_queries[request_id]

            # Update registry
            self._executors.clear()
            for executor_id, response in responses.items():
                self._executors[executor_id] = ExecutorInfo(
                    executor_id=executor_id,
                    hostname=response.get("hostname", "unknown"),
                    capabilities=response.get("capabilities", []),
                    supported_formats=response.get("supported_formats", []),
                    last_seen=time.time(),
                    last_query_id=request_id
                )

            self._last_query_time = time.time()

        logger.info(f"Capability query complete: {len(self._executors)} executors responded")
        return self._executors.copy()

    def handle_capability_response(self, executor_id: str, response: Dict) -> None:
        """
        Handle incoming capability response from executor.

        Args:
            executor_id: ID of responding executor
            response: Capability message payload
        """
        request_id = response.get("request_id")
        if not request_id:
            logger.warning(f"Capability response from {executor_id} missing request_id")
            return

        with self._lock:
            if request_id in self._pending_queries:
                self._pending_queries[request_id].responses[executor_id] = response
                logger.debug(f"Received capability response from {executor_id} (request_id={request_id})")
            else:
                logger.debug(f"Received capability response for unknown/expired query: {request_id}")

    def find_executor(self, job_id: str, job_version: str) -> Optional[str]:
        """
        Find an executor capable of running a job.

        Args:
            job_id: Job identifier
            job_version: Job version

        Returns:
            Executor ID if found, None otherwise
        """
        for executor_id, info in self._executors.items():
            for cap in info.capabilities:
                if cap.get("job_id") == job_id and cap.get("version") == job_version:
                    logger.debug(f"Found executor {executor_id} for {job_id} v{job_version}")
                    return executor_id

        logger.warning(f"No executor found for {job_id} v{job_version}")
        return None

    def get_all_executors(self) -> Dict[str, ExecutorInfo]:
        """Get all registered executors."""
        with self._lock:
            return self._executors.copy()

    def get_executor_count(self) -> int:
        """Get number of registered executors."""
        with self._lock:
            return len(self._executors)
