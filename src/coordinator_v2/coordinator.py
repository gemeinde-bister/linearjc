"""
Coordinator - Main async coordinator with aiomqtt.

Single-threaded async architecture that eliminates all threading issues.
Uses aiomqtt for event-driven MQTT communication.

Key design decisions:
- Single event loop handles all operations
- No locks needed (single-threaded)
- All handlers are async
- Heartbeat-based executor discovery (no blocking queries)
- Chain continuation is non-blocking
"""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import signal
import tarfile
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path

import aiomqtt
import yaml

from coordinator.archive_handler import create_archive, extract_archive
from coordinator.job_discovery import discover_jobs
from coordinator.logging_utils import (
    clear_correlation_ids,
    log_duration,
    log_with_fields,
    set_correlation_ids,
)
from coordinator.message_signing import sign_message, verify_message, MessageSigningError
from coordinator.minio_manager import MinioManager
from coordinator.models import CoordinatorConfig, DataRegistryEntry, Job, JobTree
from coordinator.security_utils import (
    SecurityError,
    validate_job_writes_against_registry,
    validate_minio_credentials,
    validate_path,
    validate_protected_registers_exist,
    validate_shared_secret,
)
from coordinator.tree_builder import build_trees

from coordinator_v2.event_router import EventRouter
from coordinator_v2.executor_registry import ExecutorRegistry
from coordinator_v2.job_state_machine import JobExecution, JobState, JobStateMachine
from coordinator_v2.job_tracker import JobTracker
from coordinator_v2.register_lock import RegisterLock, sorted_lock_paths
from coordinator_v2.registry_reload import validate_registry_reload
from coordinator_v2.tree_tracker import TreeExecution, TreeState, TreeTracker
from coordinator_v2.tree_validator import TreeValidator

logger = logging.getLogger(__name__)


def load_data_registry(
    path: str,
    allowed_data_roots: list[str] | None = None,
) -> dict[str, DataRegistryEntry]:
    """
    Load data registry from YAML file.

    Args:
        path: Path to registry YAML file
        allowed_data_roots: Optional list of allowed root directories for path validation

    Returns:
        Dict mapping registry keys to DataRegistryEntry objects

    Raises:
        SecurityError: If path validation is enabled and a path is outside allowed roots
    """
    with open(path) as f:
        data = yaml.safe_load(f)

    registry_data = data.get("registry", {})
    result = {}

    for key, value in registry_data.items():
        if isinstance(value, dict):
            entry = DataRegistryEntry(**value)
        else:
            # Compact formats are NOT recommended for Phase 15 register model.
            # Use full dict format for clarity:
            #   my_data: {type: fs, path: /data/data.json, kind: file}
            #   my_temp: {type: temp, kind: file}
            #   my_obj:  {type: minio, bucket: data, kind: dir}
            #
            # Legacy compact format support (deprecated):
            parts = value.split(":", 1)
            if len(parts) == 2:
                entry_type, location = parts
                if entry_type == "fs":
                    # Determine if file or dir (default to file if not specified)
                    entry = DataRegistryEntry(type="fs", path=location, kind="file")
                elif entry_type == "temp":
                    # temp:file or temp:dir (location is kind)
                    kind = location if location in ("file", "dir") else "file"
                    entry = DataRegistryEntry(type="temp", kind=kind)
                elif entry_type == "minio":
                    # Parse bucket/prefix - kind defaults to file
                    bucket_parts = location.split("/", 1)
                    bucket = bucket_parts[0]
                    prefix = bucket_parts[1] if len(bucket_parts) > 1 else None
                    entry = DataRegistryEntry(type="minio", bucket=bucket, prefix=prefix, kind="file")
                else:
                    logger.warning(f"Unknown registry entry type: {key}={value}")
                    continue
            else:
                logger.warning(f"Invalid registry entry format: {key}={value}")
                continue

        # Validate filesystem paths if allowed_data_roots is provided
        if allowed_data_roots and entry.type == "fs" and entry.path:
            validate_path(
                entry.path,
                allowed_roots=allowed_data_roots,
                allow_relative=False,
                description=f"registry '{key}' path",
            )

        result[key] = entry

    return result


def _compare_versions(v1: str, v2: str) -> int:
    """
    Compare semantic versions.

    Args:
        v1: First version string (e.g., "1.2.3")
        v2: Second version string (e.g., "1.2.0")

    Returns:
        1 if v1 > v2, 0 if equal, -1 if v1 < v2
    """
    def parse_version(v: str) -> list:
        try:
            return [int(x) for x in v.split(".")]
        except (ValueError, AttributeError):
            return [0, 0, 0]

    parts1 = parse_version(v1)
    parts2 = parse_version(v2)

    for i in range(3):
        p1 = parts1[i] if i < len(parts1) else 0
        p2 = parts2[i] if i < len(parts2) else 0
        if p1 > p2:
            return 1
        if p1 < p2:
            return -1
    return 0


class Coordinator:
    """
    Main coordinator orchestrator.

    Single-threaded async with aiomqtt for MQTT communication.
    """

    def __init__(self, config: CoordinatorConfig) -> None:
        """
        Initialize coordinator with configuration.

        Args:
            config: CoordinatorConfig instance
        """
        self.config = config
        self.client_id = "linearjc-coordinator"

        # Core components (initialized in _initialize)
        self.registry: ExecutorRegistry = ExecutorRegistry(ttl=90.0)
        self.job_tracker: JobTracker = JobTracker()
        self.tree_tracker: TreeTracker = TreeTracker()
        self.validator: TreeValidator | None = None
        self.register_lock: RegisterLock | None = None
        self.router: EventRouter = EventRouter()
        self.minio: MinioManager | None = None

        # State
        self.trees: list[JobTree] = []
        self.data_registry: dict[str, DataRegistryEntry] = {}
        self.tools_registry: dict[str, dict] = {}  # tool -> platform -> info

        # MQTT client (set during run)
        self.mqtt: aiomqtt.Client | None = None

        # Control flags
        self._reload_requested = False
        self._shutdown_requested = False

        # Developer API state
        self._pending_deployments: dict[str, dict] = {}  # request_id -> metadata

    async def run(self) -> None:
        """
        Main entry point with reconnection handling.

        Runs the main event loop until shutdown is requested.
        """
        await self._initialize()

        # Set up signal handlers
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGHUP, self._handle_reload_signal)
        loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown_signal)
        loop.add_signal_handler(signal.SIGINT, self._handle_shutdown_signal)

        while not self._shutdown_requested:
            try:
                async with aiomqtt.Client(
                    hostname=self.config.mqtt.broker,
                    port=self.config.mqtt.port,
                    identifier=self.client_id,
                    clean_session=False,
                ) as mqtt:
                    self.mqtt = mqtt
                    logger.info(
                        f"Connected to MQTT broker: "
                        f"{self.config.mqtt.broker}:{self.config.mqtt.port}"
                    )

                    await self._subscribe()
                    await self._announce_online()

                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._message_loop())
                        tg.create_task(self._scheduler_loop())
                        tg.create_task(self._maintenance_loop())

            except aiomqtt.MqttError as e:
                if self._shutdown_requested:
                    break
                logger.error(f"MQTT connection lost: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                if self._shutdown_requested:
                    break
                raise
            except ExceptionGroup as eg:
                # Handle TaskGroup exceptions
                if self._shutdown_requested:
                    break
                # Re-raise if not shutdown-related
                for exc in eg.exceptions:
                    if not isinstance(exc, asyncio.CancelledError):
                        raise

        logger.info("Coordinator shutdown complete")

    async def _initialize(self) -> None:
        """Load configuration, validate security, discover jobs, build trees."""
        logger.info("Initializing coordinator...")

        # Validate security configuration first
        self._validate_security()

        # Load data registry with path validation
        self.data_registry = load_data_registry(
            self.config.data_registry,
            allowed_data_roots=self.config.security.allowed_data_roots,
        )
        logger.info(f"Loaded {len(self.data_registry)} registry entries")

        # Validate protected registers exist (Phase 15 register model)
        validate_protected_registers_exist(self.data_registry)

        # Load tools registry (for ljc/executor self-update)
        self._load_tools_registry()

        # Initialize validator with registry
        self.validator = TreeValidator(self.data_registry)

        # Initialize register lock manager (Phase 15 ENQ/DEQ locking)
        self.register_lock = RegisterLock()

        # Initialize MinIO manager
        self.minio = MinioManager(self.config.minio)

        # Discover jobs and build trees
        jobs = discover_jobs(self.config.jobs_dir)

        # Validate jobs don't write to protected registers (Phase 15)
        for job in jobs:
            validate_job_writes_against_registry(
                job.id, job.writes, self.data_registry
            )

        self.trees = build_trees(jobs)
        logger.info(f"Built {len(self.trees)} job trees")

        # Register MQTT handlers
        self._register_handlers()

        # Cleanup orphaned data from previous runs
        await self._startup_cleanup()

        logger.info("Coordinator initialized")

    async def _startup_cleanup(self) -> None:
        """
        Clean up orphaned data from previous coordinator runs.

        Removes MinIO objects and work directories older than cleanup_age_hours.
        This prevents storage/memory runaway from crashed executions.
        """
        cleanup_age_hours = getattr(self.config, "cleanup_age_hours", 24)

        logger.info(f"Startup cleanup: removing orphaned data older than {cleanup_age_hours}h")

        try:
            # Clean orphaned MinIO objects
            minio_deleted = self._cleanup_orphaned_minio(cleanup_age_hours)
            if minio_deleted > 0:
                logger.info(f"Startup cleanup: removed {minio_deleted} orphaned MinIO objects")

            # Clean orphaned work directories
            work_deleted = self._cleanup_orphaned_work_dirs(cleanup_age_hours)
            if work_deleted > 0:
                logger.info(f"Startup cleanup: removed {work_deleted} orphaned work directories")

        except Exception as e:
            # Log but don't fail startup - cleanup is best-effort
            logger.warning(f"Startup cleanup failed: {e}")

    def _cleanup_orphaned_minio(self, age_hours: int) -> int:
        """Clean MinIO objects older than age_hours."""
        if not self.minio:
            return 0

        try:
            return self.minio.cleanup_orphaned_executions(
                bucket=self.config.minio.temp_bucket,
                age_hours=age_hours,
                dry_run=False,
            )
        except Exception as e:
            logger.warning(f"MinIO cleanup failed: {e}")
            return 0

    def _cleanup_orphaned_work_dirs(self, age_hours: int) -> int:
        """Clean work directories older than age_hours."""
        work_dir = Path(self.config.work_dir) if hasattr(self.config, "work_dir") else None
        if not work_dir or not work_dir.exists():
            return 0

        cutoff = time.time() - (age_hours * 3600)
        deleted = 0

        try:
            # Tree execution IDs follow pattern: {job_id}-{timestamp}-{uuid}
            for exec_dir in work_dir.glob("*-*-*"):
                if exec_dir.is_dir():
                    try:
                        if exec_dir.stat().st_mtime < cutoff:
                            shutil.rmtree(exec_dir)
                            deleted += 1
                            logger.debug(f"Removed orphaned work dir: {exec_dir.name}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {exec_dir}: {e}")
        except Exception as e:
            logger.warning(f"Work dir cleanup failed: {e}")

        return deleted

    def _validate_security(self) -> None:
        """
        Validate security configuration at startup.

        Checks:
        - Shared secret meets minimum requirements
        - MinIO credentials are not default/weak
        - Allowed data roots exist and are directories

        Raises:
            SecurityError: If any validation fails
        """
        if not self.config.security.validate_secrets:
            logger.warning(
                "Secret validation is DISABLED - this is insecure for production!"
            )
            return

        logger.info("Validating security configuration...")

        try:
            # Validate shared secret
            validate_shared_secret(self.config.signing.shared_secret)
            logger.info("✓ Shared secret validation passed")

            # Validate MinIO credentials
            validate_minio_credentials(
                self.config.minio.access_key,
                self.config.minio.secret_key,
            )
            logger.info("✓ MinIO credentials validation passed")

            # Validate allowed data roots exist
            for root in self.config.security.allowed_data_roots:
                root_path = Path(root)
                if not root_path.exists():
                    logger.warning(f"Allowed data root does not exist: {root}")
                elif not root_path.is_dir():
                    raise SecurityError(
                        f"Allowed data root is not a directory: {root}"
                    )

            logger.info("✓ Security configuration validated")

        except SecurityError as e:
            logger.error("=" * 60)
            logger.error("SECURITY VALIDATION FAILED")
            logger.error("=" * 60)
            logger.error(str(e))
            logger.error("")
            logger.error("To disable validation (NOT RECOMMENDED):")
            logger.error("  Add to config.yaml:")
            logger.error("    security:")
            logger.error("      validate_secrets: false")
            logger.error("=" * 60)
            raise

    def _load_tools_registry(self) -> None:
        """
        Load tools registry from YAML file.

        The tools registry maps tool names and platforms to version info:
            tools:
              ljc:
                x86_64-unknown-linux-musl:
                  version: "0.8.0"
                  checksum_sha256: "..."
                  size_bytes: 4521984
                  path: "linearjc-tools/ljc-0.8.0-x86_64-linux-musl"
        """
        if not self.config.tools_registry:
            logger.info("No tools registry configured (self-update disabled)")
            return

        registry_path = Path(self.config.tools_registry)
        if not registry_path.exists():
            logger.warning(f"Tools registry not found: {registry_path}")
            return

        try:
            with open(registry_path) as f:
                data = yaml.safe_load(f)

            self.tools_registry = data.get("tools", {})
            tool_count = sum(len(platforms) for platforms in self.tools_registry.values())
            logger.info(f"Loaded tools registry: {len(self.tools_registry)} tools, {tool_count} platform entries")
        except Exception as e:
            logger.error(f"Failed to load tools registry: {e}")
            self.tools_registry = {}

    def _register_handlers(self) -> None:
        """Register MQTT message handlers with EventRouter."""
        self.router.register("linearjc/heartbeat/+", self._on_heartbeat)
        self.router.register("linearjc/jobs/progress/+", self._on_progress)

        # Developer API handlers
        coord_id = self.client_id
        self.router.register(f"linearjc/deploy/request/{coord_id}", self._on_deploy_request)
        self.router.register(f"linearjc/deploy/complete/{coord_id}", self._on_deploy_complete)
        self.router.register(f"linearjc/registry/request/{coord_id}", self._on_registry_request)
        self.router.register(f"linearjc/dev/exec/request/{coord_id}", self._on_exec_request)
        self.router.register(f"linearjc/dev/tail/request/{coord_id}", self._on_tail_request)
        self.router.register(f"linearjc/dev/status/request/{coord_id}", self._on_status_request)
        self.router.register(f"linearjc/dev/ps/request/{coord_id}", self._on_ps_request)
        self.router.register(f"linearjc/dev/logs/request/{coord_id}", self._on_logs_request)
        self.router.register(f"linearjc/dev/kill/request/{coord_id}", self._on_kill_request)

        # Tools self-update handler
        self.router.register(f"linearjc/tools/version/request/{coord_id}", self._on_tools_version_request)

        logger.debug(f"Registered {len(self.router)} message handlers")

    async def _subscribe(self) -> None:
        """Subscribe to all required MQTT topics."""
        assert self.mqtt is not None

        topics = [
            "linearjc/heartbeat/+",
            "linearjc/jobs/progress/+",
            f"linearjc/deploy/request/{self.client_id}",
            f"linearjc/deploy/complete/{self.client_id}",
            f"linearjc/registry/request/{self.client_id}",
            f"linearjc/dev/exec/request/{self.client_id}",
            f"linearjc/dev/tail/request/{self.client_id}",
            f"linearjc/dev/status/request/{self.client_id}",
            f"linearjc/dev/ps/request/{self.client_id}",
            f"linearjc/dev/logs/request/{self.client_id}",
            f"linearjc/dev/kill/request/{self.client_id}",
            f"linearjc/tools/version/request/{self.client_id}",
        ]

        for topic in topics:
            await self.mqtt.subscribe(topic)
            logger.debug(f"Subscribed to: {topic}")

    async def _announce_online(self) -> None:
        """
        Announce coordinator is online.

        Triggers executors to send immediate heartbeats.
        """
        await self._publish(
            "linearjc/coordinator/online",
            {
                "coordinator_id": self.client_id,
                "timestamp": time.time(),
            },
        )
        logger.info("Published coordinator/online announcement")

    # ─────────────────────────────────────────────────────────────────────
    # Main Loops
    # ─────────────────────────────────────────────────────────────────────

    async def _message_loop(self) -> None:
        """Process incoming MQTT messages."""
        assert self.mqtt is not None

        async for message in self.mqtt.messages:
            if self._shutdown_requested:
                break

            try:
                topic = str(message.topic)
                payload_str = message.payload.decode("utf-8")

                logger.debug(f"Received message on {topic}: {len(payload_str)} bytes")

                # Parse JSON envelope
                envelope = json.loads(payload_str)

                # Verify signature
                try:
                    inner_payload = verify_message(
                        envelope,
                        self.config.signing.shared_secret,
                        max_age_seconds=60,
                    )
                except MessageSigningError as e:
                    logger.warning(f"Invalid signature on {topic}: {e}")
                    continue

                # Route to handler
                await self.router.route(topic, inner_payload)

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message on {topic}: {e}")
            except Exception as e:
                logger.exception(f"Error processing message: {e}")

    async def _scheduler_loop(self) -> None:
        """Main scheduling loop."""
        interval = self.config.scheduling.loop_interval

        while not self._shutdown_requested:
            try:
                # Check for hot reload request
                if self._reload_requested:
                    self._reload_requested = False
                    await self._hot_reload()

                # Get trees that are ready to execute
                ready_trees = self._get_ready_trees()

                for tree in ready_trees:
                    await self._execute_tree(tree)

            except Exception as e:
                logger.exception(f"Scheduler error: {e}")

            await asyncio.sleep(interval)

    async def _maintenance_loop(self) -> None:
        """Periodic maintenance tasks."""
        # Orphan cleanup runs every hour (120 cycles * 30s = 1 hour)
        orphan_cleanup_interval = 120
        orphan_cleanup_counter = 0

        while not self._shutdown_requested:
            try:
                # Prune stale executors and handle dead executor cleanup
                # Phase 15: executor crash detection after 3 missed heartbeats (90s)
                dead_executors = self.registry.prune_stale()
                if dead_executors:
                    await self._handle_dead_executors(dead_executors)

                # Prune completed jobs and trees
                self.job_tracker.prune_completed()
                self.tree_tracker.prune_completed()

                # Check for timed out jobs
                timed_out = self.job_tracker.check_timeouts()
                for sm in timed_out:
                    tree_exec_id = sm.job_exec.tree_execution_id
                    self.tree_tracker.mark_failed(tree_exec_id, "Job timeout")
                    self.register_lock.dequeue(tree_exec_id)
                    self.validator.unregister(tree_exec_id)
                    self.tree_tracker.remove(tree_exec_id)
                    # Also cleanup the failed execution's data
                    asyncio.create_task(self._cleanup_execution(tree_exec_id))

                # Periodic orphan cleanup (once per hour)
                orphan_cleanup_counter += 1
                if orphan_cleanup_counter >= orphan_cleanup_interval:
                    orphan_cleanup_counter = 0
                    cleanup_age_hours = getattr(self.config, "cleanup_age_hours", 24)
                    minio_deleted = self._cleanup_orphaned_minio(cleanup_age_hours)
                    work_deleted = self._cleanup_orphaned_work_dirs(cleanup_age_hours)
                    if minio_deleted > 0 or work_deleted > 0:
                        logger.info(
                            f"Periodic cleanup: {minio_deleted} MinIO objects, "
                            f"{work_deleted} work dirs"
                        )

            except Exception as e:
                logger.exception(f"Maintenance error: {e}")

            await asyncio.sleep(30)

    # ─────────────────────────────────────────────────────────────────────
    # Tree Scheduling
    # ─────────────────────────────────────────────────────────────────────

    def _get_ready_trees(self) -> list[JobTree]:
        """
        Get trees that are ready to execute.

        Uses min/max daily constraints and execution history.

        Returns:
            List of JobTree objects ready for execution
        """
        now = time.time()
        ready = []

        for tree in self.trees:
            # Skip if already executing
            if self._is_tree_active(tree):
                continue

            # Check scheduling constraints
            count_24h = tree.get_executions_last_24h(now)

            # Never exceed max_daily
            if count_24h >= tree.max_daily:
                continue

            # Always try to meet min_daily
            if count_24h < tree.min_daily:
                ready.append(tree)
                continue

            # Between min and max: check if it's time based on interval
            if tree.last_execution is None:
                ready.append(tree)
                continue

            # Calculate ideal interval
            ideal_interval = 86400 / tree.max_daily  # seconds
            time_since_last = now - tree.last_execution

            if time_since_last >= ideal_interval:
                ready.append(tree)

        return ready

    def _is_tree_active(self, tree: JobTree) -> bool:
        """Check if tree is currently executing."""
        for tex in self.tree_tracker.get_active():
            if tex.tree.root.id == tree.root.id:
                return True
        return False

    # ─────────────────────────────────────────────────────────────────────
    # Tree Execution
    # ─────────────────────────────────────────────────────────────────────

    async def _execute_tree(
        self,
        tree: JobTree,
        dev_client_id: str | None = None,
    ) -> str | None:
        """
        Execute a tree starting with root job.

        For multi-job chains: subsequent jobs are triggered by _on_progress
        when previous job completes.

        Args:
            tree: JobTree to execute
            dev_client_id: Client ID for progress forwarding (if from ljc exec)

        Returns:
            tree_execution_id if dispatched, None if blocked
        """
        tree_exec_id = self._generate_tree_exec_id(tree)
        root_job = tree.root

        # Set correlation IDs for this execution
        set_correlation_ids(tree_exec_id=tree_exec_id)

        # Check for output conflicts (TreeValidator - defense in depth)
        can_exec, error = self.validator.can_execute(tree, tree_exec_id)
        if not can_exec:
            log_with_fields(
                logger, logging.INFO, f"Tree {root_job.id} blocked",
                reason=error, job_id=root_job.id,
            )
            clear_correlation_ids()
            return None

        # Find executor (non-blocking dict lookup!)
        executor_id = self.registry.find_executor(root_job.id, root_job.version)
        if not executor_id:
            # Airdrop and retry on next scheduler cycle
            await self._distribute_job(root_job)
            log_with_fields(
                logger, logging.INFO, f"Airdropped {root_job.id}",
                job_id=root_job.id, action="airdrop",
            )
            clear_correlation_ids()
            return None

        # Update correlation with executor
        set_correlation_ids(executor_id=executor_id)

        # Acquire register locks (Phase 15 ENQ/DEQ semantics)
        # Extract all locks needed by this tree (sorted for deadlock prevention)
        try:
            locks = sorted_lock_paths(tree, self.data_registry)
        except ValueError as e:
            # Protected register violation or other validation error
            log_with_fields(
                logger, logging.WARNING, f"Tree {root_job.id} lock validation failed",
                reason=str(e), job_id=root_job.id,
            )
            clear_correlation_ids()
            return None

        if not self.register_lock.try_acquire_all(locks, tree_exec_id):
            # Tree is waiting for locks - will be retried by scheduler
            log_with_fields(
                logger, logging.INFO, f"Tree {root_job.id} waiting for register locks",
                job_id=root_job.id, lock_count=len(locks),
            )
            clear_correlation_ids()
            return None

        # Register output locks and start tracking (TreeValidator - defense in depth)
        self.validator.register(tree, tree_exec_id)
        tex = self.tree_tracker.create(tree_exec_id, tree, dev_client_id)

        # Create job execution for root (index 0)
        job_exec = self._create_job_execution(
            tree_exec_id=tree_exec_id,
            job=root_job,
            chain_index=0,
            chain_length=len(tree.jobs),
            timeout=root_job.run.timeout,
            dev_client_id=dev_client_id,
        )
        sm = self.job_tracker.create(job_exec)
        tex.job_exec_ids.append(job_exec.job_execution_id)

        # Update correlation with job execution ID
        set_correlation_ids(job_exec_id=job_exec.job_execution_id)

        # Prepare inputs and outputs
        inputs = await self._prepare_root_inputs(tree, tree_exec_id)
        outputs = self._prepare_outputs(root_job, tree_exec_id)

        # Build and dispatch request
        request = self._build_job_request(job_exec, root_job, inputs, outputs, executor_id)
        sm.transition("dispatch", {"executor_id": executor_id})
        await self._publish_job_request(job_exec.job_execution_id, request)

        # Record execution
        tree.record_execution(time.time())
        tree.last_execution = time.time()

        logger.info(
            f"Dispatched tree {root_job.id} (1/{len(tree.jobs)} jobs) to {executor_id}"
        )
        return tree_exec_id

    def _create_job_execution(
        self,
        tree_exec_id: str,
        job: Job,
        chain_index: int,
        chain_length: int,
        timeout: int,
        dev_client_id: str | None = None,
    ) -> JobExecution:
        """Create a JobExecution instance."""
        job_exec_id = f"{tree_exec_id}-{job.id}"
        return JobExecution(
            job_execution_id=job_exec_id,
            tree_execution_id=tree_exec_id,
            job_id=job.id,
            version=job.version,
            state=JobState.QUEUED,
            chain_index=chain_index,
            chain_length=chain_length,
            timeout_at=time.time() + timeout,
            dev_client_id=dev_client_id,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Message Handlers
    # ─────────────────────────────────────────────────────────────────────

    async def _on_heartbeat(self, topic: str, payload: dict) -> None:
        """Handle executor heartbeat."""
        executor_id = topic.split("/")[-1]
        self.registry.on_heartbeat(executor_id, payload)

        # Check if executor needs update
        await self._check_executor_update(executor_id, payload)

        # Check if any waiting chains can now continue
        for tex in self.tree_tracker.get_waiting_trees():
            if tex.waiting_job_id:
                current_job = self.tree_tracker.get_current_job(tex.tree_execution_id)
                if current_job:
                    exec_id = self.registry.find_executor(
                        current_job.id, current_job.version
                    )
                    if exec_id:
                        logger.info(
                            f"Resuming chain {tex.tree_execution_id} - "
                            f"executor has job {current_job.id}"
                        )
                        await self._resume_chain(tex)

    async def _on_progress(self, topic: str, payload: dict) -> None:
        """
        Handle job progress update.

        For multi-job chains:
        - On job completion (not last): continue chain with next job
        - On last job completion: collect outputs
        - On failure: abort chain and release locks
        """
        job_exec_id = payload.get("job_execution_id")
        state = payload.get("state")

        sm = self.job_tracker.get(job_exec_id)
        if not sm:
            logger.debug(f"Ignoring progress for unknown job: {job_exec_id}")
            return

        tree_exec_id = sm.job_exec.tree_execution_id
        tex = self.tree_tracker.get(tree_exec_id)

        # Set correlation IDs for this context
        set_correlation_ids(
            tree_exec_id=tree_exec_id,
            job_exec_id=job_exec_id,
            executor_id=sm.job_exec.executor_id,
        )

        log_with_fields(
            logger, logging.DEBUG, f"Progress update: {state}",
            state=state, job_id=sm.job_exec.job_id,
        )

        # Forward progress to dev client if attached
        if sm.job_exec.dev_client_id:
            await self._forward_progress(sm.job_exec.dev_client_id, payload)

        # Map executor state to event
        event_map = {
            "assigned": "assigned",
            "downloading": "downloading",
            "ready": "ready",
            "running": "running",
            "uploading": "uploading",
            "completed": "completed",
            "failed": "failed",
        }

        event = event_map.get(state)
        if event:
            sm.transition(event, payload)

        # Handle state-based actions
        if sm.job_exec.state == JobState.COLLECTING:
            # Job completed - Phase 15 write-through: collect outputs for EVERY job
            try:
                # Get the current job from the tree
                current_job = self._find_job_by_id(sm.job_exec.job_id)
                if current_job:
                    # Write-through collection: collect fs and minio outputs, skip temp
                    await self._collect_job_outputs(current_job, tree_exec_id)

                # Check if chain continues or finalizes
                if tex and not self.tree_tracker.is_last_job(tree_exec_id):
                    await self._continue_chain(tex, sm)
                else:
                    await self._collect_outputs(sm, tex)

            except Exception as e:
                # Write-through collection failed - treat as chain failure
                logger.exception(f"Write-through collection failed: {e}")
                await self._handle_chain_failure(sm, tex, str(e))

        elif sm.job_exec.state in (JobState.FAILED, JobState.TIMEOUT):
            error = payload.get("error", "Unknown error")
            await self._handle_chain_failure(sm, tex, error)

    # ─────────────────────────────────────────────────────────────────────
    # Chain Continuation
    # ─────────────────────────────────────────────────────────────────────

    async def _continue_chain(
        self,
        tex: TreeExecution,
        completed_sm: JobStateMachine,
    ) -> None:
        """
        Continue chain execution after job completion.

        Non-blocking: uses heartbeat-based registry lookup.
        """
        tree = tex.tree
        tree_exec_id = tex.tree_execution_id

        # Advance to next job
        next_index = self.tree_tracker.advance(tree_exec_id)
        if next_index is None:
            logger.error(f"advance() returned None but is_last_job was False")
            return

        next_job = tree.jobs[next_index]
        prev_job = tree.jobs[next_index - 1]

        logger.info(
            f"Chain continuation: {prev_job.id} -> {next_job.id} "
            f"(job {next_index + 1}/{len(tree.jobs)})"
        )

        # Find executor (NON-BLOCKING - just dict lookup!)
        executor_id = self.registry.find_executor(next_job.id, next_job.version)

        if not executor_id:
            # Airdrop and mark as waiting
            await self._distribute_job(next_job)
            self.tree_tracker.mark_waiting(tree_exec_id, next_job.id)
            logger.info(f"Chain waiting for executor to install {next_job.id}")
            return

        # Prepare inputs and outputs
        inputs = await self._prepare_chain_inputs(next_job, prev_job, tree_exec_id)
        outputs = self._prepare_outputs(next_job, tree_exec_id)

        # Create job execution
        job_exec = self._create_job_execution(
            tree_exec_id=tree_exec_id,
            job=next_job,
            chain_index=next_index,
            chain_length=len(tree.jobs),
            timeout=next_job.run.timeout,
            dev_client_id=tex.dev_client_id,
        )
        sm = self.job_tracker.create(job_exec)
        tex.job_exec_ids.append(job_exec.job_execution_id)

        # Build and dispatch
        request = self._build_job_request(job_exec, next_job, inputs, outputs, executor_id)
        sm.transition("dispatch", {"executor_id": executor_id})
        await self._publish_job_request(job_exec.job_execution_id, request)

        logger.info(
            f"Dispatched chain job {next_job.id} "
            f"({next_index + 1}/{len(tree.jobs)}) to {executor_id}"
        )

    async def _resume_chain(self, tex: TreeExecution) -> None:
        """Resume a waiting chain after executor installs job."""
        tree_exec_id = tex.tree_execution_id
        self.tree_tracker.clear_waiting(tree_exec_id)

        current_job = self.tree_tracker.get_current_job(tree_exec_id)
        if not current_job:
            logger.error(f"Cannot resume chain {tree_exec_id}: no current job")
            return

        # Find executor (should succeed now)
        executor_id = self.registry.find_executor(current_job.id, current_job.version)
        if not executor_id:
            # Still no executor - mark waiting again
            self.tree_tracker.mark_waiting(tree_exec_id, current_job.id)
            logger.warning(f"Chain {tree_exec_id} still waiting for executor")
            return

        # Get previous job for input preparation
        prev_job = self.tree_tracker.get_previous_job(tree_exec_id)

        # Prepare inputs and outputs
        if prev_job:
            inputs = await self._prepare_chain_inputs(current_job, prev_job, tree_exec_id)
        else:
            # Root job case (shouldn't happen in resume, but handle defensively)
            tree = tex.tree
            inputs = await self._prepare_root_inputs(tree, tree_exec_id)

        outputs = self._prepare_outputs(current_job, tree_exec_id)

        # Create job execution
        job_exec = self._create_job_execution(
            tree_exec_id=tree_exec_id,
            job=current_job,
            chain_index=tex.current_index,
            chain_length=len(tex.tree.jobs),
            timeout=current_job.run.timeout,
            dev_client_id=tex.dev_client_id,
        )
        sm = self.job_tracker.create(job_exec)
        tex.job_exec_ids.append(job_exec.job_execution_id)

        # Build and dispatch
        request = self._build_job_request(job_exec, current_job, inputs, outputs, executor_id)
        sm.transition("dispatch", {"executor_id": executor_id})
        await self._publish_job_request(job_exec.job_execution_id, request)

        logger.info(f"Resumed chain {tree_exec_id} with job {current_job.id}")

    # ─────────────────────────────────────────────────────────────────────
    # Input/Output Preparation
    # ─────────────────────────────────────────────────────────────────────

    async def _prepare_root_inputs(
        self,
        tree: JobTree,
        tree_exec_id: str,
    ) -> dict[str, dict]:
        """
        Prepare inputs for root job from data registry.

        Args:
            tree: JobTree being executed
            tree_exec_id: Unique execution ID

        Returns:
            Dict of input_name -> input_spec
        """
        root_job = tree.root
        prepared = {}

        for registry_key in root_job.reads:
            entry = self.data_registry.get(registry_key)
            if not entry:
                logger.warning(f"Registry key not found: {registry_key}")
                continue

            if entry.type == "fs":
                # Upload filesystem data to MinIO, return presigned URL
                prepared[registry_key] = await self._prepare_fs_input(
                    registry_key, entry, tree_exec_id, root_job.run.timeout
                )
            elif entry.type == "minio":
                # Generate presigned URL for existing MinIO object
                prepared[registry_key] = self._prepare_minio_input(
                    registry_key, entry, root_job.run.timeout
                )

        return prepared

    async def _prepare_chain_inputs(
        self,
        job: Job,
        prev_job: Job,
        tree_exec_id: str,
    ) -> dict[str, dict]:
        """
        Prepare inputs for non-root job in chain.

        Phase 15 cache-aware logic:
        - Data from previous job: try MinIO cache first, fallback to storage
        - temp type: always MinIO (no persistent storage)
        - fs/minio type: cache hit -> MinIO, cache miss -> read from storage
        - External inputs: read from their source
        """
        prepared = {}

        for registry_key in job.reads:
            entry = self.data_registry.get(registry_key)
            if not entry:
                logger.warning(f"Registry key not found: {registry_key}")
                continue

            if registry_key in prev_job.writes:
                # Data from previous job in chain
                if entry.type == "temp":
                    # Temp: always in MinIO (no persistent storage, no fallback)
                    prepared[registry_key] = self._prepare_intermediate_input(
                        registry_key, entry, tree_exec_id, job.run.timeout
                    )
                else:
                    # fs/minio: try cache first, fallback to storage
                    if self._minio_temp_exists(tree_exec_id, registry_key):
                        # Cache hit - read from MinIO temp
                        prepared[registry_key] = self._prepare_intermediate_input(
                            registry_key, entry, tree_exec_id, job.run.timeout
                        )
                    else:
                        # Cache miss - read from storage (write-through destination)
                        logger.info(
                            f"Cache miss for '{registry_key}', reading from storage"
                        )
                        if entry.type == "fs":
                            prepared[registry_key] = await self._prepare_fs_input(
                                registry_key, entry, tree_exec_id, job.run.timeout
                            )
                        elif entry.type == "minio":
                            prepared[registry_key] = self._prepare_minio_input(
                                registry_key, entry, job.run.timeout
                            )

            elif entry.type == "fs":
                # External filesystem input
                prepared[registry_key] = await self._prepare_fs_input(
                    registry_key, entry, tree_exec_id, job.run.timeout
                )
            elif entry.type == "minio":
                # External MinIO input
                prepared[registry_key] = self._prepare_minio_input(
                    registry_key, entry, job.run.timeout
                )
            elif entry.type == "temp":
                # External temp read - should not happen (temp is chain-only)
                logger.warning(
                    f"Job reads temp register '{registry_key}' not from chain - "
                    f"temp registers are only valid within a chain"
                )

        return prepared

    async def _prepare_fs_input(
        self,
        registry_key: str,
        entry: DataRegistryEntry,
        tree_exec_id: str,
        timeout: int,
    ) -> dict:
        """
        Prepare filesystem input by archiving and uploading to MinIO.

        Args:
            registry_key: Registry key name
            entry: DataRegistryEntry with filesystem path
            tree_exec_id: Unique execution ID
            timeout: Job timeout for URL expiration

        Returns:
            Input specification with presigned GET URL
        """
        assert self.minio is not None
        assert entry.path is not None

        source_path = entry.path
        object_name = f"jobs/{tree_exec_id}/input_{registry_key}.tar.gz"

        # Create temporary directory for archive
        work_dir = Path(self.config.work_dir) if hasattr(self.config, "work_dir") else Path("/var/lib/linearjc/work")
        work_dir.mkdir(parents=True, exist_ok=True)

        exec_dir = work_dir / tree_exec_id
        exec_dir.mkdir(parents=True, exist_ok=True)

        archive_path = exec_dir / f"input_{registry_key}.tar.gz"

        # Create archive from source path
        logger.debug(f"Creating archive: {source_path} -> {archive_path}")
        create_archive(source_path, str(archive_path))

        # Upload to MinIO
        logger.debug(f"Uploading to MinIO: {archive_path} -> {object_name}")
        self.minio.upload_file(
            str(archive_path),
            self.config.minio.temp_bucket,
            object_name,
        )

        # Generate presigned GET URL
        url = self.minio.generate_presigned_get_url(
            bucket=self.config.minio.temp_bucket,
            object_name=object_name,
            expires_seconds=timeout + 3600,
        )

        logger.info(f"Prepared input '{registry_key}' from {source_path}")

        return {
            "url": url,
            "method": "GET",
            "format": "tar.gz",
            "kind": entry.kind or "file",
        }

    def _prepare_minio_input(
        self,
        registry_key: str,
        entry: DataRegistryEntry,
        timeout: int,
    ) -> dict:
        """Prepare MinIO input with presigned GET URL."""
        assert self.minio is not None
        assert entry.bucket is not None

        object_name = entry.prefix or registry_key
        url = self.minio.generate_presigned_get_url(
            bucket=entry.bucket,
            object_name=object_name,
            expires_seconds=timeout + 3600,
        )

        return {
            "url": url,
            "method": "GET",
            "format": "tar.gz",
            "kind": "file",
        }

    def _prepare_intermediate_input(
        self,
        registry_key: str,
        entry: DataRegistryEntry,
        tree_exec_id: str,
        timeout: int,
    ) -> dict:
        """
        Prepare input from previous job's output (intermediate data).

        Previous job uploaded to: jobs/{tree_exec_id}/output_{registry_key}.tar.gz
        """
        assert self.minio is not None

        object_name = f"jobs/{tree_exec_id}/output_{registry_key}.tar.gz"
        url = self.minio.generate_presigned_get_url(
            bucket=self.config.minio.temp_bucket,
            object_name=object_name,
            expires_seconds=timeout + 3600,
        )

        return {
            "url": url,
            "method": "GET",
            "format": "tar.gz",
            "kind": entry.kind or "file",
        }

    def _minio_temp_exists(self, tree_exec_id: str, registry_key: str) -> bool:
        """
        Check if intermediate output exists in MinIO temp bucket.

        Phase 15 cache-aware: Used to check if MinIO cache is available
        before falling back to storage for chain inputs.

        Args:
            tree_exec_id: Tree execution ID
            registry_key: Registry key

        Returns:
            True if object exists in MinIO temp bucket
        """
        if not self.minio:
            return False

        object_name = f"jobs/{tree_exec_id}/output_{registry_key}.tar.gz"
        return self.minio.object_exists(self.config.minio.temp_bucket, object_name)

    def _prepare_outputs(self, job: Job, tree_exec_id: str) -> dict[str, dict]:
        """Prepare output URLs (presigned PUT to MinIO)."""
        assert self.minio is not None

        prepared = {}

        for registry_key in job.writes:
            entry = self.data_registry.get(registry_key)
            if not entry:
                logger.warning(f"Registry key not found: {registry_key}")
                continue

            object_name = f"jobs/{tree_exec_id}/output_{registry_key}.tar.gz"
            url = self.minio.generate_presigned_put_url(
                bucket=self.config.minio.temp_bucket,
                object_name=object_name,
                expires_seconds=job.run.timeout + 3600,
            )

            prepared[registry_key] = {
                "url": url,
                "method": "PUT",
                "format": "tar.gz",
                "kind": entry.kind or "file",
            }

        return prepared

    # ─────────────────────────────────────────────────────────────────────
    # Output Collection & Cleanup
    # ─────────────────────────────────────────────────────────────────────

    async def _collect_job_outputs(
        self,
        job: Job,
        tree_exec_id: str,
    ) -> None:
        """
        Write-through collection for a single job's outputs.

        Phase 15 Register Model: Collect permanent outputs (fs, minio) after
        EACH job completion, not just the leaf. Temp outputs stay in MinIO
        for chain consumption.

        Args:
            job: The completed job
            tree_exec_id: Tree execution ID

        Raises:
            Exception: If collection fails (caller handles failure)
        """
        collected_count = 0

        for registry_key in job.writes:
            entry = self.data_registry.get(registry_key)
            if not entry:
                logger.warning(f"Registry key not found for output: {registry_key}")
                continue

            if entry.type == "fs":
                # Write-through to filesystem (atomic)
                await self._collect_filesystem_output(registry_key, entry, tree_exec_id)
                collected_count += 1
                # Note: Keep MinIO data as cache until tree ends

            elif entry.type == "minio":
                # Copy from temp bucket to permanent bucket
                await self._collect_minio_output(registry_key, entry, tree_exec_id)
                collected_count += 1
                # Note: Keep temp bucket data as cache until tree ends

            elif entry.type == "temp":
                # Temp stays in MinIO only - no write-through
                logger.debug(f"Skipping temp output '{registry_key}' (stays in MinIO)")

        if collected_count > 0:
            logger.info(
                f"Write-through collected {collected_count} outputs from {job.id}"
            )

    async def _collect_minio_output(
        self,
        registry_key: str,
        entry: DataRegistryEntry,
        tree_exec_id: str,
    ) -> None:
        """
        Copy output from temp bucket to permanent MinIO bucket.

        Phase 15: Uses MinIO server-side copy for efficiency (no re-download/re-upload).

        Args:
            registry_key: Registry key
            entry: Registry entry with bucket and prefix
            tree_exec_id: Tree execution ID
        """
        assert self.minio is not None
        assert entry.bucket is not None

        source_object = f"jobs/{tree_exec_id}/output_{registry_key}.tar.gz"
        dest_object = f"{entry.prefix or ''}{registry_key}.tar.gz"

        # Use MinIO copy object for efficiency (server-side, no download/upload)
        self.minio.copy_object(
            source_bucket=self.config.minio.temp_bucket,
            source_object=source_object,
            dest_bucket=entry.bucket,
            dest_object=dest_object,
        )

        logger.info(f"Collected output '{registry_key}' to minio:{entry.bucket}/{dest_object}")

    async def _collect_outputs(
        self,
        sm: JobStateMachine,
        tex: TreeExecution | None,
    ) -> None:
        """
        Finalize tree execution after last job completion.

        Phase 15: Per-job write-through collection is done in _collect_job_outputs.
        This method now only handles finalization:
        - Transition job state to collected
        - Release register locks (DEQ)
        - Mark tree completed
        - Schedule MinIO temp cleanup

        Args:
            sm: Job state machine for the last job
            tex: Tree execution context
        """
        tree_exec_id = sm.job_exec.tree_execution_id
        tree = tex.tree if tex else self._find_tree_by_job(sm.job_exec.job_id)

        if not tree:
            logger.error(f"Tree not found for job {sm.job_exec.job_id}")
            # Still release locks to avoid deadlock
            self.register_lock.dequeue(tree_exec_id)
            self.validator.unregister(tree_exec_id)
            return

        logger.info(f"Finalizing tree {tree.root.id}")

        # Transition job to collected state
        sm.transition("collected")

        # Release all register locks (Phase 15 ENQ/DEQ)
        self.register_lock.dequeue(tree_exec_id)
        self.validator.unregister(tree_exec_id)

        # Update tree tracker
        if tex:
            self.tree_tracker.mark_completed(tree_exec_id)
            self.tree_tracker.remove(tree_exec_id)

        logger.info(f"Completed tree {tree.root.id}")

        # Cleanup MinIO temp data (non-blocking background task)
        # This deletes all objects under jobs/{tree_exec_id}/
        asyncio.create_task(self._cleanup_execution(tree_exec_id))

    async def _collect_filesystem_output(
        self,
        registry_key: str,
        entry: DataRegistryEntry,
        tree_exec_id: str,
    ) -> None:
        """
        Collect output to filesystem: download from MinIO and extract atomically.

        Phase 15 write-through: Uses atomic collection (temp + os.replace)
        to ensure partial failures don't corrupt the destination.

        Args:
            registry_key: Registry key (used as output name)
            entry: Registry entry with filesystem path and kind
            tree_exec_id: Execution ID

        Raises:
            Exception: If download or extraction fails (temp cleaned up)
        """
        assert entry.path is not None
        assert self.minio is not None

        dest_path = Path(entry.path)
        kind = entry.kind or "dir"

        # Setup paths
        work_dir = Path(self.config.work_dir) if hasattr(self.config, "work_dir") else Path("/var/lib/linearjc/work")
        exec_dir = work_dir / tree_exec_id
        exec_dir.mkdir(parents=True, exist_ok=True)

        archive_filename = f"output_{registry_key}.tar.gz"
        archive_path = exec_dir / archive_filename
        minio_object_name = f"jobs/{tree_exec_id}/{archive_filename}"

        # Atomic collection: extract to temp location, then os.replace()
        # Temp path is in same directory as dest for same-filesystem atomic replace
        temp_path = dest_path.parent / f".{dest_path.name}.{tree_exec_id}.tmp"

        try:
            # Download from MinIO
            logger.debug(f"Downloading from MinIO: {minio_object_name} -> {archive_path}")
            self.minio.download_file(
                self.config.minio.temp_bucket,
                minio_object_name,
                str(archive_path),
            )

            # Ensure parent directory exists for temp path
            temp_path.parent.mkdir(parents=True, exist_ok=True)

            # Extract to temp location
            logger.debug(f"Extracting archive as {kind}: {archive_path} -> {temp_path}")
            extract_archive(str(archive_path), str(temp_path), path_type=kind)

            # Atomic replace: os.replace() is atomic on POSIX
            # For directories, we need to handle the case where dest exists
            if dest_path.exists():
                if dest_path.is_dir():
                    # Remove existing directory before replace
                    shutil.rmtree(dest_path)
                else:
                    # os.replace handles file overwrite atomically
                    pass

            os.replace(temp_path, dest_path)

            logger.info(f"Collected output '{registry_key}' to {dest_path} ({kind})")

        except Exception:
            # Cleanup temp on failure - don't leave partial state
            if temp_path.exists():
                if temp_path.is_dir():
                    shutil.rmtree(temp_path)
                else:
                    temp_path.unlink()
            raise

    async def _handle_dead_executors(self, dead_executor_ids: list[str]) -> None:
        """
        Handle executor deaths detected by heartbeat timeout.

        Phase 15 executor crash detection:
        - All trees running on dead executor are marked failed
        - All locks held by those trees are immediately released
        - MinIO temp data for those trees is cleaned up
        - Waiting trees can now proceed on next scheduler cycle

        Args:
            dead_executor_ids: List of executor IDs that missed 3+ heartbeats
        """
        if not dead_executor_ids:
            return

        # Find all active jobs on dead executors
        affected_trees: dict[str, str] = {}  # tree_exec_id -> executor_id

        for sm in self.job_tracker.get_active():
            if sm.job_exec.executor_id in dead_executor_ids:
                tree_exec_id = sm.job_exec.tree_execution_id
                affected_trees[tree_exec_id] = sm.job_exec.executor_id

        if not affected_trees:
            return

        logger.warning(
            f"Executor death detected: {len(dead_executor_ids)} executor(s), "
            f"{len(affected_trees)} tree(s) affected"
        )

        # Process each affected tree
        for tree_exec_id, executor_id in affected_trees.items():
            error = f"Executor death: {executor_id} (no heartbeat for 90s)"

            # Get tree execution for state update
            tex = self.tree_tracker.get(tree_exec_id)

            # Release all locks held by this tree
            self.register_lock.dequeue(tree_exec_id)
            self.validator.unregister(tree_exec_id)

            # Mark tree as failed
            if tex:
                self.tree_tracker.mark_failed(tree_exec_id, error)
                self.tree_tracker.remove(tree_exec_id)

                # Forward failure to dev client if attached
                if tex.dev_client_id:
                    await self._publish(
                        f"linearjc/dev/progress/{tex.dev_client_id}",
                        {
                            "tree_execution_id": tree_exec_id,
                            "state": "failed",
                            "error": error,
                        },
                    )

            # Mark all jobs in the tree as failed (for tracking purposes)
            for job_sm in self.job_tracker.get_by_tree(tree_exec_id):
                if job_sm.is_active:
                    job_sm.transition("failed", {"error": error})

            # Cleanup intermediate data
            asyncio.create_task(self._cleanup_execution(tree_exec_id))

            logger.info(f"Marked tree {tree_exec_id} failed due to executor death")

    async def _handle_chain_failure(
        self,
        sm: JobStateMachine,
        tex: TreeExecution | None,
        error: str,
    ) -> None:
        """Handle chain failure - cleanup and release resources."""
        tree_exec_id = sm.job_exec.tree_execution_id

        logger.warning(
            f"Chain failed at job {sm.job_exec.job_id} "
            f"(index {sm.job_exec.chain_index}/{sm.job_exec.chain_length}): {error}"
        )

        # Release all locks
        self.register_lock.dequeue(tree_exec_id)
        self.validator.unregister(tree_exec_id)

        # Update tree tracker
        if tex:
            self.tree_tracker.mark_failed(tree_exec_id, error)
            self.tree_tracker.remove(tree_exec_id)

        # Forward failure to dev client if attached
        if sm.job_exec.dev_client_id:
            await self._publish(
                f"linearjc/dev/progress/{sm.job_exec.dev_client_id}",
                {
                    "tree_execution_id": tree_exec_id,
                    "job_execution_id": sm.job_exec.job_execution_id,
                    "state": "failed",
                    "error": error,
                },
            )

        # Cleanup intermediate data (non-blocking background task)
        asyncio.create_task(self._cleanup_execution(tree_exec_id))

    async def _cleanup_execution(self, tree_exec_id: str) -> None:
        """
        Clean up intermediate data for a tree execution.

        Removes:
        - MinIO objects under jobs/{tree_exec_id}/
        - Local work directory for this execution

        This runs as a background task to not block chain completion.
        """
        try:
            # Clean MinIO intermediate objects
            if self.minio:
                deleted = self.minio.cleanup_execution(
                    self.config.minio.temp_bucket,
                    tree_exec_id,
                )
                if deleted > 0:
                    logger.debug(f"Cleaned {deleted} MinIO objects for {tree_exec_id}")

            # Clean local work directory
            work_dir = Path(self.config.work_dir) if hasattr(self.config, "work_dir") else Path("/var/lib/linearjc/work")
            exec_dir = work_dir / tree_exec_id
            if exec_dir.exists():
                shutil.rmtree(exec_dir)
                logger.debug(f"Cleaned work directory: {exec_dir}")

        except Exception as e:
            # Log but don't fail - cleanup is best-effort
            logger.warning(f"Cleanup failed for {tree_exec_id}: {e}")

    # ─────────────────────────────────────────────────────────────────────
    # Job Distribution
    # ─────────────────────────────────────────────────────────────────────

    async def _distribute_job(self, job: Job) -> None:
        """Distribute job to pool executors via Airdrop."""
        pool_executors = self.registry.get_pool_executors()
        if not pool_executors:
            logger.warning(f"No pool executors available for {job.id}")
            return

        # Look for package file
        packages_dir = Path(self.config.jobs_dir).parent / "packages"
        package_path = packages_dir / f"{job.id}.ljc"

        if not package_path.exists():
            logger.error(f"Package not found: {package_path}")
            return

        # Upload to MinIO and get presigned URL
        assert self.minio is not None
        object_name = f"packages/{job.id}-v{job.version}.ljc"
        self.minio.upload_file(
            local_path=str(package_path),
            bucket=self.config.minio.temp_bucket,
            object_name=object_name,
        )
        url = self.minio.generate_presigned_get_url(
            bucket=self.config.minio.temp_bucket,
            object_name=object_name,
            expires_seconds=3600,
        )

        # Compute checksum
        import hashlib

        with open(package_path, "rb") as f:
            checksum = hashlib.sha256(f.read()).hexdigest()

        # Announce via Airdrop
        await self._publish(
            "linearjc/jobs/available",
            {
                "job_id": job.id,
                "version": job.version,
                "capability": "pool",
                "script_artifact": {
                    "uri": url,
                    "checksum_sha256": checksum,
                    "unpacked_size_bytes": package_path.stat().st_size,
                },
            },
        )
        logger.info(f"Airdropped {job.id} v{job.version}")

    # ─────────────────────────────────────────────────────────────────────
    # Developer API Handlers
    # ─────────────────────────────────────────────────────────────────────

    async def _on_deploy_request(self, topic: str, payload: dict) -> None:
        """
        Handle deploy request from developer.

        Generates a presigned MinIO URL for package upload.

        Request payload:
            request_id: str - Unique request ID
            job_id: str - Job identifier
            package_size: int - Package size in bytes
            checksum_sha256: str - Expected SHA256 checksum
            client_id: str - Developer client ID

        Response on linearjc/deploy/response/{client_id}:
            request_id: str
            upload_url: str - Presigned PUT URL (10 min expiry)
            expires_in: int - URL expiration in seconds
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        job_id = payload.get("job_id")
        package_size = payload.get("package_size")
        checksum = payload.get("checksum_sha256")

        logger.info(
            f"Deploy request from {client_id}: job_id={job_id}, "
            f"size={package_size}, request_id={request_id}"
        )

        # Validate required fields
        if not all([request_id, job_id, package_size, checksum]):
            await self._send_deploy_error(
                client_id, request_id or "unknown",
                "Missing required fields (request_id, job_id, package_size, checksum_sha256)"
            )
            return

        assert self.minio is not None

        # Generate presigned upload URL (10 minute expiry)
        object_key = f"deployments/{job_id}-{request_id}.ljc"
        upload_url = self.minio.generate_presigned_put_url(
            bucket=self.config.minio.temp_bucket,
            object_name=object_key,
            expires_seconds=600,
        )

        # Store deployment metadata for later verification
        self._pending_deployments[request_id] = {
            "job_id": job_id,
            "object_key": object_key,
            "checksum": checksum,
            "client_id": client_id,
        }

        # Send response
        response = {
            "request_id": request_id,
            "upload_url": upload_url,
            "expires_in": 600,
        }

        await self._publish(f"linearjc/deploy/response/{client_id}", response)
        logger.info(f"Sent upload URL to {client_id} (request_id={request_id})")

    async def _on_deploy_complete(self, topic: str, payload: dict) -> None:
        """
        Handle deploy complete notification from developer.

        Downloads package from MinIO, verifies checksum, and installs.

        Request payload:
            request_id: str - Must match previous deploy request
            job_id: str - Must match original request
            checksum_sha256: str - Must match original request
            client_id: str - Developer client ID

        Response on linearjc/deploy/response/{client_id}:
            request_id: str
            status: "installed" | "failed"
            job_id: str
            version: str (if installed)
            error: str (if failed)
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        job_id = payload.get("job_id")
        checksum = payload.get("checksum_sha256")

        logger.info(
            f"Deploy complete from {client_id}: job_id={job_id}, "
            f"request_id={request_id}"
        )

        # Validate required fields
        if not all([request_id, job_id, checksum]):
            await self._send_deploy_error(
                client_id, request_id or "unknown",
                "Missing required fields (request_id, job_id, checksum_sha256)"
            )
            return

        # Check if we have this deployment
        if request_id not in self._pending_deployments:
            logger.error(f"Unknown deployment request_id: {request_id}")
            await self._send_deploy_error(client_id, request_id, "Unknown deployment")
            return

        deployment = self._pending_deployments[request_id]

        # Verify job_id matches
        if deployment["job_id"] != job_id:
            logger.error(f"Job ID mismatch: expected {deployment['job_id']}, got {job_id}")
            await self._send_deploy_error(client_id, request_id, "Job ID mismatch")
            return

        # Verify checksum matches
        if deployment["checksum"] != checksum:
            logger.error(f"Checksum mismatch for {job_id}")
            await self._send_deploy_error(client_id, request_id, "Checksum mismatch")
            return

        # Download package from MinIO
        logger.info(f"Downloading package from MinIO: {deployment['object_key']}")

        assert self.minio is not None

        try:
            package_data = self.minio.download_object_to_bytes(
                bucket=self.config.minio.temp_bucket,
                object_name=deployment["object_key"],
            )
        except Exception as e:
            logger.error(f"Failed to download package from MinIO: {e}")
            await self._send_deploy_error(
                client_id, request_id, f"MinIO download failed: {e}"
            )
            return

        # Verify checksum after download
        computed_checksum = hashlib.sha256(package_data).hexdigest()
        if computed_checksum != checksum:
            logger.error("Checksum verification failed after download")
            await self._send_deploy_error(
                client_id, request_id, "Checksum verification failed"
            )
            return

        logger.info(f"Checksum verified: {checksum[:16]}...")

        # Save to temp file and install
        try:
            with tempfile.NamedTemporaryFile(suffix=".ljc", delete=False) as tmp:
                tmp.write(package_data)
                tmp_path = Path(tmp.name)

            logger.info(f"Installing package from {tmp_path}")

            # Install the package
            result = await self._install_package(tmp_path)

            # Clean up temp file
            tmp_path.unlink()

            # Clean up pending deployment
            del self._pending_deployments[request_id]

            # Send success response
            response = {
                "request_id": request_id,
                "status": "installed",
                "job_id": result.get("job_id", job_id),
                "version": result.get("version", "unknown"),
            }

            await self._publish(f"linearjc/deploy/response/{client_id}", response)

            logger.info(
                f"Successfully installed {job_id} v{result.get('version')} "
                f"for {client_id}"
            )

        except Exception as e:
            logger.error(f"Installation failed: {e}")
            await self._send_deploy_error(client_id, request_id, str(e))
            # Clean up temp file if it exists
            if "tmp_path" in locals():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

    async def _on_registry_request(self, topic: str, payload: dict) -> None:
        """
        Handle registry request from developer (sync or push).

        Routes to sync (pull) or push based on 'action' field.

        Sync request payload:
            request_id: str
            action: "registry_sync"
            client_id: str

        Sync response:
            request_id: str
            status: "success" | "failed"
            registry: dict (all entries)
            entry_count: int

        Push request payload:
            request_id: str
            action: "registry_push"
            client_id: str
            registry: dict (entries to add/update)
            entry_count: int

        Push response:
            request_id: str
            status: "success" | "failed"
            added: int
            updated: int
            total: int
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        action = payload.get("action", "registry_sync")

        logger.info(f"Registry {action} request from {client_id}: request_id={request_id}")

        if not request_id:
            await self._send_registry_error(client_id, "unknown", "Missing request_id")
            return

        if action == "registry_push":
            await self._handle_registry_push(payload, client_id, request_id)
        else:
            await self._handle_registry_sync(payload, client_id, request_id)

    async def _handle_registry_sync(
        self,
        payload: dict,
        client_id: str,
        request_id: str,
    ) -> None:
        """Handle registry sync (pull) request."""
        # Serialize registry entries to dict format
        registry_dict = {}
        for key, entry in self.data_registry.items():
            # Convert pydantic model to dict, excluding None values
            registry_dict[key] = entry.model_dump(exclude_none=True)

        # Send response
        response = {
            "request_id": request_id,
            "status": "success",
            "registry": registry_dict,
            "entry_count": len(registry_dict),
        }

        await self._publish(f"linearjc/registry/response/{client_id}", response)
        logger.info(f"Sent registry ({len(registry_dict)} entries) to {client_id}")

    async def _handle_registry_push(
        self,
        payload: dict,
        client_id: str,
        request_id: str,
    ) -> None:
        """Handle registry push request."""
        incoming_registry = payload.get("registry", {})

        logger.info(
            f"Registry push from {client_id}: {len(incoming_registry)} entries"
        )

        added = 0
        updated = 0

        # Merge incoming entries into data_registry
        for key, entry_data in incoming_registry.items():
            try:
                # Parse and validate entry
                new_entry = DataRegistryEntry(**entry_data)

                if key in self.data_registry:
                    # Check if entry changed
                    existing = self.data_registry[key]
                    if existing.model_dump(exclude_none=True) != new_entry.model_dump(exclude_none=True):
                        self.data_registry[key] = new_entry
                        updated += 1
                        logger.info(f"Updated registry entry: {key}")
                else:
                    self.data_registry[key] = new_entry
                    added += 1
                    logger.info(f"Added registry entry: {key}")

            except Exception as e:
                logger.error(f"Invalid registry entry '{key}': {e}")
                # Continue processing other entries

        # Save registry if changes were made
        if added > 0 or updated > 0:
            try:
                self._save_data_registry()
                logger.info(f"Registry saved: {added} added, {updated} updated")
            except Exception as e:
                logger.error(f"Failed to save registry: {e}")
                await self._send_registry_error(
                    client_id, request_id, f"Failed to save registry: {e}"
                )
                return

        # Send response
        response = {
            "request_id": request_id,
            "status": "success",
            "added": added,
            "updated": updated,
            "total": len(self.data_registry),
        }

        await self._publish(f"linearjc/registry/response/{client_id}", response)
        logger.info(
            f"Registry push complete from {client_id}: {added} added, "
            f"{updated} updated, total {len(self.data_registry)} entries"
        )

    async def _send_registry_error(
        self,
        client_id: str,
        request_id: str,
        error: str,
    ) -> None:
        """Send error response for registry operation."""
        response = {
            "request_id": request_id,
            "status": "failed",
            "error": error,
        }
        await self._publish(f"linearjc/registry/response/{client_id}", response)

    def _save_data_registry(self) -> None:
        """Save data registry to YAML file (atomic write)."""
        registry_dict = {}
        for key, entry in self.data_registry.items():
            registry_dict[key] = entry.model_dump(exclude_none=True)

        # Write atomically (write to temp, then rename)
        registry_path = Path(self.config.data_registry)
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=registry_path.parent,
            delete=False,
        ) as tmp:
            yaml.dump({"registry": registry_dict}, tmp, default_flow_style=False)
            tmp_path = Path(tmp.name)

        tmp_path.rename(registry_path)
        logger.info(f"Saved {len(registry_dict)} registry entries")

    async def _on_exec_request(self, topic: str, payload: dict) -> None:
        """
        Handle exec request from developer (immediate job execution).

        Request payload:
            request_id: str
            action: "exec_job"
            client_id: str
            job_id: str - Job tree to execute
            follow: bool - If true, attach for progress updates

        Response on linearjc/dev/exec/response/{client_id}:
            request_id: str
            status: "accepted" | "rejected" | "error"
            job_execution_id: str (if accepted)
            tree_execution_id: str (if accepted)
            error: str (if rejected/error)
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        job_id = payload.get("job_id")
        follow = payload.get("follow", False)

        logger.info(
            f"Exec request from {client_id}: job_id={job_id}, "
            f"follow={follow}, request_id={request_id}"
        )

        # Validate required fields
        if not request_id:
            await self._send_exec_error(client_id, "unknown", "Missing request_id")
            return

        if not job_id:
            await self._send_exec_error(client_id, request_id, "Missing job_id")
            return

        # Find the tree for this job
        tree = None
        for t in self.trees:
            if t.root.id == job_id:
                tree = t
                break

        if not tree:
            logger.warning(f"Job not found: {job_id}")
            await self._send_exec_response(client_id, {
                "request_id": request_id,
                "status": "rejected",
                "error": f"Job not found: {job_id}",
            })
            return

        # Execute the tree
        # In async architecture, we don't need threading - just await!
        dev_client_id = client_id if follow else None

        try:
            tree_exec_id = await self._execute_tree(tree, dev_client_id)

            if not tree_exec_id:
                # Tree was blocked (output conflict or no executor)
                await self._send_exec_response(client_id, {
                    "request_id": request_id,
                    "status": "rejected",
                    "error": "Execution blocked (output conflict or no executor available)",
                })
                return

            # Get the root job execution ID
            job_exec_id = f"{tree_exec_id}-{tree.root.id}"

            # Send accepted response
            await self._send_exec_response(client_id, {
                "request_id": request_id,
                "status": "accepted",
                "job_execution_id": job_exec_id,
                "tree_execution_id": tree_exec_id,
            })

            logger.info(
                f"Exec accepted for {job_id}: job_execution_id={job_exec_id}"
            )

        except Exception as e:
            logger.error(f"Exec failed for {job_id}: {e}")
            await self._send_exec_response(client_id, {
                "request_id": request_id,
                "status": "error",
                "error": str(e),
            })

    async def _send_exec_response(self, client_id: str, payload: dict) -> None:
        """Send exec response to developer."""
        await self._publish(f"linearjc/dev/exec/response/{client_id}", payload)

    async def _send_exec_error(self, client_id: str, request_id: str, error: str) -> None:
        """Send exec error response."""
        await self._send_exec_response(client_id, {
            "request_id": request_id,
            "status": "error",
            "error": error,
        })

    async def _on_tail_request(self, topic: str, payload: dict) -> None:
        """
        Handle tail request from developer (attach to existing execution).

        Request payload:
            request_id: str
            action: "tail"
            client_id: str
            job_id: str (optional) - Find active execution by job
            execution_id: str (optional) - Attach directly by execution ID

        Response on linearjc/dev/tail/response/{client_id}:
            request_id: str
            status: "attached" | "not_found" | "error"
            job_execution_id: str (if attached)
            job_id: str (if attached)
            current_state: str (if attached)
            executor_id: str (if attached)
            error: str (if not_found/error)
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        job_id = payload.get("job_id")
        execution_id = payload.get("execution_id")

        logger.info(
            f"Tail request from {client_id}: job_id={job_id}, "
            f"execution_id={execution_id}, request_id={request_id}"
        )

        # Validate required fields
        if not request_id:
            await self._send_tail_response(client_id, {
                "request_id": "unknown",
                "status": "error",
                "error": "Missing request_id",
            })
            return

        if not job_id and not execution_id:
            await self._send_tail_response(client_id, {
                "request_id": request_id,
                "status": "error",
                "error": "Must provide either job_id or execution_id",
            })
            return

        # Find the execution
        sm = None

        if execution_id:
            # Direct attachment by execution ID
            sm = self.job_tracker.get(execution_id)
            if not sm:
                logger.warning(f"Execution not found: {execution_id}")
                await self._send_tail_response(client_id, {
                    "request_id": request_id,
                    "status": "not_found",
                    "error": f"Execution not found: {execution_id}",
                })
                return
        else:
            # Find active execution by job ID
            for active_sm in self.job_tracker.get_active():
                if active_sm.job_exec.job_id == job_id:
                    sm = active_sm
                    break

            if not sm:
                logger.warning(f"No active execution for job: {job_id}")
                await self._send_tail_response(client_id, {
                    "request_id": request_id,
                    "status": "not_found",
                    "error": f"No active execution found for job: {job_id}",
                })
                return

        # Attach dev client for progress forwarding
        sm.job_exec.dev_client_id = client_id

        # Send attached response
        await self._send_tail_response(client_id, {
            "request_id": request_id,
            "status": "attached",
            "job_execution_id": sm.job_exec.job_execution_id,
            "job_id": sm.job_exec.job_id,
            "current_state": sm.job_exec.state.value,
            "executor_id": sm.job_exec.executor_id,
        })

        logger.info(
            f"Tail attached for {sm.job_exec.job_id}: "
            f"execution={sm.job_exec.job_execution_id}, "
            f"state={sm.job_exec.state.value}"
        )

    async def _send_tail_response(self, client_id: str, payload: dict) -> None:
        """Send tail response to developer."""
        await self._publish(f"linearjc/dev/tail/response/{client_id}", payload)

    async def _on_status_request(self, topic: str, payload: dict) -> None:
        """
        Handle status request from developer.

        Request payload:
            request_id: str
            action: "status"
            client_id: str
            job_id: str (optional) - Specific job to query
            all: bool - If true, return all jobs

        Response on linearjc/dev/status/response/{client_id}:
            request_id: str
            success: bool
            data: dict - Job status info
            error: str (if failed)
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        job_id = payload.get("job_id")
        show_all = payload.get("all", False)

        logger.info(
            f"Status request from {client_id}: job_id={job_id}, all={show_all}"
        )

        if not request_id:
            await self._send_status_response(client_id, {
                "request_id": "unknown",
                "success": False,
                "error": "Missing request_id",
            })
            return

        try:
            if job_id:
                data = self._get_job_status(job_id)
            elif show_all:
                data = self._get_all_jobs_status()
            else:
                await self._send_status_response(client_id, {
                    "request_id": request_id,
                    "success": False,
                    "error": "Specify job_id or use all=true",
                })
                return

            await self._send_status_response(client_id, {
                "request_id": request_id,
                "success": True,
                "data": data,
            })

        except ValueError as e:
            await self._send_status_response(client_id, {
                "request_id": request_id,
                "success": False,
                "error": str(e),
            })

    def _get_job_status(self, job_id: str) -> dict:
        """Get status for a single job."""
        tree = None
        for t in self.trees:
            if t.root.id == job_id:
                tree = t
                break

        if not tree:
            raise ValueError(f"Job not found: {job_id}")

        now = time.time()
        active_exec = None

        # Check for active execution
        for sm in self.job_tracker.get_active():
            if sm.job_exec.job_id == job_id:
                active_exec = {
                    "execution_id": sm.job_exec.job_execution_id,
                    "state": sm.job_exec.state.value,
                    "executor": sm.job_exec.executor_id,
                    "duration": now - sm.job_exec.started_at if sm.job_exec.started_at else 0,
                    "timeout_remaining": sm.job_exec.timeout_at - now if sm.job_exec.timeout_at else None,
                }
                break

        return {
            "job_id": tree.root.id,
            "version": tree.root.version,
            "jobs_in_chain": len(tree.jobs),
            "schedule": {
                "min_daily": tree.min_daily,
                "max_daily": tree.max_daily,
            },
            "last_execution": tree.last_execution,
            "next_execution": tree.next_execution,
            "executions_24h": tree.get_executions_last_24h(now),
            "active_execution": active_exec,
        }

    def _get_all_jobs_status(self) -> dict:
        """Get status for all jobs."""
        jobs = []
        for tree in self.trees:
            try:
                status = self._get_job_status(tree.root.id)
                jobs.append(status)
            except Exception as e:
                logger.warning(f"Failed to get status for {tree.root.id}: {e}")

        return {"jobs": jobs}

    async def _send_status_response(self, client_id: str, payload: dict) -> None:
        """Send status response to developer."""
        await self._publish(f"linearjc/dev/status/response/{client_id}", payload)

    async def _on_ps_request(self, topic: str, payload: dict) -> None:
        """
        Handle ps request from developer (list active jobs).

        Request payload:
            request_id: str
            action: "ps"
            client_id: str
            executor: str (optional) - Filter by executor
            all: bool - Include completed jobs

        Response on linearjc/dev/ps/response/{client_id}:
            request_id: str
            success: bool
            data: {jobs: []}
            error: str (if failed)
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        executor_filter = payload.get("executor")
        show_all = payload.get("all", False)

        logger.info(
            f"PS request from {client_id}: executor={executor_filter}, all={show_all}"
        )

        if not request_id:
            await self._send_ps_response(client_id, {
                "request_id": "unknown",
                "success": False,
                "error": "Missing request_id",
            })
            return

        terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
        jobs = []
        now = time.time()

        for sm in self.job_tracker.get_all().values():
            # Filter by state
            if not show_all and sm.job_exec.state in terminal_states:
                continue

            # Filter by executor
            if executor_filter and sm.job_exec.executor_id != executor_filter:
                continue

            jobs.append({
                "execution_id": sm.job_exec.job_execution_id,
                "tree_execution_id": sm.job_exec.tree_execution_id,
                "job_id": sm.job_exec.job_id,
                "state": sm.job_exec.state.value,
                "executor": sm.job_exec.executor_id,
                "duration": now - sm.job_exec.started_at if sm.job_exec.started_at else 0,
                "job_index": sm.job_exec.chain_index,
            })

        await self._send_ps_response(client_id, {
            "request_id": request_id,
            "success": True,
            "data": {"jobs": jobs},
        })

    async def _send_ps_response(self, client_id: str, payload: dict) -> None:
        """Send ps response to developer."""
        await self._publish(f"linearjc/dev/ps/response/{client_id}", payload)

    async def _on_logs_request(self, topic: str, payload: dict) -> None:
        """
        Handle logs request from developer (execution history).

        Request payload:
            request_id: str
            action: "logs"
            client_id: str
            job_id: str - Job to get history for
            last: int - Number of executions (default 10)
            failed: bool - Only show failed (not implemented in history)

        Response on linearjc/dev/logs/response/{client_id}:
            request_id: str
            success: bool
            data: {job_id, total_24h, executions: []}
            error: str (if failed)
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        job_id = payload.get("job_id")
        last_n = payload.get("last", 10)

        logger.info(
            f"Logs request from {client_id}: job_id={job_id}, last={last_n}"
        )

        if not request_id:
            await self._send_logs_response(client_id, {
                "request_id": "unknown",
                "success": False,
                "error": "Missing request_id",
            })
            return

        if not job_id:
            await self._send_logs_response(client_id, {
                "request_id": request_id,
                "success": False,
                "error": "Missing job_id",
            })
            return

        # Find tree
        tree = None
        for t in self.trees:
            if t.root.id == job_id:
                tree = t
                break

        if not tree:
            await self._send_logs_response(client_id, {
                "request_id": request_id,
                "success": False,
                "error": f"Job not found: {job_id}",
            })
            return

        now = time.time()
        executions = []

        # Get execution history from tree (timestamps only)
        for timestamp in sorted(tree.execution_history, reverse=True)[:last_n]:
            age_hours = (now - timestamp) / 3600
            executions.append({
                "timestamp": timestamp,
                "age_hours": round(age_hours, 1),
            })

        await self._send_logs_response(client_id, {
            "request_id": request_id,
            "success": True,
            "data": {
                "job_id": job_id,
                "total_24h": tree.get_executions_last_24h(now),
                "executions": executions,
            },
        })

    async def _send_logs_response(self, client_id: str, payload: dict) -> None:
        """Send logs response to developer."""
        await self._publish(f"linearjc/dev/logs/response/{client_id}", payload)

    async def _on_kill_request(self, topic: str, payload: dict) -> None:
        """
        Handle kill request from developer (cancel running job).

        Request payload:
            request_id: str
            action: "kill"
            client_id: str
            execution_id: str - Execution to cancel
            force: bool - SIGKILL instead of SIGTERM

        Response on linearjc/dev/kill/response/{client_id}:
            request_id: str
            success: bool
            data: {message, execution_id, signal}
            error: str (if failed)
        """
        client_id = payload.get("client_id", "unknown")
        request_id = payload.get("request_id")
        execution_id = payload.get("execution_id")
        force = payload.get("force", False)

        logger.info(
            f"Kill request from {client_id}: execution_id={execution_id}, force={force}"
        )

        if not request_id:
            await self._send_kill_response(client_id, {
                "request_id": "unknown",
                "success": False,
                "error": "Missing request_id",
            })
            return

        if not execution_id:
            await self._send_kill_response(client_id, {
                "request_id": request_id,
                "success": False,
                "error": "Missing execution_id",
            })
            return

        sm = self.job_tracker.get(execution_id)
        if not sm:
            await self._send_kill_response(client_id, {
                "request_id": request_id,
                "success": False,
                "error": f"Execution not found: {execution_id}",
            })
            return

        terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
        if sm.job_exec.state in terminal_states:
            await self._send_kill_response(client_id, {
                "request_id": request_id,
                "success": False,
                "error": f"Job already terminated: {sm.job_exec.state.value}",
            })
            return

        if not sm.job_exec.executor_id:
            await self._send_kill_response(client_id, {
                "request_id": request_id,
                "success": False,
                "error": "Job not yet assigned to executor",
            })
            return

        # Publish cancel message to executor
        signal_name = "SIGKILL" if force else "SIGTERM"
        cancel_msg = {
            "execution_id": execution_id,
            "signal": signal_name,
        }

        cancel_topic = f"linearjc/executors/{sm.job_exec.executor_id}/cancel"
        await self._publish(cancel_topic, cancel_msg)

        logger.info(
            f"Kill signal ({signal_name}) sent to executor {sm.job_exec.executor_id} "
            f"for {execution_id}"
        )

        await self._send_kill_response(client_id, {
            "request_id": request_id,
            "success": True,
            "data": {
                "message": f"Kill signal sent to executor {sm.job_exec.executor_id}",
                "execution_id": execution_id,
                "signal": signal_name,
            },
        })

    async def _send_kill_response(self, client_id: str, payload: dict) -> None:
        """Send kill response to developer."""
        await self._publish(f"linearjc/dev/kill/response/{client_id}", payload)

    # ─────────────────────────────────────────────────────────────────────
    # Tools Self-Update
    # ─────────────────────────────────────────────────────────────────────

    async def _on_tools_version_request(self, topic: str, payload: dict) -> None:
        """
        Handle tool version query from ljc or executor.

        Returns latest version info and presigned download URL for the tool.

        Request:
            {
                "tool": "ljc",
                "platform": "x86_64-unknown-linux-musl",
                "current_version": "0.7.0",
                "client_id": "ljc-abc123"
            }

        Response (success):
            {
                "tool": "ljc",
                "version": "0.8.0",
                "platform": "x86_64-unknown-linux-musl",
                "artifact": {
                    "uri": "http://minio:9000/linearjc-tools/...",
                    "checksum_sha256": "...",
                    "size_bytes": 4521984
                }
            }

        Response (error):
            {"error": "Unknown tool/platform: ljc/aarch64-apple-darwin"}
        """
        client_id = payload.get("client_id", "unknown")
        tool = payload.get("tool")
        platform = payload.get("platform", "x86_64-unknown-linux-musl")
        current_version = payload.get("current_version", "unknown")

        logger.info(
            f"Tools version request from {client_id}: "
            f"tool={tool}, platform={platform}, current={current_version}"
        )

        response_topic = f"linearjc/tools/version/response/{client_id}"

        # Check if tools registry is configured
        if not self.tools_registry:
            await self._publish(response_topic, {
                "error": "Tools registry not configured on coordinator"
            })
            return

        # Check if tool exists
        if tool not in self.tools_registry:
            await self._publish(response_topic, {
                "error": f"Unknown tool: {tool}"
            })
            return

        # Check if platform exists
        tool_info = self.tools_registry[tool].get(platform)
        if not tool_info:
            available = ", ".join(self.tools_registry[tool].keys())
            await self._publish(response_topic, {
                "error": f"Unknown platform: {platform}. Available: {available}"
            })
            return

        # Generate presigned download URL
        assert self.minio is not None
        try:
            url = self.minio.generate_presigned_get_url(
                bucket=self.config.minio.temp_bucket,
                object_name=tool_info["path"],
                expires_seconds=3600,
            )
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for tool: {e}")
            await self._publish(response_topic, {
                "error": f"Failed to generate download URL: {e}"
            })
            return

        # Send success response
        response = {
            "tool": tool,
            "version": tool_info["version"],
            "platform": platform,
            "artifact": {
                "uri": url,
                "checksum_sha256": tool_info["checksum_sha256"],
                "size_bytes": tool_info.get("size_bytes", 0),
            }
        }

        logger.info(
            f"Sending tool version response: {tool} v{tool_info['version']} for {platform}"
        )
        await self._publish(response_topic, response)

    async def _check_executor_update(self, executor_id: str, payload: dict) -> None:
        """
        Check if executor needs an update based on heartbeat version.

        If the executor version is older than the version in tools_registry,
        sends an update command to the executor.
        """
        # Skip if tools registry not configured
        if not self.tools_registry:
            return

        # Skip if linearjc-executor not in registry
        executor_info = self.tools_registry.get("linearjc-executor")
        if not executor_info:
            return

        # Get executor's reported version and platform
        executor_version = payload.get("executor_version")
        platform = payload.get("platform", "x86_64-unknown-linux-musl")

        if not executor_version:
            logger.debug(f"Executor {executor_id} did not report version (old executor?)")
            return

        # Get registry info for this platform
        platform_info = executor_info.get(platform)
        if not platform_info:
            logger.debug(
                f"No executor update available for platform {platform}"
            )
            return

        registry_version = platform_info.get("version", "0.0.0")

        # Compare versions
        if self._version_needs_update(executor_version, registry_version):
            logger.info(
                f"Executor {executor_id} needs update: {executor_version} -> {registry_version}"
            )
            await self._send_executor_update(executor_id, platform_info, platform)
        else:
            logger.debug(
                f"Executor {executor_id} is up to date: {executor_version}"
            )

    def _version_needs_update(self, current: str, target: str) -> bool:
        """Check if current version is older than target version."""
        def parse(v: str) -> tuple[int, int, int]:
            parts = v.lstrip("v").split(".")
            return (
                int(parts[0]) if len(parts) > 0 else 0,
                int(parts[1]) if len(parts) > 1 else 0,
                int(parts[2]) if len(parts) > 2 else 0,
            )

        try:
            return parse(current) < parse(target)
        except (ValueError, IndexError):
            return False

    async def _send_executor_update(
        self,
        executor_id: str,
        tool_info: dict,
        platform: str,
    ) -> None:
        """Send update command to executor."""
        assert self.minio is not None

        try:
            url = self.minio.generate_presigned_get_url(
                bucket=self.config.minio.temp_bucket,
                object_name=tool_info["path"],
                expires_seconds=3600,
            )
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for executor update: {e}")
            return

        update_msg = {
            "version": tool_info["version"],
            "uri": url,
            "checksum_sha256": tool_info["checksum_sha256"],
            "size_bytes": tool_info.get("size_bytes", 0),
        }

        topic = f"linearjc/executors/{executor_id}/update"
        await self._publish(topic, update_msg)

        logger.info(
            f"Sent update command to {executor_id}: v{tool_info['version']} ({platform})"
        )

    # ─────────────────────────────────────────────────────────────────────
    # Hot Reload
    # ─────────────────────────────────────────────────────────────────────

    async def _hot_reload(self) -> None:
        """
        Hot reload with state preservation and safety validation.

        Phase 15 safety checks:
        - Validates registry changes are safe given current lock state
        - Blocks reload if changes would corrupt active tree operations
        - Warns on changes that may affect running trees
        """
        logger.info("=" * 60)
        logger.info("Hot reloading jobs...")

        # Load new registry without applying yet
        try:
            new_registry = load_data_registry(
                self.config.data_registry,
                allowed_data_roots=self.config.security.allowed_data_roots,
            )
        except Exception as e:
            logger.error(f"Failed to load registry: {e}")
            logger.error("Hot reload aborted - keeping current configuration")
            return

        # Phase 15: Validate registry changes are safe given current locks
        if self.register_lock is not None:
            validation = validate_registry_reload(
                self.data_registry, new_registry, self.register_lock
            )

            if validation.blocked:
                for error in validation.errors:
                    logger.error(f"Hot reload blocked: {error}")
                logger.error(
                    "Hot reload aborted - complete active trees first or restart coordinator"
                )
                return

            # Log warnings but proceed
            for warning in validation.warnings:
                logger.warning(f"Hot reload warning: {warning}")

        # Preserve scheduling state
        old_state = {
            tree.root.id: {
                "last_execution": tree.last_execution,
                "next_execution": tree.next_execution,
                "execution_history": tree.execution_history.copy(),
            }
            for tree in self.trees
        }

        # Apply new registry (validation passed)
        self.data_registry = new_registry
        self.validator.update_registry(self.data_registry)

        # Validate protected registers exist (Phase 15 register model)
        try:
            validate_protected_registers_exist(self.data_registry)
        except Exception as e:
            logger.error(f"Protected register validation failed: {e}")
            # Don't abort - registry is already applied, just warn
            logger.warning("Some protected registers may be missing")

        jobs = discover_jobs(self.config.jobs_dir)

        # Validate jobs don't write to protected registers (Phase 15)
        for job in jobs:
            try:
                validate_job_writes_against_registry(
                    job.id, job.writes, self.data_registry
                )
            except Exception as e:
                logger.error(f"Job validation failed: {e}")
                # Job will fail at dispatch time if it tries to write to protected

        self.trees = build_trees(jobs)

        # Restore state for trees that still exist
        for tree in self.trees:
            if tree.root.id in old_state:
                state = old_state[tree.root.id]
                tree.last_execution = state["last_execution"]
                tree.next_execution = state["next_execution"]
                tree.execution_history = state["execution_history"]

        logger.info(f"Reloaded {len(self.trees)} job trees")

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────

    def _generate_tree_exec_id(self, tree: JobTree) -> str:
        """Generate unique tree execution ID."""
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        unique = uuid.uuid4().hex[:8]
        return f"{tree.root.id}-{timestamp}-{unique}"

    def _build_job_request(
        self,
        job_exec: JobExecution,
        job: Job,
        inputs: dict,
        outputs: dict,
        executor_id: str,
    ) -> dict:
        """Build MQTT job request message."""
        request = {
            "tree_execution_id": job_exec.tree_execution_id,
            "job_execution_id": job_exec.job_execution_id,
            "job_id": job_exec.job_id,
            "job_version": job_exec.version,
            "assigned_to": executor_id,
            "inputs": inputs,
            "outputs": outputs,
            "run": {
                "user": job.run.user,
                "timeout": job.run.timeout,
                "entry": job.run.entry,
                "isolation": job.run.isolation,
                "network": job.run.network,
                "extra_read_paths": job.run.extra_read_paths,
            },
            "callback": f"linearjc/jobs/progress/{job_exec.job_execution_id}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Include resource limits if specified
        if job.run.limits:
            request["run"]["limits"] = {
                "cpu_percent": job.run.limits.cpu_percent,
                "memory_mb": job.run.limits.memory_mb,
                "processes": job.run.limits.processes,
            }

        return request

    def _find_tree_by_job(self, job_id: str) -> JobTree | None:
        """Find tree containing job (searches all jobs, not just root)."""
        for tree in self.trees:
            for job in tree.jobs:
                if job.id == job_id:
                    return tree
        return None

    def _find_job_by_id(self, job_id: str) -> Job | None:
        """Find job by ID across all trees."""
        for tree in self.trees:
            for job in tree.jobs:
                if job.id == job_id:
                    return job
        return None

    async def _publish_job_request(self, job_execution_id: str, request: dict) -> None:
        """Publish job request to specific executor."""
        topic = f"linearjc/jobs/requests/{job_execution_id}"
        await self._publish(topic, request)

    async def _publish(self, topic: str, payload: dict) -> None:
        """Publish signed message to MQTT."""
        assert self.mqtt is not None

        signed = sign_message(payload, self.config.signing.shared_secret)
        await self.mqtt.publish(topic, json.dumps(signed).encode())

    async def _forward_progress(self, dev_client_id: str, payload: dict) -> None:
        """Forward progress to developer client."""
        topic = f"linearjc/dev/progress/{dev_client_id}"
        await self._publish(topic, payload)

    # ─────────────────────────────────────────────────────────────────────
    # Developer API Helpers
    # ─────────────────────────────────────────────────────────────────────

    async def _send_deploy_error(
        self,
        client_id: str,
        request_id: str,
        error: str,
    ) -> None:
        """Send error response for deployment."""
        response = {
            "request_id": request_id,
            "status": "failed",
            "error": error,
        }
        await self._publish(f"linearjc/deploy/response/{client_id}", response)

    async def _install_package(self, package_path: Path) -> dict:
        """
        Install a job package (.ljc file).

        Extracts the package, validates structure, checks version,
        copies files, and triggers hot reload.

        Args:
            package_path: Path to .ljc package file

        Returns:
            dict with 'job_id' and 'version' fields

        Raises:
            ValueError: On validation or version conflicts
            Exception: On installation failure
        """
        from coordinator.archive_handler import safe_extract_member

        logger.info(f"Installing package: {package_path}")

        # Create temp directory for extraction
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir).resolve()

            # Extract package with security validation
            logger.debug("Extracting package...")
            with tarfile.open(package_path, "r:gz") as tar:
                for member in tar.getmembers():
                    safe_extract_member(tar, member, tmppath)

            # Validate structure
            job_yaml = tmppath / "job.yaml"
            script_sh = tmppath / "script.sh"

            if not job_yaml.exists():
                raise ValueError("Package missing job.yaml")

            if not script_sh.exists():
                raise ValueError("Package missing script.sh")

            # Parse job to get job_id and version
            with open(job_yaml) as f:
                job_data = yaml.safe_load(f)

            if "job" not in job_data or "id" not in job_data["job"]:
                raise ValueError("Invalid job.yaml structure")

            job_id = job_data["job"]["id"]
            version = job_data["job"].get("version", "unknown")

            # Check for existing job and handle version upgrade
            jobs_dir = Path(self.config.jobs_dir)
            dest_job = jobs_dir / f"{job_id}.yaml"

            if dest_job.exists():
                # Load existing version
                with open(dest_job) as f:
                    existing_data = yaml.safe_load(f)
                existing_version = existing_data.get("job", {}).get("version", "0.0.0")

                comparison = _compare_versions(version, existing_version)

                if comparison > 0:
                    # Upgrade: new version is higher
                    logger.info(f"Upgrading job {job_id}: {existing_version} -> {version}")
                elif comparison == 0:
                    # Same version already installed
                    raise ValueError(
                        f"Version {version} of {job_id} is already installed. "
                        f"Bump the version to deploy a new package."
                    )
                else:
                    # Downgrade attempt
                    raise ValueError(
                        f"Cannot downgrade {job_id}: {version} < {existing_version}. "
                        f"Only upgrades are allowed."
                    )
            else:
                logger.info(f"Installing new job: {job_id} v{version}")

            # Cache original .ljc package
            packages_dir = Path(self.config.jobs_dir).parent / "packages"
            packages_dir.mkdir(exist_ok=True)
            dest_package = packages_dir / f"{job_id}.ljc"
            shutil.copy2(package_path, dest_package)
            logger.debug(f"Cached package: {dest_package}")

            # Copy job.yaml to jobs directory
            shutil.copy2(job_yaml, dest_job)
            logger.info(f"Installed job definition: {dest_job}")

            # Trigger reload
            logger.info("Triggering coordinator reload (SIGHUP)")
            self._reload_requested = True

            return {
                "job_id": job_id,
                "version": version,
            }

    def _handle_reload_signal(self) -> None:
        """Signal handler for SIGHUP."""
        logger.info("Received SIGHUP signal - reload requested")
        self._reload_requested = True

    def _handle_shutdown_signal(self) -> None:
        """Signal handler for SIGTERM/SIGINT."""
        logger.info("Received shutdown signal")
        self._shutdown_requested = True
