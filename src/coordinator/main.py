#!/usr/bin/env python3
"""
LinearJC Coordinator - Main entry point.
"""
import sys
import logging
import time
import yaml
import click
import signal
from pathlib import Path
from typing import List, Optional

from coordinator.models import CoordinatorConfigFile, JobTree, DataRegistry, DataRegistryEntry
from coordinator.job_discovery import discover_jobs, validate_jobs, load_data_registry
from coordinator.tree_builder import build_trees
from coordinator.minio_manager import MinioManager
from coordinator.job_executor import JobExecutor
from coordinator.mqtt_client import MqttClient
from coordinator.capability_discovery import ExecutorRegistry
from coordinator.job_tracker import JobTracker, JobExecution, JobState
from coordinator.output_locks import OutputLockManager
from coordinator.tree_validation import TreeOutputValidator, OutputConflictError
from coordinator.logging_utils import (
    setup_logging,
    set_correlation_ids,
    clear_correlation_ids,
    log_with_fields,
    log_duration
)
from coordinator.security_utils import (
    validate_shared_secret,
    validate_minio_credentials,
    validate_path,
    SecurityError
)

# Setup logging will be called in main()
logger = logging.getLogger(__name__)


class Coordinator:
    """Main coordinator class."""

    def __init__(self, config_path: str):
        """
        Initialize coordinator.

        Args:
            config_path: Path to coordinator config YAML
        """
        self.config_path = config_path
        self.config = None
        self.trees: List[JobTree] = []
        self.data_registry = None
        self.minio_manager = None
        self.job_executor = None
        self.mqtt_client = None
        self.executor_registry = None
        self.job_tracker = None
        self.output_lock_manager = None
        self.tree_validator = None
        self._reload_requested = False

    def load_config(self):
        """Load coordinator configuration."""
        logger.info(f"Loading configuration from {self.config_path}")

        try:
            with open(self.config_path, 'r') as f:
                data = yaml.safe_load(f)

            config_file = CoordinatorConfigFile(**data)
            self.config = config_file.coordinator
            logger.info("Configuration loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def validate_security(self):
        """Validate security configuration (secrets, credentials, etc)."""
        if not self.config.security.validate_secrets:
            logger.warning("Secret validation is DISABLED - this is insecure for production!")
            return

        logger.info("Validating security configuration...")

        try:
            # Validate shared secret
            validate_shared_secret(self.config.signing.shared_secret)
            logger.info("✓ Shared secret validation passed")

            # Validate Minio credentials
            validate_minio_credentials(
                self.config.minio.access_key,
                self.config.minio.secret_key
            )
            logger.info("✓ Minio credentials validation passed")

            # Validate allowed data roots exist
            for root in self.config.security.allowed_data_roots:
                root_path = Path(root)
                if not root_path.exists():
                    logger.warning(f"Allowed data root does not exist: {root}")
                elif not root_path.is_dir():
                    raise SecurityError(f"Allowed data root is not a directory: {root}")

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

    def load_data_registry(self):
        """Load data registry."""
        logger.info(f"Loading data registry from {self.config.data_registry}")

        try:
            registry_dict = load_data_registry(self.config.data_registry)

            # Parse into DataRegistryEntry objects and validate paths
            self.data_registry = {}
            for key, entry_data in registry_dict.items():
                entry = DataRegistryEntry(**entry_data)

                # Validate filesystem paths are within allowed roots
                if entry.type == "filesystem" and entry.path:
                    try:
                        validate_path(
                            entry.path,
                            allowed_roots=self.config.security.allowed_data_roots,
                            allow_relative=False,
                            description=f"registry '{key}' path"
                        )
                    except SecurityError as e:
                        logger.error(
                            f"Data registry entry '{key}' has invalid path: {e}\n"
                            f"Allowed roots: {self.config.security.allowed_data_roots}"
                        )
                        raise

                self.data_registry[key] = entry

            logger.info(f"Loaded {len(self.data_registry)} registry entries")

            # Create tree output validator for conflict detection
            logger.info("Initializing tree output validator...")
            self.tree_validator = TreeOutputValidator(self.data_registry)

        except Exception as e:
            logger.error(f"Failed to load data registry: {e}")
            raise

    def discover_and_build_trees(self):
        """Discover jobs and build trees."""
        logger.info(f"Discovering jobs from {self.config.jobs_dir}")

        try:
            # Discover jobs
            jobs = discover_jobs(self.config.jobs_dir)

            if not jobs:
                logger.warning("No jobs discovered!")
                return

            logger.info(f"Discovered {len(jobs)} jobs")

            # Validate jobs
            errors = validate_jobs(jobs)
            if errors:
                logger.error("Job validation errors:")
                for job_id, job_errors in errors.items():
                    for error in job_errors:
                        logger.error(f"  {job_id}: {error}")
                raise ValueError("Job validation failed")

            logger.info("All jobs validated successfully")

            # Build trees
            self.trees = build_trees(jobs)
            logger.info(f"Built {len(self.trees)} job trees")

            # Display trees
            for i, tree in enumerate(self.trees, 1):
                job_chain = " → ".join([j.id for j in tree.jobs])
                logger.info(
                    f"Tree {i}: {job_chain} "
                    f"(schedule: {tree.min_daily}-{tree.max_daily} runs/day)"
                )

        except Exception as e:
            logger.error(f"Failed to discover/build trees: {e}")
            raise

    def reload_jobs(self):
        """Reload jobs from disk and rebuild trees."""
        logger.info("=" * 60)
        logger.info("Reloading jobs...")
        logger.info("=" * 60)

        try:
            # Preserve execution state
            old_trees_state = {
                tree.root.id: {
                    'last_execution': tree.last_execution,
                    'next_execution': tree.next_execution
                }
                for tree in self.trees
            }

            # Reload jobs and rebuild trees
            self.discover_and_build_trees()
            self.validate_data_registry_references()

            # Validate tree output configurations
            self.tree_validator.validate_tree_configurations(self.trees)

            # Restore execution state for trees that still exist
            for tree in self.trees:
                if tree.root.id in old_trees_state:
                    state = old_trees_state[tree.root.id]
                    tree.last_execution = state['last_execution']
                    tree.next_execution = state['next_execution']
                    logger.info(f"Restored state for tree: {tree.root.id}")

            logger.info("=" * 60)
            logger.info(f"✓ Reloaded {len(self.trees)} job trees")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Failed to reload jobs: {e}", exc_info=True)
            raise

    def validate_data_registry_references(self):
        """Validate that all job inputs/outputs reference valid registry entries."""
        logger.info("Validating data registry references...")

        errors = []
        for tree in self.trees:
            for job in tree.jobs:
                # Check inputs
                for input_name, registry_key in job.inputs.items():
                    if registry_key not in self.data_registry:
                        errors.append(
                            f"Job {job.id}: input '{input_name}' "
                            f"references unknown registry key '{registry_key}'"
                        )

                # Check outputs
                for output_name, registry_key in job.outputs.items():
                    if registry_key not in self.data_registry:
                        errors.append(
                            f"Job {job.id}: output '{output_name}' "
                            f"references unknown registry key '{registry_key}'"
                        )

        if errors:
            logger.error("Data registry validation errors:")
            for error in errors:
                logger.error(f"  {error}")
            raise ValueError("Data registry validation failed")

        logger.info("All data registry references are valid")

    def initialize_minio(self):
        """Initialize Minio manager and test connection."""
        logger.info("Initializing Minio manager...")

        try:
            self.minio_manager = MinioManager(self.config.minio)

            # Test connection
            if not self.minio_manager.test_connection():
                raise Exception("Minio connection test failed")

            # Ensure temp bucket exists
            self.minio_manager.ensure_bucket(self.config.minio.temp_bucket)

            logger.info("Minio initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Minio: {e}")
            raise

    def initialize_job_executor(self):
        """Initialize job executor."""
        logger.info("Initializing job executor...")

        try:
            # Create output lock manager for atomic filesystem writes
            logger.info("Initializing output lock manager...")
            self.output_lock_manager = OutputLockManager()

            self.job_executor = JobExecutor(
                minio_manager=self.minio_manager,
                data_registry=self.data_registry,
                temp_bucket=self.config.minio.temp_bucket,
                output_lock_manager=self.output_lock_manager,
                archive_format=self.config.archive.format,
                work_dir=self.config.work_dir
            )

            logger.info(
                f"Job executor initialized "
                f"(archive format: {self.config.archive.format}, "
                f"work dir: {self.config.work_dir})"
            )

        except Exception as e:
            logger.error(f"Failed to initialize job executor: {e}")
            raise

    def cleanup_orphaned_executions(self, age_hours: int = 24, dry_run: bool = False):
        """
        Clean up orphaned execution artifacts from Minio.

        Args:
            age_hours: Clean up executions older than this many hours
            dry_run: If True, only log what would be deleted
        """
        logger.info(
            f"Cleaning up orphaned executions "
            f"(age > {age_hours}h, dry_run={dry_run})..."
        )

        try:
            deleted_count = self.minio_manager.cleanup_orphaned_executions(
                self.config.minio.temp_bucket,
                age_hours=age_hours,
                dry_run=dry_run
            )

            if dry_run:
                logger.info(
                    f"Would delete {deleted_count} orphaned objects "
                    f"(dry run mode)"
                )
            else:
                logger.info(f"Deleted {deleted_count} orphaned objects")

        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")

    def initialize_mqtt(self):
        """Initialize MQTT client and register handlers."""
        logger.info("Initializing MQTT client...")

        try:
            self.mqtt_client = MqttClient(
                config=self.config.mqtt,
                signing_config=self.config.signing,
                client_id="linearjc-coordinator"
            )

            # Initialize executor registry and job tracker
            self.executor_registry = ExecutorRegistry(
                self.mqtt_client,
                query_timeout=5.0,
                registry_ttl=60.0
            )
            self.job_tracker = JobTracker()

            # Register message handlers
            self.mqtt_client.register_capability_response_handler(
                self.executor_registry.handle_capability_response
            )
            self.mqtt_client.register_progress_handler(
                self._handle_progress_update
            )

            # Connect to broker
            self.mqtt_client.connect()

            logger.info("MQTT client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize MQTT client: {e}")
            raise


    def _handle_progress_update(self, job_execution_id: str, message: dict):
        """Handle job progress update."""
        state = message.get('state', 'unknown')
        executor_id = message.get('executor_id', 'unknown')
        msg = message.get('message', '')

        # Extract correlation IDs from message if available
        tree_exec_id = message.get('tree_execution_id')

        # Set correlation context for progress logging
        set_correlation_ids(
            tree_exec_id=tree_exec_id,
            job_exec_id=job_execution_id,
            executor_id=executor_id
        )

        try:
            log_with_fields(
                logger, logging.INFO, f"Progress update: {msg}" if msg else "Progress update",
                state=state
            )

            # Update job tracker
            self.job_tracker.handle_progress_update(job_execution_id, message)

            # Unregister tree outputs when job completes (terminal state)
            if state in ('completed', 'failed', 'timeout'):
                if tree_exec_id:
                    tree = self._find_tree_by_execution_id(tree_exec_id)
                    if tree:
                        logger.info(f"Unregistering tree outputs for {tree.root.id}")
                        self.tree_validator.unregister_tree(tree)
                    else:
                        logger.warning(
                            f"Could not find tree for execution ID: {tree_exec_id}"
                        )
                else:
                    logger.warning(
                        f"No tree_execution_id in progress update for job {job_execution_id}"
                    )

        finally:
            clear_correlation_ids()

    def _find_tree_by_execution_id(self, tree_execution_id: str) -> Optional[JobTree]:
        """
        Find tree by its tree_execution_id.

        The tree_execution_id format is: {job_id}-{timestamp}-{uuid}
        We extract the job_id and match against tree.root.id.

        Args:
            tree_execution_id: Tree execution ID

        Returns:
            JobTree if found, None otherwise

        Note:
            This is O(n) but trees list is small (typically < 10).
        """
        if not tree_execution_id:
            return None

        # Extract job ID from tree_execution_id (format: job_id-timestamp-uuid)
        # Split from the right to handle job IDs that may contain hyphens
        parts = tree_execution_id.rsplit('-', 2)
        if len(parts) < 3:
            logger.warning(f"Invalid tree_execution_id format: {tree_execution_id}")
            return None

        job_id = parts[0]

        for tree in self.trees:
            if tree.root.id == job_id:
                return tree

        return None

    def execute_tree(self, tree: JobTree) -> str:
        """
        Execute a tree by publishing its root job.

        Args:
            tree: The tree to execute

        Returns:
            tree_execution_id if successful
        """
        root_job = tree.root

        # Generate execution ID
        tree_execution_id = self.job_executor.generate_tree_execution_id(tree)
        job_execution_id = f"{root_job.id}-{tree_execution_id.split('-', 1)[1]}"

        # Set correlation IDs for all logging in this context
        set_correlation_ids(tree_exec_id=tree_execution_id, job_exec_id=job_execution_id)

        try:
            # Validate no output conflicts with currently active trees
            try:
                self.tree_validator.validate_tree(tree)
                self.tree_validator.register_tree(tree)
            except OutputConflictError as e:
                logger.error(f"Cannot execute tree due to output conflict:\n{e}")
                raise

            log_with_fields(
                logger, logging.INFO, "Starting tree execution",
                job_id=root_job.id,
                state="starting"
            )

            # Prepare inputs with timing
            with log_duration("input_preparation", logger, inputs=len(root_job.inputs)):
                prepared_inputs = self.job_executor.prepare_tree_inputs(
                    tree,
                    tree_execution_id
                )

            # Prepare outputs
            with log_duration("output_preparation", logger, outputs=len(root_job.outputs)):
                prepared_outputs = self.job_executor.prepare_job_outputs(
                    root_job,
                    tree_execution_id
                )

            # Build job request
            job_request = self.job_executor.build_job_request(
                root_job,
                tree_execution_id,
                job_execution_id,
                prepared_inputs,
                prepared_outputs
            )

            # Find executor for job
            if self.executor_registry.is_stale():
                log_with_fields(logger, logging.INFO, "Executor registry stale, refreshing...")
                self.executor_registry.query_capabilities()

            executor_id = self.executor_registry.find_executor(root_job.id, root_job.version)
            if not executor_id:
                raise Exception(f"No executor available for {root_job.id} v{root_job.version}")

            # Add assigned_to field
            job_request['assigned_to'] = executor_id

            # Track job execution
            now = time.time()
            job_exec = JobExecution(
                job_execution_id=job_execution_id,
                tree_execution_id=tree_execution_id,
                job_id=root_job.id,
                job_version=root_job.version,
                state=JobState.QUEUED,
                executor_id=executor_id,
                assigned_at=now,
                timeout_at=now + root_job.executor.timeout,
                last_progress=now
            )
            self.job_tracker.add(job_exec)

            # Publish to MQTT
            log_with_fields(
                logger, logging.INFO, "Publishing job request to MQTT",
                state="publishing",
                executor=executor_id
            )
            self.mqtt_client.publish_job_request(job_execution_id, job_request)

            # Update tree execution tracking
            tree.last_execution = now
            ideal_interval = (24 * 3600) / tree.max_daily
            tree.next_execution = now + ideal_interval

            # Record execution in sliding history
            tree.record_execution(now)

            log_with_fields(
                logger, logging.INFO, "Tree scheduled successfully",
                state="scheduled",
                next_interval_hours=round(ideal_interval/3600, 1)
            )

            return tree_execution_id

        except Exception as e:
            log_with_fields(
                logger, logging.ERROR, f"Tree execution failed: {e}",
                state="failed",
                error=str(e)
            )
            logger.error("Exception details:", exc_info=True)
            raise
        finally:
            # Clear correlation IDs when done
            clear_correlation_ids()

    def schedule_tree(self, tree: JobTree) -> Optional[str]:
        """
        Check if a tree should be scheduled and execute it if needed.

        Implements the scheduling algorithm from SPEC.md section 5.3:
        - First execution: schedule immediately
        - Must run: if time_since_last >= min_interval
        - May run: if time_since_last >= ideal_interval

        Args:
            tree: The tree to check

        Returns:
            tree_execution_id if tree was scheduled, None otherwise
        """
        now = time.time()

        # Calculate intervals in seconds
        ideal_interval = (24 * 3600) / tree.max_daily  # 24h / max_daily
        min_interval = (24 * 3600) / tree.min_daily     # 24h / min_daily

        # First execution - schedule immediately
        if tree.last_execution is None:
            logger.info(f"First execution for tree {tree.root.id}")
            return self.execute_tree(tree)

        # Calculate time since last execution
        time_since_last = now - tree.last_execution

        # Must run if minimum interval exceeded
        if time_since_last >= min_interval:
            hours_since = time_since_last / 3600
            logger.info(
                f"Tree {tree.root.id} must run (last: {hours_since:.1f}h ago, "
                f"min interval: {min_interval/3600:.1f}h)"
            )
            return self.execute_tree(tree)

        # May run if ideal interval reached
        elif time_since_last >= ideal_interval:
            hours_since = time_since_last / 3600
            logger.info(
                f"Tree {tree.root.id} ready to run (last: {hours_since:.1f}h ago, "
                f"ideal interval: {ideal_interval/3600:.1f}h)"
            )
            return self.execute_tree(tree)

        # Not ready yet
        return None

    def _handle_reload_signal(self, signum, frame):
        """Signal handler for SIGHUP to trigger job reload."""
        logger.info("Received SIGHUP signal - reload requested")
        self._reload_requested = True

    def scheduler_loop(self):
        """
        Main scheduling loop.

        Continuously checks all trees and schedules them according to their
        min_daily and max_daily constraints.
        """
        logger.info("=" * 60)
        logger.info("Starting scheduler loop...")
        logger.info(f"  Loop interval: {self.config.scheduling.loop_interval}s")
        logger.info(f"  Trees to schedule: {len(self.trees)}")
        logger.info("=" * 60)

        # Set up signal handler for reload
        signal.signal(signal.SIGHUP, self._handle_reload_signal)
        logger.info("Signal handler registered: SIGHUP -> reload jobs")

        # Print schedule summary
        for tree in self.trees:
            logger.info(
                f"  {tree.root.id}: {tree.min_daily}-{tree.max_daily} runs/day "
                f"(every {24/tree.max_daily:.1f}h - {24/tree.min_daily:.1f}h)"
            )
        logger.info("")

        try:
            while True:
                # Check if reload was requested
                if self._reload_requested:
                    self._reload_requested = False
                    try:
                        self.reload_jobs()
                    except Exception as e:
                        logger.error(f"Reload failed: {e}", exc_info=True)

                # Check for timed-out jobs
                timed_out = self.job_tracker.check_timeouts()
                if timed_out:
                    logger.warning(f"Found {len(timed_out)} timed-out jobs")

                # Remove completed jobs from tracker
                self.job_tracker.remove_completed()

                # Check each tree
                for tree in self.trees:
                    try:
                        tree_id = self.schedule_tree(tree)
                        if tree_id:
                            logger.debug(f"Scheduled: {tree_id}")
                    except Exception as e:
                        logger.error(
                            f"Error scheduling tree {tree.root.id}: {e}",
                            exc_info=True
                        )

                # Sleep until next check
                time.sleep(self.config.scheduling.loop_interval)

        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in scheduler loop: {e}", exc_info=True)
            raise

    def publish_test_job(self):
        """
        Publish a test job request to MQTT.

        This demonstrates the full flow of preparing inputs and publishing.
        """
        if not self.trees:
            logger.warning("No trees available to publish")
            return

        logger.info("=" * 60)
        logger.info("Publishing test job to MQTT...")
        logger.info("=" * 60)

        # Use first tree for testing
        tree = self.trees[0]
        root_job = tree.root

        try:
            # Generate execution ID
            tree_execution_id = self.job_executor.generate_tree_execution_id(tree)
            job_execution_id = f"{root_job.id}-{tree_execution_id.split('-', 1)[1]}"

            logger.info(f"Tree execution ID: {tree_execution_id}")
            logger.info(f"Job execution ID: {job_execution_id}")

            # Prepare inputs
            logger.info("Preparing inputs...")
            prepared_inputs = self.job_executor.prepare_tree_inputs(
                tree,
                tree_execution_id
            )
            logger.info(f"  ✓ Prepared {len(prepared_inputs)} inputs")

            # Prepare outputs
            logger.info("Preparing output URLs...")
            prepared_outputs = self.job_executor.prepare_job_outputs(
                root_job,
                tree_execution_id
            )
            logger.info(f"  ✓ Prepared {len(prepared_outputs)} outputs")

            # Build job request
            logger.info("Building job request...")
            job_request = self.job_executor.build_job_request(
                root_job,
                tree_execution_id,
                job_execution_id,
                prepared_inputs,
                prepared_outputs
            )

            # Publish to MQTT
            logger.info("Publishing to MQTT...")
            self.mqtt_client.publish_job_request(job_execution_id, job_request)

            logger.info("=" * 60)
            logger.info("✓ Test job published successfully!")
            logger.info("=" * 60)
            logger.info("")
            logger.info("Job details:")
            logger.info(f"  Job ID: {root_job.id} v{root_job.version}")
            logger.info(f"  Execution ID: {job_execution_id}")
            logger.info(f"  Inputs: {list(prepared_inputs.keys())}")
            logger.info(f"  Outputs: {list(prepared_outputs.keys())}")
            logger.info(f"  User: {root_job.executor.user}")
            logger.info(f"  Timeout: {root_job.executor.timeout}s")
            logger.info("")

            return tree_execution_id

        except Exception as e:
            logger.error(f"Failed to publish test job: {e}", exc_info=True)
            raise

    def initialize(self):
        """Initialize coordinator components."""
        logger.info("=" * 60)
        logger.info("LinearJC Coordinator starting...")
        logger.info("=" * 60)

        # Load configuration
        self.load_config()

        # Validate security (secrets, credentials)
        self.validate_security()

        # Load data registry
        self.load_data_registry()

        # Discover jobs and build trees
        self.discover_and_build_trees()

        # Validate data registry references
        self.validate_data_registry_references()

        # Validate tree output configurations (static conflicts)
        logger.info("Validating tree output configurations...")
        try:
            self.tree_validator.validate_tree_configurations(self.trees)
            logger.info("✓ No tree output conflicts detected")
        except OutputConflictError as e:
            logger.error("=" * 70)
            logger.error(str(e))
            logger.error("=" * 70)
            raise

        # Initialize Minio
        self.initialize_minio()

        # Initialize job executor
        self.initialize_job_executor()

        # Initialize MQTT client
        self.initialize_mqtt()

        logger.info("=" * 60)
        logger.info("Coordinator initialized successfully!")
        logger.info("=" * 60)
        logger.info("")
        logger.info("Summary:")
        logger.info(f"  Jobs discovered: {sum(len(tree.jobs) for tree in self.trees)}")
        logger.info(f"  Job trees: {len(self.trees)}")
        logger.info(f"  Data registry entries: {len(self.data_registry)}")
        logger.info(f"  Minio bucket: {self.config.minio.temp_bucket}")
        logger.info(f"  MQTT broker: {self.config.mqtt.broker}:{self.config.mqtt.port}")
        logger.info("")

        # Query executor capabilities
        logger.info("Querying executors for capabilities...")
        self.executor_registry.query_capabilities()

        executors = self.executor_registry.get_all_executors()
        if executors:
            logger.info(f"✓ Found {len(executors)} executor(s)")
            for executor_id, info in executors.items():
                logger.info(f"  - {executor_id}: {len(info.capabilities)} jobs")
        else:
            logger.info("  (No executors responded)")
        logger.info("")

    def run(self):
        """Run coordinator in scheduler mode."""
        try:
            self.initialize()

            # Start scheduler loop
            self.scheduler_loop()

        except KeyboardInterrupt:
            logger.info("Coordinator stopped by user")
        except Exception as e:
            logger.error(f"Coordinator failed: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # Cleanup
            if self.mqtt_client and self.mqtt_client.is_connected():
                logger.info("Disconnecting MQTT client...")
                self.mqtt_client.disconnect()

    def shutdown(self):
        """Clean shutdown of coordinator."""
        if self.mqtt_client and self.mqtt_client.is_connected():
            logger.info("Disconnecting MQTT client...")
            self.mqtt_client.disconnect()

    def get_state_summary(self) -> dict:
        """
        Get current coordinator state for querying/display.

        Returns:
            Dictionary with current state including trees, executions, etc.
        """
        import time
        now = time.time()

        trees_data = []
        for tree in self.trees:
            job_chain = " → ".join([j.id for j in tree.jobs])

            # Calculate time-related info
            last_exec_ago = None
            if tree.last_execution:
                last_exec_ago = (now - tree.last_execution) / 3600  # hours

            next_exec_in = None
            if tree.next_execution:
                next_exec_in = (tree.next_execution - now) / 3600  # hours

            trees_data.append({
                'root_job': tree.root.id,
                'job_chain': job_chain,
                'min_daily': tree.min_daily,
                'max_daily': tree.max_daily,
                'last_execution': tree.last_execution,
                'last_execution_ago_hours': last_exec_ago,
                'next_execution': tree.next_execution,
                'next_execution_in_hours': next_exec_in,
                'executions_last_24h': tree.get_executions_last_24h(now),
                'within_bounds': tree.is_within_bounds(now),
                'boundary_status': tree.get_boundary_status(now),
                'execution_history': tree.execution_history.copy(),
            })

        active_jobs = []
        if self.job_tracker:
            for job_id, job_exec in self.job_tracker.get_all_jobs().items():
                active_jobs.append({
                    'job_execution_id': job_exec.job_execution_id,
                    'job_id': job_exec.job_id,
                    'state': job_exec.state.value,
                    'executor_id': job_exec.executor_id,
                })

        executors = []
        if self.executor_registry:
            for exec_id, exec_info in self.executor_registry.get_all_executors().items():
                executors.append({
                    'executor_id': exec_info.executor_id,
                    'hostname': exec_info.hostname,
                    'capabilities': len(exec_info.capabilities),
                })

        return {
            'timestamp': now,
            'trees': trees_data,
            'active_jobs': active_jobs,
            'executors': executors,
        }


@click.group()
@click.option(
    '--config', '-c',
    default='/etc/linearjc/config.yaml',
    type=click.Path(exists=True),
    help='Path to coordinator configuration file'
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
    help='Override log level from config'
)
@click.pass_context
def cli(ctx, config, log_level):
    """LinearJC Coordinator - Distributed job scheduling system."""
    # Load config to get logging settings (but don't fail if config is invalid for some commands)
    level = "INFO"  # Default
    json_format = False
    log_file = None
    module_levels = {}

    try:
        with open(config, 'r') as f:
            config_data = yaml.safe_load(f)
        if 'coordinator' in config_data and 'logging' in config_data['coordinator']:
            logging_config = config_data['coordinator']['logging']
            level = logging_config.get('level', 'INFO')
            json_format = logging_config.get('json_format', False)
            log_file = logging_config.get('file')
            module_levels = logging_config.get('module_levels', {})
    except Exception:
        # If config can't be loaded, use defaults (some commands like --help don't need valid config)
        pass

    # CLI option overrides config
    if log_level:
        level = log_level

    # Setup structured logging with all configuration
    setup_logging(
        level=level,
        json_output=json_format,
        log_file=log_file,
        module_levels=module_levels
    )

    # Store config path in context
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config


@cli.command()
@click.pass_context
def run(ctx):
    """Run the coordinator scheduler loop (default mode)."""
    config_path = ctx.obj['config_path']
    coordinator = Coordinator(config_path)
    coordinator.run()


@cli.command()
@click.option(
    '--age-hours',
    default=24,
    type=int,
    help='Clean up executions older than this many hours'
)
@click.option(
    '--dry-run/--no-dry-run',
    default=False,
    help='Only show what would be deleted without actually deleting'
)
@click.option(
    '--work-dir/--no-work-dir',
    default=True,
    help='Also clean up work directory (default: yes)'
)
@click.pass_context
def cleanup(ctx, age_hours, dry_run, work_dir):
    """Clean up orphaned execution artifacts from MinIO and work directory."""
    config_path = ctx.obj['config_path']
    coordinator = Coordinator(config_path)

    try:
        # Initialize only what's needed for cleanup
        coordinator.load_config()

        # Clean up Minio
        coordinator.initialize_minio()
        minio_deleted = coordinator.cleanup_orphaned_executions(
            age_hours=age_hours,
            dry_run=dry_run
        )

        # Clean up work directory
        if work_dir:
            coordinator.load_data_registry()
            coordinator.initialize_job_executor()
            work_deleted = coordinator.job_executor.cleanup_work_directory(
                age_hours=age_hours,
                dry_run=dry_run
            )

            logger.info("=" * 60)
            logger.info("Cleanup Summary:")
            logger.info(f"  Minio objects: {minio_deleted}")
            logger.info(f"  Work directories: {work_deleted}")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def test_job(ctx):
    """Publish a test job to verify the system is working."""
    config_path = ctx.obj['config_path']
    coordinator = Coordinator(config_path)

    try:
        coordinator.initialize()
        coordinator.publish_test_job()

        # Wait a bit for the job to process
        logger.info("Waiting for job to process...")
        time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        coordinator.shutdown()


@cli.command()
@click.pass_context
def status(ctx):
    """Show coordinator status and current job trees."""
    config_path = ctx.obj['config_path']
    coordinator = Coordinator(config_path)

    try:
        coordinator.load_config()
        coordinator.load_data_registry()
        coordinator.discover_and_build_trees()
        coordinator.validate_data_registry_references()

        # Display status
        click.echo("=" * 60)
        click.echo("Coordinator Status")
        click.echo("=" * 60)
        click.echo(f"Config: {config_path}")
        click.echo(f"Jobs directory: {coordinator.config.jobs_dir}")
        click.echo(f"Job trees: {len(coordinator.trees)}")
        click.echo(f"Total jobs: {sum(len(tree.jobs) for tree in coordinator.trees)}")
        click.echo(f"Data registry entries: {len(coordinator.data_registry)}")
        click.echo("")

        # Display trees
        click.echo("Job Trees:")
        for i, tree in enumerate(coordinator.trees, 1):
            job_chain = " → ".join([j.id for j in tree.jobs])
            click.echo(f"  {i}. {job_chain}")
            click.echo(f"     Schedule: {tree.min_daily}-{tree.max_daily} runs/day")

    except Exception as e:
        logger.error(f"Status check failed: {e}", exc_info=True)
        sys.exit(1)


@cli.command()
@click.option('--json', 'output_json', is_flag=True, help='Output as JSON')
@click.pass_context
def monitor(ctx, output_json):
    """Show live coordinator state and job execution status."""
    config_path = ctx.obj['config_path']

    # Check if coordinator is running by trying to import state
    # For now, we'll just load config and show what we can from disk
    # In future, this could connect to a running coordinator via socket/API

    click.echo("Note: Monitoring is limited to static configuration.")
    click.echo("For live state, a running coordinator would need to expose an API.")
    click.echo("")

    # For now, just show status
    ctx.invoke(status)


@cli.command()
@click.option('--hours', default=24, help='Show executions from last N hours')
@click.pass_context
def timeline(ctx, hours):
    """Show job execution timeline from logs."""
    config_path = ctx.obj['config_path']

    click.echo(f"Job Execution Timeline (last {hours}h)")
    click.echo("=" * 60)
    click.echo("")
    click.echo("Note: This would parse coordinator logs to show execution history.")
    click.echo("For live timeline, use 'grep \"Tree scheduled successfully\" logs/coordinator.log'")
    click.echo("")
    click.echo("Example:")
    click.echo("  grep -E '(Tree scheduled|Job completed)' logs/coordinator.log | tail -20")


def main():
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
