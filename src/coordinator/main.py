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
from coordinator.developer_api import DeveloperAPI
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


def compare_versions(v1: str, v2: str) -> int:
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
            return [int(x) for x in v.split('.')]
        except (ValueError, AttributeError):
            return [0, 0, 0]

    parts1 = parse_version(v1)
    parts2 = parse_version(v2)

    for i in range(3):
        p1 = parts1[i] if i < len(parts1) else 0
        p2 = parts2[i] if i < len(parts2) else 0
        if p1 > p2:
            return 1
        elif p1 < p2:
            return -1

    return 0


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
        self.developer_api = None
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

    def save_data_registry(self, data_registry: dict = None):
        """Save data registry to YAML file."""
        import yaml

        registry_to_save = data_registry if data_registry is not None else self.data_registry

        logger.info(f"Saving data registry to {self.config.data_registry}")

        try:
            # Convert DataRegistryEntry objects to dicts
            registry_dict = {}
            for key, entry in registry_to_save.items():
                if hasattr(entry, 'model_dump'):
                    registry_dict[key] = entry.model_dump(exclude_none=True)
                else:
                    registry_dict[key] = entry

            # Write YAML file
            with open(self.config.data_registry, 'w') as f:
                yaml.dump({'registry': registry_dict}, f, default_flow_style=False, sort_keys=True)

            logger.info(f"Saved {len(registry_dict)} registry entries")

        except Exception as e:
            logger.error(f"Failed to save data registry: {e}")
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
        """Validate that all job reads/writes reference valid registry entries."""
        logger.info("Validating data registry references...")

        errors = []
        for tree in self.trees:
            for job in tree.jobs:
                # Check reads
                for registry_key in job.reads:
                    if registry_key not in self.data_registry:
                        errors.append(
                            f"Job {job.id}: reads unknown registry key '{registry_key}'"
                        )

                # Check writes
                for registry_key in job.writes:
                    if registry_key not in self.data_registry:
                        errors.append(
                            f"Job {job.id}: writes unknown registry key '{registry_key}'"
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

            # Initialize developer API
            self.developer_api = DeveloperAPI(
                minio_manager=self.minio_manager,
                mqtt_client=self.mqtt_client,
                shared_secret=self.config.signing.shared_secret,
                coordinator_id="linearjc-coordinator",
                install_callback=self.install_package_from_api,
                exec_callback=self._exec_tree_for_dev_api,
            )

            # Register developer API handlers
            self.mqtt_client.register_developer_deploy_request_handler(
                self.developer_api.handle_deploy_request
            )
            self.mqtt_client.register_developer_deploy_complete_handler(
                self.developer_api.handle_deploy_complete
            )
            # Registry sync/push handler needs access to data_registry and save callback
            self.mqtt_client.register_developer_registry_sync_handler(
                lambda envelope, client_id: self.developer_api.handle_registry_sync_request(
                    envelope, client_id, self.data_registry, self.save_data_registry
                )
            )
            # Exec handler needs access to trees
            self.mqtt_client.register_developer_exec_handler(
                lambda envelope, client_id: self.developer_api.handle_exec_request(
                    envelope, client_id, self.trees
                )
            )
            # Tail handler needs access to job_tracker
            self.mqtt_client.register_developer_tail_handler(
                lambda envelope, client_id: self.developer_api.handle_tail_request(
                    envelope, client_id, self.job_tracker
                )
            )
            # Status handler needs access to trees and job_tracker
            self.mqtt_client.register_developer_status_handler(
                lambda envelope, client_id: self.developer_api.handle_status_request(
                    envelope, client_id, self.trees, self.job_tracker
                )
            )
            # PS handler needs access to job_tracker
            self.mqtt_client.register_developer_ps_handler(
                lambda envelope, client_id: self.developer_api.handle_ps_request(
                    envelope, client_id, self.job_tracker
                )
            )
            # Logs handler needs access to trees
            self.mqtt_client.register_developer_logs_handler(
                lambda envelope, client_id: self.developer_api.handle_logs_request(
                    envelope, client_id, self.trees
                )
            )
            # Kill handler needs access to job_tracker
            self.mqtt_client.register_developer_kill_handler(
                lambda envelope, client_id: self.developer_api.handle_kill_request(
                    envelope, client_id, self.job_tracker
                )
            )

            # Connect to broker
            self.mqtt_client.connect()

            logger.info("MQTT client initialized successfully")
            logger.info("Developer API initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize MQTT client: {e}")
            raise


    def _exec_tree_for_dev_api(self, tree: JobTree, dev_client_id: Optional[str] = None) -> dict:
        """
        Execute a tree for the developer API (immediate execution).

        This wraps execute_tree to return the execution IDs needed by
        the developer API.

        Args:
            tree: The tree to execute
            dev_client_id: If set, forward progress updates to this developer client

        Returns:
            dict with 'tree_execution_id' and 'job_execution_id'

        Raises:
            Exception on execution failure
        """
        root_job = tree.root

        logger.info(
            f"Dev API executing tree: job={root_job.id}, dev_client={dev_client_id}"
        )

        # Execute the tree with dev_client_id for progress forwarding
        # Note: dev_client_id is registered in JobTracker BEFORE MQTT publish
        tree_execution_id = self.execute_tree(tree, dev_client_id=dev_client_id)
        job_execution_id = f"{root_job.id}-{tree_execution_id.split('-', 1)[1]}"

        return {
            'tree_execution_id': tree_execution_id,
            'job_execution_id': job_execution_id,
        }

    def install_package_from_api(self, package_path: Path) -> dict:
        """
        Install a package from the developer API.

        This is a wrapper around the install command logic that can be called
        by the developer API.

        Args:
            package_path: Path to .ljc package file

        Returns:
            dict with 'job_id' and 'version' fields

        Raises:
            Exception on installation failure
        """
        import tarfile
        import tempfile
        import shutil

        logger.info(f"Installing package from API: {package_path}")

        # Create temp directory for extraction
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir).resolve()

            # Extract package with security validation
            logger.debug("Extracting package...")
            from coordinator.archive_handler import safe_extract_member, ArchiveError

            with tarfile.open(package_path, 'r:gz') as tar:
                for member in tar.getmembers():
                    safe_extract_member(tar, member, tmppath)

            # Validate structure
            job_yaml = tmppath / 'job.yaml'
            script_sh = tmppath / 'script.sh'

            if not job_yaml.exists():
                raise ValueError("Package missing job.yaml")

            if not script_sh.exists():
                raise ValueError("Package missing script.sh")

            # Parse job to get job_id and version
            with open(job_yaml) as f:
                job_data = yaml.safe_load(f)

            if 'job' not in job_data or 'id' not in job_data['job']:
                raise ValueError("Invalid job.yaml structure")

            job_id = job_data['job']['id']
            version = job_data['job'].get('version', 'unknown')

            # Check for existing job and handle version upgrade
            jobs_dir = Path(self.config.jobs_dir)
            dest_job = jobs_dir / f"{job_id}.yaml"

            if dest_job.exists():
                # Load existing version
                with open(dest_job) as f:
                    existing_data = yaml.safe_load(f)
                existing_version = existing_data.get('job', {}).get('version', '0.0.0')

                comparison = compare_versions(version, existing_version)

                if comparison > 0:
                    # Upgrade: new version is higher
                    logger.info(f"Upgrading job {job_id}: {existing_version} → {version}")
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
            packages_dir = Path(self.config.jobs_dir).parent / 'packages'
            packages_dir.mkdir(exist_ok=True)
            dest_package = packages_dir / f"{job_id}.ljc"
            shutil.copy2(package_path, dest_package)
            logger.debug(f"Cached package: {dest_package}")

            # Copy job.yaml to jobs directory (dest_job already defined above)
            shutil.copy2(job_yaml, dest_job)
            logger.info(f"Installed job definition: {dest_job}")

            # TODO: Merge data registry if exists (skipping for now)

            # Trigger reload
            logger.info("Triggering coordinator reload (SIGHUP)")
            self._reload_requested = True

            return {
                'job_id': job_id,
                'version': version
            }

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

            # Forward progress to developer if this is a dev execution
            # Note: dev_client_id is stored in JobExecution, set BEFORE MQTT publish
            job_exec = self.job_tracker.get_job(job_execution_id)
            if job_exec and job_exec.dev_client_id:
                self._forward_progress_to_dev_client(job_exec.dev_client_id, job_execution_id, message)

            # Update job tracker
            self.job_tracker.handle_progress_update(job_execution_id, message)

            # Handle terminal states (completed, failed, timeout)
            if state in ('completed', 'failed', 'timeout'):
                if tree_exec_id:
                    tree = self._find_tree_by_execution_id(tree_exec_id)
                    if tree:
                        job_exec = self.job_tracker.get_job(job_execution_id)
                        current_job_index = job_exec.job_index if job_exec else 0
                        is_last_job = (current_job_index >= len(tree.jobs) - 1)

                        if state == 'completed' and not is_last_job:
                            # Chain continuation: execute next job
                            next_job_index = current_job_index + 1
                            next_job = tree.jobs[next_job_index]
                            logger.info(
                                f"Chain continuation: {tree.jobs[current_job_index].id} → {next_job.id} "
                                f"(job {next_job_index + 1}/{len(tree.jobs)})"
                            )
                            try:
                                # Pass dev_client_id for progress forwarding
                                dev_client = job_exec.dev_client_id if job_exec else None
                                self._execute_chain_job(
                                    tree,
                                    tree_exec_id,
                                    next_job_index,
                                    dev_client_id=dev_client
                                )
                            except Exception as e:
                                logger.error(
                                    f"Failed to execute chain job {next_job.id}: {e}",
                                    exc_info=True
                                )
                                # Unregister tree on chain failure
                                logger.info(f"Unregistering tree outputs due to chain failure")
                                self.tree_validator.unregister_tree(tree)
                        else:
                            # Terminal state: collect outputs and cleanup
                            if state == 'completed':
                                # Last job completed - collect outputs from leaf job
                                try:
                                    logger.info(
                                        f"Chain complete: collecting outputs from {tree.jobs[-1].id} "
                                        f"(tree: {tree.root.id})"
                                    )
                                    self.job_executor.collect_tree_outputs(tree, tree_exec_id)
                                    logger.info(f"Outputs collected successfully for {tree.root.id}")
                                except Exception as e:
                                    logger.error(
                                        f"Failed to collect outputs for {tree.root.id}: {e}",
                                        exc_info=True
                                    )
                                    # Continue to unregister locks even if collection fails
                            else:
                                # Failed or timeout - log the failure
                                failed_job = tree.jobs[current_job_index]
                                logger.warning(
                                    f"Chain terminated: {failed_job.id} {state} "
                                    f"(job {current_job_index + 1}/{len(tree.jobs)})"
                                )

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

    def _forward_progress_to_dev_client(
        self,
        dev_client_id: str,
        job_execution_id: str,
        message: dict
    ) -> None:
        """
        Forward progress update to a developer client via MQTT.

        Args:
            dev_client_id: Developer client ID to forward to
            job_execution_id: Job execution ID
            message: Progress message from executor
        """
        topic = f"linearjc/dev/progress/{dev_client_id}"

        try:
            # Build progress message for developer
            progress = {
                'job_execution_id': job_execution_id,
                'state': message.get('state'),
                'message': message.get('message', ''),
                'executor_id': message.get('executor_id'),
                'tree_execution_id': message.get('tree_execution_id'),
            }

            # Include error/exit_code for terminal states
            state = message.get('state')
            if state in ('failed', 'timeout'):
                progress['error'] = message.get('error', '')
                progress['exit_code'] = message.get('exit_code', 1)

            self.mqtt_client.publish_message(topic, progress, qos=1)

            logger.debug(
                f"Forwarded progress to dev client {dev_client_id}: state={state}"
            )

        except Exception as e:
            logger.error(f"Failed to forward progress to {dev_client_id}: {e}")

    def _find_tree_by_execution_id(self, tree_execution_id: str) -> Optional[JobTree]:
        """
        Find tree by its tree_execution_id.

        The tree_execution_id format is: {job_id}-{YYYYMMDD}-{HHMMSS}-{uuid8}
        Example: hello.world-20251116-131651-c4630045

        We extract the job_id and match against tree.root.id.

        Args:
            tree_execution_id: Tree execution ID

        Returns:
            JobTree if found, None otherwise

        Note:
            This is O(n) but trees list is small (typically < 10).
            We split from the right to preserve job IDs that contain hyphens.
        """
        if not tree_execution_id:
            return None

        # Extract job ID from tree_execution_id (format: job_id-YYYYMMDD-HHMMSS-uuid8)
        # Split from the right to handle job IDs that may contain hyphens
        # Example: "my-job-20251116-131651-c4630045" -> ["my-job", "20251116", "131651", "c4630045"]
        parts = tree_execution_id.rsplit('-', 3)
        if len(parts) < 4:
            logger.warning(f"Invalid tree_execution_id format: {tree_execution_id}")
            return None

        job_id = parts[0]

        for tree in self.trees:
            if tree.root.id == job_id:
                return tree

        return None

    def _execute_chain_job(
        self,
        tree: JobTree,
        tree_execution_id: str,
        job_index: int,
        dev_client_id: Optional[str] = None
    ) -> str:
        """
        Execute a non-root job in a chain.

        Called by progress handler when previous job completes.

        Args:
            tree: Job tree containing the chain
            tree_execution_id: Execution ID (same for all jobs in chain)
            job_index: Index of job in tree.jobs[] to execute
            dev_client_id: If set, forward progress to this developer client

        Returns:
            job_execution_id of the new job

        Raises:
            Exception on execution failure
        """
        job = tree.jobs[job_index]
        prev_job = tree.jobs[job_index - 1]

        # Generate job_execution_id using same timestamp portion as tree_execution_id
        timestamp_part = tree_execution_id.split('-', 1)[1]  # YYYYMMDD-HHMMSS-uuid8
        job_execution_id = f"{job.id}-{timestamp_part}"

        # Set correlation IDs for logging
        set_correlation_ids(
            tree_exec_id=tree_execution_id,
            job_exec_id=job_execution_id
        )

        try:
            log_with_fields(
                logger, logging.INFO,
                f"Executing chain job {job_index + 1}/{len(tree.jobs)}",
                job_id=job.id,
                prev_job_id=prev_job.id,
                state="starting"
            )

            # Prepare inputs (may come from previous job's outputs)
            with log_duration("chain_input_preparation", logger, inputs=len(job.reads)):
                prepared_inputs = self.job_executor.prepare_chain_job_inputs(
                    job,
                    prev_job,
                    tree,
                    tree_execution_id
                )

            # Prepare outputs
            with log_duration("output_preparation", logger, outputs=len(job.writes)):
                prepared_outputs = self.job_executor.prepare_job_outputs(
                    job,
                    tree_execution_id
                )

            # Build job request
            job_request = self.job_executor.build_job_request(
                job,
                tree_execution_id,
                job_execution_id,
                prepared_inputs,
                prepared_outputs
            )

            # Find executor for job
            if self.executor_registry.is_stale():
                self.executor_registry.query_capabilities()

            executor_id = self.executor_registry.find_executor(job.id, job.version)

            # Try on-demand distribution if needed
            if not executor_id:
                log_with_fields(
                    logger, logging.INFO,
                    f"No executor has {job.id} cached, attempting on-demand distribution"
                )
                executor_id = self._distribute_job_on_demand(job.id, job.version)
                if not executor_id:
                    raise Exception(f"No pool executors available for {job.id}")

                if not self._wait_for_job_ready(executor_id, job.id, job.version, timeout=60):
                    raise Exception(f"Executor {executor_id} failed to install {job.id}")

            job_request['assigned_to'] = executor_id

            # Track job execution
            now = time.time()
            job_exec = JobExecution(
                job_execution_id=job_execution_id,
                tree_execution_id=tree_execution_id,
                job_id=job.id,
                job_version=job.version,
                state=JobState.QUEUED,
                executor_id=executor_id,
                assigned_at=now,
                timeout_at=now + job.run.timeout,
                last_progress=now,
                dev_client_id=dev_client_id,
                job_index=job_index,
            )
            self.job_tracker.add(job_exec)

            # Publish to MQTT
            log_with_fields(
                logger, logging.INFO,
                "Publishing chain job request to MQTT",
                state="publishing",
                executor=executor_id
            )
            self.mqtt_client.publish_job_request(job_execution_id, job_request)

            log_with_fields(
                logger, logging.INFO,
                f"Chain job {job_index + 1}/{len(tree.jobs)} scheduled",
                state="scheduled"
            )

            return job_execution_id

        except Exception as e:
            log_with_fields(
                logger, logging.ERROR,
                f"Chain job execution failed: {e}",
                state="failed",
                error=str(e)
            )
            raise
        finally:
            clear_correlation_ids()

    def execute_tree(self, tree: JobTree, dev_client_id: Optional[str] = None) -> str:
        """
        Execute a tree by publishing its root job.

        Args:
            tree: The tree to execute
            dev_client_id: If set, forward progress updates to this developer client

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

            # Update tree execution tracking immediately after lock registration
            # This prevents the scheduler from trying to run the same tree again
            # before it completes (avoiding self-conflict)
            now = time.time()
            tree.last_execution = now
            ideal_interval = (24 * 3600) / tree.max_daily
            tree.next_execution = now + ideal_interval
            tree.record_execution(now)

            log_with_fields(
                logger, logging.INFO, "Starting tree execution",
                job_id=root_job.id,
                state="starting"
            )

            # Prepare inputs with timing
            with log_duration("input_preparation", logger, inputs=len(root_job.reads)):
                prepared_inputs = self.job_executor.prepare_tree_inputs(
                    tree,
                    tree_execution_id
                )

            # Prepare outputs
            with log_duration("output_preparation", logger, outputs=len(root_job.writes)):
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

            # If no executor has job cached, try on-demand distribution
            if not executor_id:
                log_with_fields(
                    logger, logging.INFO,
                    f"No executor has {root_job.id} cached, attempting on-demand distribution",
                    job_id=root_job.id,
                    version=root_job.version
                )

                # Distribute to pool executor
                executor_id = self._distribute_job_on_demand(root_job.id, root_job.version)
                if not executor_id:
                    raise Exception(f"No pool executors available for {root_job.id} v{root_job.version}")

                # Wait for executor to install the job
                if not self._wait_for_job_ready(executor_id, root_job.id, root_job.version, timeout=60):
                    raise Exception(f"Executor {executor_id} failed to install {root_job.id} in time")

            # Add assigned_to field
            job_request['assigned_to'] = executor_id

            # Track job execution (reuse 'now' from tree tracking above)
            # Note: dev_client_id is set BEFORE MQTT publish to avoid race conditions
            # job_index=0 for root job (first in chain)
            job_exec = JobExecution(
                job_execution_id=job_execution_id,
                tree_execution_id=tree_execution_id,
                job_id=root_job.id,
                job_version=root_job.version,
                state=JobState.QUEUED,
                executor_id=executor_id,
                assigned_at=now,
                timeout_at=now + root_job.run.timeout,
                last_progress=now,
                dev_client_id=dev_client_id,
                job_index=0,  # Root job is at index 0 in tree.jobs[]
            )
            self.job_tracker.add(job_exec)

            # Publish to MQTT
            log_with_fields(
                logger, logging.INFO, "Publishing job request to MQTT",
                state="publishing",
                executor=executor_id
            )
            self.mqtt_client.publish_job_request(job_execution_id, job_request)

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

    def _compute_sha256(self, file_path: Path) -> str:
        """
        Compute SHA256 checksum of a file.

        Args:
            file_path: Path to file

        Returns:
            Hex-encoded SHA256 checksum
        """
        import hashlib

        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _read_manifest(self, package_path: Path) -> dict:
        """
        Extract and read manifest.yaml from .ljc package.

        Args:
            package_path: Path to .ljc file

        Returns:
            Manifest data as dictionary

        Raises:
            Exception: If manifest cannot be read
        """
        import tarfile
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir).resolve()

            # Extract manifest.yaml only with security validation
            from coordinator.archive_handler import safe_extract_member, ArchiveError

            with tarfile.open(package_path, 'r:gz') as tar:
                # Check if manifest exists
                try:
                    manifest_member = tar.getmember('manifest.yaml')
                except KeyError:
                    raise Exception(f"Package {package_path} missing manifest.yaml")

                # Use safe extraction
                safe_extract_member(tar, manifest_member, tmppath)

            # Read manifest
            manifest_file = tmppath / 'manifest.yaml'
            with open(manifest_file) as f:
                return yaml.safe_load(f)

    def _distribute_job_on_demand(self, job_id: str, version: str) -> Optional[str]:
        """
        Upload job package to MinIO and announce availability via MQTT.

        Args:
            job_id: Job identifier
            version: Job version

        Returns:
            Selected executor_id, or None if no pool executors available

        Raises:
            Exception: If distribution fails
        """
        # Find pool executors
        pool_executors = self.executor_registry.find_by_capability_type("pool")
        if not pool_executors:
            logger.error("No pool executors available for on-demand distribution")
            return None

        # Pick first available pool executor
        executor_id = pool_executors[0]
        logger.info(f"Distributing {job_id} v{version} on-demand to {executor_id}")

        # Check if cached package exists
        packages_dir = Path(self.config.jobs_dir).parent / 'packages'
        package_path = packages_dir / f"{job_id}.ljc"

        if not package_path.exists():
            raise Exception(f"Cached package not found: {package_path}")

        # Read manifest to get unpacked size
        try:
            manifest = self._read_manifest(package_path)
            unpacked_size = manifest.get('unpacked_size', {}).get('total_bytes', 0)
        except Exception as e:
            logger.warning(f"Could not read manifest, using default size: {e}")
            unpacked_size = 0

        # Compute checksum
        checksum = self._compute_sha256(package_path)
        logger.debug(f"Package checksum: {checksum}")

        # Upload to MinIO (temp storage, 24h TTL handled by cleanup)
        minio_bucket = self.config.minio.temp_bucket
        minio_key = f"temp/{job_id}-v{version}.ljc"

        logger.info(f"Uploading to MinIO: s3://{minio_bucket}/{minio_key}")
        self.minio_manager.upload_file(
            str(package_path),
            minio_bucket,
            minio_key,
            content_type="application/gzip"
        )

        # Generate pre-signed download URL (valid for 10 minutes)
        # Executor downloads immediately after receiving MQTT announcement
        download_url = self.minio_manager.generate_presigned_get_url(
            minio_bucket,
            minio_key,
            expires_seconds=600  # 10 minutes
        )
        logger.debug(f"Generated download URL (expires in 10 minutes)")

        # Announce job availability via MQTT
        announcement = {
            "job_id": job_id,
            "version": version,
            "capability": "pool",
            "script_artifact": {
                "uri": download_url,
                "checksum_sha256": checksum,
                "unpacked_size_bytes": unpacked_size
            }
        }

        logger.info(f"Publishing job announcement: linearjc/jobs/available")
        self.mqtt_client.publish_message(
            "linearjc/jobs/available",
            announcement,
            qos=1
        )

        logger.info(f"✓ Job {job_id} v{version} distributed to MinIO and announced")
        return executor_id

    def _wait_for_job_ready(
        self,
        executor_id: str,
        job_id: str,
        version: str,
        timeout: int = 60
    ) -> bool:
        """
        Wait for executor to install job after on-demand distribution.

        Args:
            executor_id: Executor to wait for
            job_id: Job identifier
            version: Job version
            timeout: Maximum wait time in seconds

        Returns:
            True if job installed in time, False otherwise
        """
        logger.info(f"Waiting for {executor_id} to install {job_id} v{version} (timeout: {timeout}s)")

        deadline = time.time() + timeout
        poll_interval = 2  # Query every 2 seconds

        while time.time() < deadline:
            # Refresh executor capabilities
            self.executor_registry.query_capabilities()

            # Check if executor has the job now
            if self.executor_registry.has_job(executor_id, job_id, version):
                logger.info(f"✓ Executor {executor_id} has installed {job_id} v{version}")
                return True

            # Wait before next check
            remaining = deadline - time.time()
            wait_time = min(poll_interval, remaining)
            if wait_time > 0:
                time.sleep(wait_time)

        logger.error(f"Timeout waiting for {executor_id} to install {job_id} v{version}")
        return False

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
            logger.info(f"  Reads: {list(prepared_inputs.keys())}")
            logger.info(f"  Writes: {list(prepared_outputs.keys())}")
            logger.info(f"  User: {root_job.run.user}")
            logger.info(f"  Timeout: {root_job.run.timeout}s")
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


@cli.command()
@click.argument('package', type=click.Path(exists=True))
@click.pass_context
def install(ctx, package):
    """Install a job package (.ljc file)."""
    import tarfile
    import tempfile
    import shutil
    from pathlib import Path

    config_path = ctx.obj['config_path']

    try:
        # Load config
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
        config_file = CoordinatorConfigFile(**config_data)
        config = config_file.coordinator

        click.echo(f"Installing package: {package}")

        # Create temp directory for extraction
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Extract package
            click.echo("  Extracting package...")
            with tarfile.open(package, 'r:gz') as tar:
                tar.extractall(tmppath)

            # Validate structure
            job_yaml = tmppath / 'job.yaml'
            script_sh = tmppath / 'script.sh'

            if not job_yaml.exists():
                click.echo("  ERROR: Package missing job.yaml", err=True)
                sys.exit(1)

            if not script_sh.exists():
                click.echo("  ERROR: Package missing script.sh", err=True)
                sys.exit(1)

            # Parse job to get job_id
            with open(job_yaml) as f:
                job_data = yaml.safe_load(f)

            if 'job' not in job_data or 'id' not in job_data['job']:
                click.echo("  ERROR: Invalid job.yaml structure", err=True)
                sys.exit(1)

            job_id = job_data['job']['id']
            click.echo(f"  Job ID: {job_id}")

            # Cache original .ljc package for on-demand distribution
            packages_dir = Path(config.jobs_dir).parent / 'packages'
            packages_dir.mkdir(exist_ok=True)
            dest_package = packages_dir / f"{job_id}.ljc"
            shutil.copy2(package, dest_package)
            click.echo(f"  Cached package: {dest_package}")

            # Copy job.yaml to jobs directory
            jobs_dir = Path(config.jobs_dir)
            dest_job = jobs_dir / f"{job_id}.yaml"

            click.echo(f"  Installing job definition: {dest_job}")
            shutil.copy2(job_yaml, dest_job)

            # Note: Registry entries are managed separately via 'ljc registry push'
            # (see SPEC.md). Packages do NOT contain registry definitions.

            # Send SIGHUP to reload coordinator (hot reload without restart)
            click.echo("")
            try:
                pid_file = Path('/var/run/coordinator.pid')
                if pid_file.exists():
                    with open(pid_file) as f:
                        pid = int(f.read().strip())
                    import os
                    os.kill(pid, signal.SIGHUP)
                    click.echo("✓ Package installed successfully")
                    click.echo("✓ Reloaded coordinator (SIGHUP)")
                else:
                    click.echo("✓ Package installed successfully")
                    click.echo("  Note: Coordinator not running (will load on next start)")
            except (FileNotFoundError, ProcessLookupError, PermissionError) as e:
                click.echo("✓ Package installed successfully")
                click.echo(f"  Note: Could not reload coordinator ({e})")

            click.echo("")
            click.echo("Next steps:")
            click.echo(f"  1. Deploy script to executor: {script_sh}")
            click.echo(f"     OR wait for Phase 3 on-demand distribution")
            click.echo(f"  2. Check job discovered: linearjc-coordinator status")

    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
