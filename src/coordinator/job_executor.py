"""
Job executor for LinearJC coordinator.

Orchestrates tree execution lifecycle:
- Prepare inputs (archive + upload)
- Build job request messages
- Collect outputs (download + extract)
- Cleanup temporary files
"""
import logging
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from coordinator.models import Job, JobTree, DataRegistry, DataRegistryEntry
from coordinator.minio_manager import MinioManager
from coordinator.archive_handler import create_archive, extract_archive
from coordinator.output_locks import OutputLockManager

logger = logging.getLogger(__name__)


class JobExecutorError(Exception):
    """Error during job execution."""
    pass


class JobExecutor:
    """
    Orchestrates tree execution lifecycle.

    Handles input preparation, output collection, and MQTT message building.
    """

    def __init__(
        self,
        minio_manager: MinioManager,
        data_registry: Dict[str, DataRegistryEntry],
        temp_bucket: str,
        output_lock_manager: OutputLockManager,
        archive_format: str = "tar.gz",
        work_dir: str = "/var/lib/linearjc/work"
    ):
        """
        Initialize job executor.

        Args:
            minio_manager: Minio manager instance
            data_registry: Data registry mapping
            temp_bucket: Minio bucket for temporary job data
            output_lock_manager: Manager for per-filesystem-path output locks
            archive_format: Archive format to use (from coordinator config)
            work_dir: Local working directory for temp files
        """
        self.minio = minio_manager
        self.data_registry = data_registry
        self.temp_bucket = temp_bucket
        self.output_lock_manager = output_lock_manager
        self.archive_format = archive_format
        self.work_dir = Path(work_dir)

        # Create work directory with secure permissions
        self.work_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.work_dir, 0o700)  # Owner-only access

        logger.info(f"Work directory: {self.work_dir}")

        # Clean up old orphaned execution directories on startup
        self._cleanup_orphaned_directories(age_hours=24)

    def generate_tree_execution_id(self, tree: JobTree) -> str:
        """
        Generate unique execution ID for a tree.

        Format: {root_job_id}-{timestamp}-{uuid}
        Example: hello.world-20251114-143022-a7b3c9d2

        Includes UUID component to prevent execution ID guessing attacks.

        Args:
            tree: Job tree

        Returns:
            Unique execution ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        unique_id = uuid.uuid4().hex[:8]  # First 8 chars of UUID
        return f"{tree.root.id}-{timestamp}-{unique_id}"

    def prepare_tree_inputs(
        self,
        tree: JobTree,
        tree_execution_id: str
    ) -> Dict[str, Dict[str, str]]:
        """
        Prepare inputs for root job of tree.

        For each input:
        1. Look up in data registry
        2. If filesystem: create archive → upload to Minio → generate GET URL
        3. If minio: generate GET URL directly (already in Minio)

        Args:
            tree: Job tree to prepare
            tree_execution_id: Unique execution ID

        Returns:
            Dict mapping input_name -> {url, method, format}

        Raises:
            JobExecutorError: If preparation fails
        """
        root_job = tree.root

        # Create secure execution directory with unique name
        # Using tempfile.mkdtemp ensures unpredictable names and secure permissions
        execution_dir = Path(tempfile.mkdtemp(
            prefix=f"{tree_execution_id}-",
            dir=self.work_dir
        ))
        os.chmod(execution_dir, 0o700)  # Ensure owner-only access

        logger.info(
            f"Preparing inputs for {root_job.id} "
            f"(execution: {tree_execution_id}, work dir: {execution_dir})"
        )

        prepared_inputs = {}

        try:
            for input_name, registry_key in root_job.inputs.items():
                if registry_key not in self.data_registry:
                    raise JobExecutorError(
                        f"Input '{input_name}' references unknown registry key: "
                        f"{registry_key}"
                    )

                registry_entry = self.data_registry[registry_key]

                logger.debug(
                    f"Preparing input '{input_name}' from registry '{registry_key}' "
                    f"(type: {registry_entry.type})"
                )

                if registry_entry.type == "filesystem":
                    prepared_inputs[input_name] = self._prepare_filesystem_input(
                        input_name,
                        registry_entry,
                        tree_execution_id,
                        execution_dir,
                        root_job
                    )

                elif registry_entry.type == "minio":
                    prepared_inputs[input_name] = self._prepare_minio_input(
                        input_name,
                        registry_entry,
                        root_job
                    )

                else:
                    raise JobExecutorError(
                        f"Unsupported registry type: {registry_entry.type}"
                    )

            logger.info(
                f"Prepared {len(prepared_inputs)} inputs for {root_job.id}"
            )

            return prepared_inputs

        except Exception as e:
            logger.error(f"Failed to prepare inputs: {e}")
            raise JobExecutorError(f"Input preparation failed: {e}")

    def _check_disk_space(self, source_path: str, safety_factor: float = 2.0) -> None:
        """
        Check if there's enough disk space to safely create archive.

        Args:
            source_path: Path to source file/directory
            safety_factor: Multiply estimated size by this factor (default: 2x for safety)

        Raises:
            JobExecutorError: If insufficient disk space
        """
        source = Path(source_path)

        # Calculate source size
        if source.is_file():
            source_size = source.stat().st_size
        elif source.is_dir():
            source_size = sum(
                f.stat().st_size
                for f in source.rglob('*')
                if f.is_file()
            )
        else:
            raise JobExecutorError(f"Source path is neither file nor directory: {source_path}")

        # Get available disk space in work_dir
        stat = os.statvfs(self.work_dir)
        available_space = stat.f_bavail * stat.f_frsize

        # Estimate required space (compressed archive is usually smaller, but be safe)
        # Factor accounts for: archive overhead, compression ratio variance, temp files
        required_space = int(source_size * safety_factor)

        logger.debug(
            f"Disk space check: source={source_size / 1024 / 1024:.1f}MB, "
            f"required={required_space / 1024 / 1024:.1f}MB, "
            f"available={available_space / 1024 / 1024:.1f}MB"
        )

        if available_space < required_space:
            raise JobExecutorError(
                f"Insufficient disk space in {self.work_dir}\n"
                f"Source size: {source_size / 1024 / 1024:.1f} MB\n"
                f"Required (with {safety_factor}x safety): {required_space / 1024 / 1024:.1f} MB\n"
                f"Available: {available_space / 1024 / 1024:.1f} MB\n"
                f"Free up at least {(required_space - available_space) / 1024 / 1024:.1f} MB"
            )

    def _prepare_filesystem_input(
        self,
        input_name: str,
        registry_entry: DataRegistryEntry,
        tree_execution_id: str,
        execution_dir: Path,
        job: Job
    ) -> Dict[str, str]:
        """
        Prepare input from filesystem: archive → upload → URL.

        Args:
            input_name: Logical input name
            registry_entry: Registry entry with filesystem path
            tree_execution_id: Execution ID
            execution_dir: Working directory
            job: Job configuration

        Returns:
            Dict with url, method, format
        """
        source_path = registry_entry.path

        # Check disk space before creating archive
        self._check_disk_space(source_path)

        # Create archive in temp directory (format from config)
        archive_filename = f"input_{input_name}.{self.archive_format}"
        archive_path = execution_dir / archive_filename

        logger.debug(f"Creating {self.archive_format} archive: {source_path} -> {archive_path}")
        create_archive(source_path, str(archive_path))

        # Upload to Minio
        minio_object_name = f"jobs/{tree_execution_id}/{archive_filename}"

        logger.debug(f"Uploading to Minio: {archive_path} -> {minio_object_name}")
        self.minio.upload_file(
            str(archive_path),
            self.temp_bucket,
            minio_object_name
        )

        # Generate pre-signed GET URL
        expires_seconds = job.executor.timeout + 3600  # timeout + 1 hour
        url = self.minio.generate_presigned_get_url(
            self.temp_bucket,
            minio_object_name,
            expires_seconds
        )

        return {
            "url": url,
            "method": "GET",
            "format": self.archive_format
        }

    def _prepare_minio_input(
        self,
        input_name: str,
        registry_entry: DataRegistryEntry,
        job: Job
    ) -> Dict[str, str]:
        """
        Prepare input from Minio: just generate URL.

        Args:
            input_name: Logical input name
            registry_entry: Registry entry with minio location
            job: Job configuration

        Returns:
            Dict with url, method, format
        """
        bucket = registry_entry.bucket
        # Minio registry entries use prefix as the object path
        object_name = registry_entry.prefix

        # Generate pre-signed GET URL
        expires_seconds = job.executor.timeout + 3600
        url = self.minio.generate_presigned_get_url(
            bucket,
            object_name,
            expires_seconds
        )

        return {
            "url": url,
            "method": "GET",
            "format": self.archive_format
        }

    def prepare_job_outputs(
        self,
        job: Job,
        tree_execution_id: str
    ) -> Dict[str, Dict[str, str]]:
        """
        Prepare output URLs for a job.

        Generates pre-signed PUT URLs for executor to upload outputs.

        Args:
            job: Job configuration
            tree_execution_id: Execution ID

        Returns:
            Dict mapping output_name -> {url, method, format}

        Raises:
            JobExecutorError: If preparation fails
        """
        logger.debug(f"Preparing output URLs for {job.id}")

        prepared_outputs = {}

        try:
            for output_name, registry_key in job.outputs.items():
                if registry_key not in self.data_registry:
                    raise JobExecutorError(
                        f"Output '{output_name}' references unknown registry key: "
                        f"{registry_key}"
                    )

                registry_entry = self.data_registry[registry_key]

                # Generate Minio object name for output (format from config)
                archive_filename = f"output_{output_name}.{self.archive_format}"
                minio_object_name = f"jobs/{tree_execution_id}/{archive_filename}"

                # Generate pre-signed PUT URL
                expires_seconds = job.executor.timeout + 3600
                url = self.minio.generate_presigned_put_url(
                    self.temp_bucket,
                    minio_object_name,
                    expires_seconds
                )

                prepared_outputs[output_name] = {
                    "url": url,
                    "method": "PUT",
                    "format": self.archive_format
                }

            logger.debug(f"Prepared {len(prepared_outputs)} output URLs")
            return prepared_outputs

        except Exception as e:
            logger.error(f"Failed to prepare outputs: {e}")
            raise JobExecutorError(f"Output preparation failed: {e}")

    def collect_tree_outputs(
        self,
        tree: JobTree,
        tree_execution_id: str
    ) -> None:
        """
        Collect outputs from leaf job of tree.

        For each output:
        1. Look up in data registry
        2. If filesystem: download from Minio → extract archive → write to path
        3. If minio: already in Minio, nothing to do

        Args:
            tree: Job tree
            tree_execution_id: Execution ID

        Raises:
            JobExecutorError: If collection fails
        """
        leaf_job = tree.jobs[-1]  # Last job in linear tree
        execution_dir = self.work_dir / tree_execution_id

        logger.info(
            f"Collecting outputs from {leaf_job.id} "
            f"(execution: {tree_execution_id})"
        )

        try:
            for output_name, registry_key in leaf_job.outputs.items():
                if registry_key not in self.data_registry:
                    raise JobExecutorError(
                        f"Output '{output_name}' references unknown registry key: "
                        f"{registry_key}"
                    )

                registry_entry = self.data_registry[registry_key]

                logger.debug(
                    f"Collecting output '{output_name}' to registry '{registry_key}' "
                    f"(type: {registry_entry.type})"
                )

                if registry_entry.type == "filesystem":
                    self._collect_filesystem_output(
                        output_name,
                        registry_entry,
                        tree_execution_id,
                        execution_dir
                    )

                elif registry_entry.type == "minio":
                    # Already in Minio, nothing to do
                    logger.debug(
                        f"Output '{output_name}' is minio type, "
                        f"already in bucket {registry_entry.bucket}"
                    )

            logger.info(
                f"Collected {len(leaf_job.outputs)} outputs from {leaf_job.id}"
            )

        except Exception as e:
            logger.error(f"Failed to collect outputs: {e}")
            raise JobExecutorError(f"Output collection failed: {e}")

    def _collect_filesystem_output(
        self,
        output_name: str,
        registry_entry: DataRegistryEntry,
        tree_execution_id: str,
        execution_dir: Path
    ) -> None:
        """
        Collect output to filesystem: download → extract → validate.

        Uses per-path locking to ensure atomic writes when multiple jobs
        write to the same destination.

        Args:
            output_name: Logical output name
            registry_entry: Registry entry with filesystem path and path_type
            tree_execution_id: Execution ID
            execution_dir: Working directory

        Raises:
            JobExecutorError: If extraction or validation fails
        """
        dest_path = registry_entry.path
        path_type = registry_entry.path_type or 'directory'  # Default for backward compatibility

        # Download archive from Minio (format from config)
        archive_filename = f"output_{output_name}.{self.archive_format}"
        archive_path = execution_dir / archive_filename
        minio_object_name = f"jobs/{tree_execution_id}/{archive_filename}"

        logger.debug(f"Downloading from Minio: {minio_object_name} -> {archive_path}")
        self.minio.download_file(
            self.temp_bucket,
            minio_object_name,
            str(archive_path)
        )

        # Extract archive to destination WITH LOCK
        # This ensures atomic writes when multiple jobs target same path
        logger.debug(f"Acquiring lock for filesystem path: {dest_path}")
        with self.output_lock_manager.acquire(dest_path):
            logger.debug(
                f"Extracting {self.archive_format} archive as {path_type}: "
                f"{archive_path} -> {dest_path}"
            )
            extract_archive(str(archive_path), dest_path, path_type=path_type)

            # Validate extraction result matches declared path_type
            dest = Path(dest_path)
            if path_type == 'file' and not dest.is_file():
                raise JobExecutorError(
                    f"Expected file at {dest_path} but found "
                    f"{'directory' if dest.is_dir() else 'nothing'}"
                )
            elif path_type == 'directory' and not dest.is_dir():
                raise JobExecutorError(
                    f"Expected directory at {dest_path} but found "
                    f"{'file' if dest.is_file() else 'nothing'}"
                )

        logger.info(f"Collected output '{output_name}' to {dest_path} ({path_type})")

    def build_job_request(
        self,
        job: Job,
        tree_execution_id: str,
        job_execution_id: str,
        inputs: Dict[str, Dict[str, str]],
        outputs: Dict[str, Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Build MQTT job request message.

        Args:
            job: Job configuration
            tree_execution_id: Tree execution ID
            job_execution_id: Job execution ID (unique per job)
            inputs: Prepared input URLs
            outputs: Prepared output URLs

        Returns:
            Job request message dict (ready for JSON serialization)
        """
        timestamp = datetime.utcnow().isoformat() + 'Z'

        request = {
            # Correlation IDs for tracing
            "tree_execution_id": tree_execution_id,
            "job_execution_id": job_execution_id,
            # Job details
            "job_id": job.id,
            "job_version": job.version,
            "inputs": inputs,
            "outputs": outputs,
            "executor": {
                "user": job.executor.user,
                "timeout": job.executor.timeout
            },
            "callback": f"linearjc/jobs/progress/{job_execution_id}",
            "timestamp": timestamp
            # Note: signature will be added later by MQTT client
        }

        return request

    def cleanup_execution(self, tree_execution_id: str) -> None:
        """
        Clean up temporary files for an execution.

        Args:
            tree_execution_id: Execution ID to clean up
        """
        execution_dir = self.work_dir / tree_execution_id

        if execution_dir.exists():
            logger.info(f"Cleaning up execution directory: {execution_dir}")
            shutil.rmtree(execution_dir)
        else:
            logger.debug(f"Execution directory does not exist: {execution_dir}")

    def cleanup_minio_execution(
        self,
        tree_execution_id: str,
        dry_run: bool = False
    ) -> int:
        """
        Clean up Minio objects for an execution.

        Args:
            tree_execution_id: Execution ID to clean up
            dry_run: If True, only log what would be deleted

        Returns:
            Number of objects deleted
        """
        prefix = f"jobs/{tree_execution_id}/"

        logger.info(
            f"Cleaning up Minio objects for execution: {tree_execution_id}"
        )

        return self.minio.cleanup_old_objects(
            self.temp_bucket,
            prefix,
            older_than_days=0,  # Delete regardless of age
            dry_run=dry_run
        )

    def _cleanup_orphaned_directories(self, age_hours: int = 24) -> None:
        """
        Clean up orphaned execution directories from work_dir.

        Removes directories older than age_hours that were left behind
        from crashed coordinator instances or failed executions.

        Args:
            age_hours: Delete directories older than this many hours
        """
        if not self.work_dir.exists():
            return

        logger.info(f"Cleaning up orphaned directories in {self.work_dir} (age > {age_hours}h)")

        now = time.time()
        cutoff_time = now - (age_hours * 3600)
        deleted_count = 0
        total_size = 0

        try:
            for item in self.work_dir.iterdir():
                if not item.is_dir():
                    continue

                # Check directory age
                mtime = item.stat().st_mtime
                age_hours_actual = (now - mtime) / 3600

                if mtime < cutoff_time:
                    # Calculate directory size
                    dir_size = sum(
                        f.stat().st_size
                        for f in item.rglob('*')
                        if f.is_file()
                    )

                    logger.info(
                        f"Deleting orphaned directory: {item.name} "
                        f"(age: {age_hours_actual:.1f}h, size: {dir_size / 1024 / 1024:.1f}MB)"
                    )

                    shutil.rmtree(item)
                    deleted_count += 1
                    total_size += dir_size

            if deleted_count > 0:
                logger.info(
                    f"Cleaned up {deleted_count} orphaned directories, "
                    f"freed {total_size / 1024 / 1024:.1f} MB"
                )
            else:
                logger.debug("No orphaned directories found")

        except Exception as e:
            logger.warning(f"Failed to clean up orphaned directories: {e}")

    def cleanup_work_directory(self, age_hours: int = 24, dry_run: bool = False) -> int:
        """
        Clean up old execution directories from work_dir.

        Public method for manual cleanup or scheduled cleanup tasks.

        Args:
            age_hours: Delete directories older than this many hours
            dry_run: If True, only log what would be deleted

        Returns:
            Number of directories deleted (or would be deleted if dry_run)
        """
        if not self.work_dir.exists():
            logger.info(f"Work directory does not exist: {self.work_dir}")
            return 0

        logger.info(
            f"Cleaning up work directory: {self.work_dir} "
            f"(age > {age_hours}h, dry_run={dry_run})"
        )

        now = time.time()
        cutoff_time = now - (age_hours * 3600)
        deleted_count = 0

        try:
            for item in self.work_dir.iterdir():
                if not item.is_dir():
                    continue

                mtime = item.stat().st_mtime
                age_hours_actual = (now - mtime) / 3600

                if mtime < cutoff_time:
                    dir_size = sum(
                        f.stat().st_size
                        for f in item.rglob('*')
                        if f.is_file()
                    )

                    if dry_run:
                        logger.info(
                            f"[DRY RUN] Would delete: {item.name} "
                            f"(age: {age_hours_actual:.1f}h, size: {dir_size / 1024 / 1024:.1f}MB)"
                        )
                    else:
                        logger.info(
                            f"Deleting: {item.name} "
                            f"(age: {age_hours_actual:.1f}h, size: {dir_size / 1024 / 1024:.1f}MB)"
                        )
                        shutil.rmtree(item)

                    deleted_count += 1

            return deleted_count

        except Exception as e:
            logger.error(f"Failed to clean up work directory: {e}")
            raise JobExecutorError(f"Work directory cleanup failed: {e}")
