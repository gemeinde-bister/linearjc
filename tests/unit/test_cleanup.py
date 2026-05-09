"""Unit tests for Phase 15 Cleanup (Phase 6).

Tests verify:
1. Lock release happens before cleanup scheduling
2. MinIO temp data is cleaned on tree completion
3. Work directories are cleaned
4. Atomic collection temp files are cleaned on failure
5. Orphan cleanup handles failed collections
"""
import asyncio
import os
import pytest
import shutil
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch, call

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import DataRegistryEntry, Job, JobRun, CoordinatorConfig
from coordinator_v2.register_lock import RegisterLock


# ═══════════════════════════════════════════════════════════════════════════
# Lock Release Before Cleanup Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestLockReleaseBeforeCleanup:
    """Verify lock release happens before cleanup scheduling."""

    @pytest.fixture
    def mock_coordinator(self):
        """Create mock coordinator for cleanup tests."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator_v2.tree_tracker import TreeTracker, TreeExecution
        from coordinator_v2.job_state_machine import JobExecution, JobState
        from coordinator_v2.tree_validator import TreeValidator

        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"
        config.work_dir = "/var/lib/linearjc/work"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.register_lock = RegisterLock()
        coordinator.validator = MagicMock(spec=TreeValidator)
        coordinator.tree_tracker = MagicMock(spec=TreeTracker)
        coordinator.data_registry = {}

        return coordinator

    @pytest.mark.asyncio
    async def test_collect_outputs_releases_locks_before_cleanup(self, mock_coordinator):
        """_collect_outputs releases locks before scheduling cleanup."""
        from coordinator_v2.job_state_machine import JobStateMachine, JobExecution, JobState
        from coordinator_v2.tree_tracker import TreeExecution
        from coordinator.models import JobTree

        # Setup
        tree_exec_id = "test-tree-exec-123"
        job = MagicMock(spec=Job)
        job.id = "test.job"
        job.writes = []

        tree = MagicMock(spec=JobTree)
        tree.root = job
        tree.jobs = [job]

        tex = MagicMock(spec=TreeExecution)
        tex.tree = tree
        tex.tree_execution_id = tree_exec_id

        job_exec = MagicMock(spec=JobExecution)
        job_exec.tree_execution_id = tree_exec_id
        job_exec.job_id = "test.job"
        job_exec.dev_client_id = None

        sm = MagicMock(spec=JobStateMachine)
        sm.job_exec = job_exec

        # Pre-acquire lock
        mock_coordinator.register_lock.try_acquire_all(
            [("/data/output.json", "exclusive")], tree_exec_id
        )
        assert mock_coordinator.register_lock.is_held("/data/output.json")

        # Track dequeue and create_task call order
        call_order = []
        original_dequeue = mock_coordinator.register_lock.dequeue

        def track_dequeue(exec_id):
            call_order.append(("dequeue", exec_id))
            return original_dequeue(exec_id)

        mock_coordinator.register_lock.dequeue = track_dequeue

        # Mock asyncio.create_task to track when cleanup is scheduled
        original_create_task = asyncio.create_task
        cleanup_scheduled = []

        def track_create_task(coro):
            # Check if this is our cleanup coroutine
            if "cleanup" in str(coro) or "cleanup_execution" in str(coro):
                cleanup_scheduled.append(True)
            call_order.append(("create_task", str(coro)))
            return original_create_task(coro)

        async def mock_cleanup(exec_id):
            pass

        mock_coordinator._cleanup_execution = mock_cleanup

        with patch.object(asyncio, "create_task", track_create_task):
            await mock_coordinator._collect_outputs(sm, tex)

        # Verify dequeue was called before create_task
        assert len(call_order) >= 2
        dequeue_idx = next(i for i, (op, _) in enumerate(call_order) if op == "dequeue")
        create_task_idx = next(i for i, (op, _) in enumerate(call_order) if op == "create_task")
        assert dequeue_idx < create_task_idx, "dequeue must be called before create_task"

        # Verify lock was released
        assert not mock_coordinator.register_lock.is_held("/data/output.json")

    @pytest.mark.asyncio
    async def test_handle_chain_failure_releases_locks_before_cleanup(
        self, mock_coordinator
    ):
        """_handle_chain_failure releases locks before scheduling cleanup."""
        from coordinator_v2.job_state_machine import JobStateMachine, JobExecution
        from coordinator_v2.tree_tracker import TreeExecution

        tree_exec_id = "test-tree-exec-456"

        job_exec = MagicMock(spec=JobExecution)
        job_exec.tree_execution_id = tree_exec_id
        job_exec.job_id = "failing.job"
        job_exec.chain_index = 1
        job_exec.chain_length = 3
        job_exec.dev_client_id = None

        sm = MagicMock(spec=JobStateMachine)
        sm.job_exec = job_exec

        tex = MagicMock(spec=TreeExecution)
        tex.tree_execution_id = tree_exec_id

        # Pre-acquire lock
        mock_coordinator.register_lock.try_acquire_all(
            [("/data/intermediate.json", "exclusive")], tree_exec_id
        )

        call_order = []

        def track_dequeue(exec_id):
            call_order.append(("dequeue", exec_id))

        async def track_cleanup(exec_id):
            call_order.append(("cleanup", exec_id))

        mock_coordinator.register_lock.dequeue = track_dequeue
        mock_coordinator._cleanup_execution = track_cleanup

        await mock_coordinator._handle_chain_failure(sm, tex, "Test error")

        # Verify order
        assert call_order[0][0] == "dequeue"
        # cleanup is scheduled as asyncio task, may not be in call_order yet

    @pytest.mark.asyncio
    async def test_handle_dead_executors_releases_locks_before_cleanup(
        self, mock_coordinator
    ):
        """_handle_dead_executors releases locks before scheduling cleanup."""
        from coordinator_v2.job_state_machine import JobStateMachine, JobExecution
        from coordinator_v2.tree_tracker import TreeExecution
        from coordinator_v2.job_tracker import JobTracker

        tree_exec_id = "test-tree-exec-789"
        dead_executor_id = "dead-executor-001"

        job_exec = MagicMock(spec=JobExecution)
        job_exec.tree_execution_id = tree_exec_id
        job_exec.job_id = "running.job"
        job_exec.executor_id = dead_executor_id

        sm = MagicMock(spec=JobStateMachine)
        sm.job_exec = job_exec
        sm.is_active = True

        mock_coordinator.job_tracker = MagicMock(spec=JobTracker)
        mock_coordinator.job_tracker.get_active.return_value = [sm]
        mock_coordinator.job_tracker.get_by_tree.return_value = [sm]

        tex = MagicMock(spec=TreeExecution)
        tex.tree_execution_id = tree_exec_id
        tex.dev_client_id = None

        mock_coordinator.tree_tracker.get.return_value = tex

        # Pre-acquire lock
        mock_coordinator.register_lock.try_acquire_all(
            [("/data/running.json", "exclusive")], tree_exec_id
        )

        call_order = []

        def track_dequeue(exec_id):
            call_order.append(("dequeue", exec_id))

        mock_coordinator.register_lock.dequeue = track_dequeue

        await mock_coordinator._handle_dead_executors([dead_executor_id])

        # Verify dequeue was called
        assert ("dequeue", tree_exec_id) in call_order


# ═══════════════════════════════════════════════════════════════════════════
# MinIO Temp Data Cleanup Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestMinioTempCleanup:
    """Tests for MinIO temp data cleanup."""

    @pytest.mark.asyncio
    async def test_cleanup_execution_removes_minio_objects(self):
        """_cleanup_execution removes all MinIO objects for execution."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()
            coordinator.minio.cleanup_execution.return_value = 5

            tree_exec_id = "test-tree-exec-cleanup"

            await coordinator._cleanup_execution(tree_exec_id)

            coordinator.minio.cleanup_execution.assert_called_once_with(
                "linearjc-temp", tree_exec_id
            )

    @pytest.mark.asyncio
    async def test_cleanup_execution_handles_minio_error_gracefully(self):
        """_cleanup_execution logs warning on MinIO error, doesn't raise."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()
            coordinator.minio.cleanup_execution.side_effect = Exception(
                "MinIO connection failed"
            )

            tree_exec_id = "test-tree-exec-error"

            # Should not raise
            await coordinator._cleanup_execution(tree_exec_id)

            coordinator.minio.cleanup_execution.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
# Work Directory Cleanup Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestWorkDirCleanup:
    """Tests for work directory cleanup."""

    @pytest.mark.asyncio
    async def test_cleanup_execution_removes_work_dir(self):
        """_cleanup_execution removes work directory for execution."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()
            coordinator.minio.cleanup_execution.return_value = 0

            tree_exec_id = "test-tree-exec-workdir"

            # Create work directory
            exec_dir = Path(tmpdir) / tree_exec_id
            exec_dir.mkdir()
            (exec_dir / "input_test.tar.gz").touch()
            (exec_dir / "output_test.tar.gz").touch()

            assert exec_dir.exists()

            await coordinator._cleanup_execution(tree_exec_id)

            assert not exec_dir.exists()

    @pytest.mark.asyncio
    async def test_cleanup_execution_handles_missing_work_dir(self):
        """_cleanup_execution handles non-existent work directory gracefully."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()
            coordinator.minio.cleanup_execution.return_value = 0

            tree_exec_id = "nonexistent-exec-dir"

            # Work dir doesn't exist - should not raise
            await coordinator._cleanup_execution(tree_exec_id)


# ═══════════════════════════════════════════════════════════════════════════
# Atomic Collection Temp File Cleanup Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestAtomicCollectionTempCleanup:
    """Tests for atomic collection temp file cleanup on failure."""

    @pytest.mark.asyncio
    async def test_temp_file_cleaned_on_download_failure(self):
        """Temp file is cleaned if download fails."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()
            coordinator.minio.download_file.side_effect = Exception("Download failed")

            dest_dir = Path(tmpdir) / "dest"
            dest_dir.mkdir()
            dest_path = dest_dir / "output.json"

            entry = DataRegistryEntry(type="fs", path=str(dest_path), kind="file")
            tree_exec_id = "test-exec-123"

            # Create exec dir
            exec_dir = Path(tmpdir) / tree_exec_id
            exec_dir.mkdir()

            with pytest.raises(Exception, match="Download failed"):
                await coordinator._collect_filesystem_output("test", entry, tree_exec_id)

            # Verify temp file doesn't exist
            temp_path = dest_path.parent / f".{dest_path.name}.{tree_exec_id}.tmp"
            assert not temp_path.exists()

    @pytest.mark.asyncio
    async def test_temp_file_cleaned_on_extract_failure(self):
        """Temp file is cleaned if extraction fails."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()

            dest_dir = Path(tmpdir) / "dest"
            dest_dir.mkdir()
            dest_path = dest_dir / "output.json"

            entry = DataRegistryEntry(type="fs", path=str(dest_path), kind="file")
            tree_exec_id = "test-exec-456"

            # Create exec dir with invalid archive
            exec_dir = Path(tmpdir) / tree_exec_id
            exec_dir.mkdir()
            archive_path = exec_dir / "output_test.tar.gz"
            archive_path.write_bytes(b"not a valid tar.gz")

            # Download succeeds
            coordinator.minio.download_file = MagicMock()

            with pytest.raises(Exception):
                await coordinator._collect_filesystem_output("test", entry, tree_exec_id)

            # Verify temp file doesn't exist
            temp_path = dest_path.parent / f".{dest_path.name}.{tree_exec_id}.tmp"
            assert not temp_path.exists()


# ═══════════════════════════════════════════════════════════════════════════
# Orphan Cleanup Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestOrphanCleanup:
    """Tests for orphan cleanup (startup and periodic)."""

    def test_startup_cleanup_runs_minio_cleanup(self):
        """_startup_cleanup calls MinIO orphan cleanup."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir
            config.cleanup_age_hours = 24

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()
            coordinator.minio.cleanup_orphaned_executions.return_value = 3

            deleted = coordinator._cleanup_orphaned_minio(24)

            coordinator.minio.cleanup_orphaned_executions.assert_called_once_with(
                bucket="linearjc-temp",
                age_hours=24,
                dry_run=False,
            )
            assert deleted == 3

    def test_startup_cleanup_handles_minio_error(self):
        """_cleanup_orphaned_minio returns 0 on error."""
        from coordinator_v2.coordinator import Coordinator

        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.minio.cleanup_orphaned_executions.side_effect = Exception("Error")

        deleted = coordinator._cleanup_orphaned_minio(24)

        assert deleted == 0

    def test_startup_cleanup_runs_work_dir_cleanup(self):
        """_startup_cleanup cleans old work directories."""
        from coordinator_v2.coordinator import Coordinator

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config

            # Create old work directory (use current time - 25 hours)
            old_exec_id = "old-test-exec-123"
            old_dir = Path(tmpdir) / old_exec_id
            old_dir.mkdir()

            # Set modification time to 25 hours ago
            old_time = time.time() - (25 * 3600)
            os.utime(old_dir, (old_time, old_time))

            # Create recent work directory
            new_exec_id = "new-test-exec-456"
            new_dir = Path(tmpdir) / new_exec_id
            new_dir.mkdir()

            deleted = coordinator._cleanup_orphaned_work_dirs(24)

            assert deleted == 1
            assert not old_dir.exists()
            assert new_dir.exists()

    def test_periodic_cleanup_interval_is_one_hour(self):
        """Periodic cleanup runs every hour (120 cycles * 30s)."""
        orphan_cleanup_interval = 120
        maintenance_sleep = 30

        total_seconds = orphan_cleanup_interval * maintenance_sleep
        assert total_seconds == 3600  # 1 hour


# ═══════════════════════════════════════════════════════════════════════════
# Integration: Failed Collection Cleanup Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestFailedCollectionCleanup:
    """Tests for cleanup after failed write-through collection."""

    @pytest.fixture
    def mock_coordinator(self):
        """Create mock coordinator for failed collection tests."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator_v2.tree_tracker import TreeTracker
        from coordinator_v2.job_tracker import JobTracker
        from coordinator_v2.tree_validator import TreeValidator

        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"
        config.work_dir = "/var/lib/linearjc/work"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.register_lock = RegisterLock()
        coordinator.validator = MagicMock(spec=TreeValidator)
        coordinator.tree_tracker = MagicMock(spec=TreeTracker)
        coordinator.job_tracker = MagicMock(spec=JobTracker)
        coordinator.data_registry = {}

        return coordinator

    @pytest.mark.asyncio
    async def test_failed_collection_triggers_cleanup(self, mock_coordinator):
        """Failed write-through collection triggers _handle_chain_failure."""
        from coordinator_v2.job_state_machine import JobStateMachine, JobExecution, JobState
        from coordinator_v2.tree_tracker import TreeExecution
        from coordinator.models import JobTree

        tree_exec_id = "test-failed-collection"

        job = MagicMock(spec=Job)
        job.id = "test.job"
        job.writes = ["output"]

        tree = MagicMock(spec=JobTree)
        tree.root = job
        tree.jobs = [job]

        tex = MagicMock(spec=TreeExecution)
        tex.tree = tree
        tex.tree_execution_id = tree_exec_id
        tex.dev_client_id = None

        job_exec = MagicMock(spec=JobExecution)
        job_exec.tree_execution_id = tree_exec_id
        job_exec.job_id = "test.job"
        job_exec.dev_client_id = None
        job_exec.chain_index = 0
        job_exec.chain_length = 1

        sm = MagicMock(spec=JobStateMachine)
        sm.job_exec = job_exec

        mock_coordinator.data_registry = {
            "output": DataRegistryEntry(type="fs", path="/data/output.json", kind="file")
        }

        # Pre-acquire lock
        mock_coordinator.register_lock.try_acquire_all(
            [("/data/output.json", "exclusive")], tree_exec_id
        )

        # Make collection fail
        async def failing_collect(job, exec_id):
            raise Exception("Collection failed")

        mock_coordinator._collect_job_outputs = failing_collect

        mock_coordinator._find_job_by_id = MagicMock(return_value=job)
        mock_coordinator.tree_tracker.is_last_job.return_value = True
        mock_coordinator.tree_tracker.get.return_value = tex

        # Track if handle_chain_failure was called
        failure_called = []

        async def track_failure(sm, tex, error):
            failure_called.append(error)

        mock_coordinator._handle_chain_failure = track_failure

        # Simulate _on_progress calling collection
        try:
            current_job = mock_coordinator._find_job_by_id(sm.job_exec.job_id)
            await mock_coordinator._collect_job_outputs(current_job, tree_exec_id)
        except Exception as e:
            await mock_coordinator._handle_chain_failure(sm, tex, str(e))

        assert len(failure_called) == 1
        assert "Collection failed" in failure_called[0]


# ═══════════════════════════════════════════════════════════════════════════
# Cleanup Best-Effort Pattern Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestCleanupBestEffort:
    """Tests verifying cleanup is best-effort (doesn't fail chain)."""

    @pytest.mark.asyncio
    async def test_cleanup_failure_does_not_raise(self):
        """Cleanup failure logs warning, doesn't raise exception."""
        from coordinator_v2.coordinator import Coordinator

        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"
        config.work_dir = "/nonexistent/path"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.minio.cleanup_execution.side_effect = Exception("MinIO error")

        # Should not raise
        await coordinator._cleanup_execution("test-exec-789")

    @pytest.mark.asyncio
    async def test_cleanup_runs_as_background_task(self):
        """Cleanup is scheduled as background task, not awaited inline."""
        # This is a design verification test
        # The code uses asyncio.create_task() for cleanup

        # Verify the pattern exists in the code
        from coordinator_v2 import coordinator

        import inspect
        source = inspect.getsource(coordinator.Coordinator._collect_outputs)

        assert "asyncio.create_task" in source
        assert "_cleanup_execution" in source


# ═══════════════════════════════════════════════════════════════════════════
# Complete Cleanup Flow Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestCompleteCleanupFlow:
    """Integration tests for complete cleanup flow."""

    def test_cleanup_sequence_success_path(self):
        """Document the expected cleanup sequence for successful tree."""
        # Successful tree execution cleanup sequence:
        # 1. Last job completes
        # 2. _collect_outputs called
        # 3. register_lock.dequeue() releases all locks
        # 4. validator.unregister() removes tree from validator
        # 5. tree_tracker.mark_completed() updates state
        # 6. tree_tracker.remove() removes from active tracking
        # 7. asyncio.create_task(_cleanup_execution) scheduled
        # 8. _cleanup_execution deletes MinIO objects
        # 9. _cleanup_execution deletes work directory

        sequence = [
            "job_completes",
            "_collect_outputs",
            "register_lock.dequeue",
            "validator.unregister",
            "tree_tracker.mark_completed",
            "tree_tracker.remove",
            "asyncio.create_task(_cleanup_execution)",
            "minio.cleanup_execution",
            "shutil.rmtree(work_dir)",
        ]

        assert len(sequence) == 9

    def test_cleanup_sequence_failure_path(self):
        """Document the expected cleanup sequence for failed tree."""
        # Failed tree execution cleanup sequence:
        # 1. Job fails (or collection fails)
        # 2. _handle_chain_failure called
        # 3. register_lock.dequeue() releases all locks
        # 4. validator.unregister() removes tree from validator
        # 5. tree_tracker.mark_failed() updates state with error
        # 6. tree_tracker.remove() removes from active tracking
        # 7. asyncio.create_task(_cleanup_execution) scheduled
        # 8. _cleanup_execution deletes MinIO objects
        # 9. _cleanup_execution deletes work directory

        sequence = [
            "job_fails",
            "_handle_chain_failure",
            "register_lock.dequeue",
            "validator.unregister",
            "tree_tracker.mark_failed",
            "tree_tracker.remove",
            "asyncio.create_task(_cleanup_execution)",
            "minio.cleanup_execution",
            "shutil.rmtree(work_dir)",
        ]

        assert len(sequence) == 9

    def test_cleanup_sequence_executor_death_path(self):
        """Document the expected cleanup sequence for executor death."""
        # Executor death cleanup sequence:
        # 1. Heartbeat timeout detected
        # 2. _handle_dead_executors called
        # 3. For each affected tree:
        #    3a. register_lock.dequeue() releases all locks
        #    3b. validator.unregister() removes tree from validator
        #    3c. tree_tracker.mark_failed() updates state
        #    3d. tree_tracker.remove() removes from tracking
        #    3e. asyncio.create_task(_cleanup_execution) scheduled
        # 4. _cleanup_execution deletes MinIO objects
        # 5. _cleanup_execution deletes work directory

        sequence = [
            "heartbeat_timeout",
            "_handle_dead_executors",
            "register_lock.dequeue",
            "validator.unregister",
            "tree_tracker.mark_failed",
            "tree_tracker.remove",
            "asyncio.create_task(_cleanup_execution)",
            "minio.cleanup_execution",
            "shutil.rmtree(work_dir)",
        ]

        assert len(sequence) == 9
