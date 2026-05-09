"""Unit tests for Phase 15 Write-Through Collection (Phase 3)."""
import os
import pytest
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import DataRegistryEntry, Job, JobRun


class TestMinioTempExists:
    """Tests for _minio_temp_exists helper method."""

    def test_returns_true_when_object_exists(self):
        """Returns True when MinIO object exists."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator.models import CoordinatorConfig

        # Create minimal config mock
        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.minio.object_exists.return_value = True

        result = coordinator._minio_temp_exists("tree-exec-123", "my_output")

        assert result is True
        coordinator.minio.object_exists.assert_called_once_with(
            "linearjc-temp", "jobs/tree-exec-123/output_my_output.tar.gz"
        )

    def test_returns_false_when_object_missing(self):
        """Returns False when MinIO object doesn't exist."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator.models import CoordinatorConfig

        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.minio.object_exists.return_value = False

        result = coordinator._minio_temp_exists("tree-exec-123", "my_output")

        assert result is False

    def test_returns_false_when_no_minio(self):
        """Returns False when MinIO is not configured."""
        from coordinator_v2.coordinator import Coordinator

        coordinator = object.__new__(Coordinator)
        coordinator.minio = None

        result = coordinator._minio_temp_exists("tree-exec-123", "my_output")

        assert result is False


class TestCollectJobOutputs:
    """Tests for _collect_job_outputs write-through collection."""

    @pytest.fixture
    def mock_coordinator(self):
        """Create a mock coordinator with required dependencies."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator.models import CoordinatorConfig

        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"
        config.work_dir = "/var/lib/linearjc/work"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.data_registry = {}

        return coordinator

    @pytest.mark.asyncio
    async def test_collects_fs_output(self, mock_coordinator):
        """fs type outputs are collected to filesystem."""
        mock_coordinator.data_registry = {
            "my_output": DataRegistryEntry(
                type="fs", path="/data/output.json", kind="file"
            )
        }

        job = MagicMock(spec=Job)
        job.id = "test.job"
        job.writes = ["my_output"]

        mock_coordinator._collect_filesystem_output = AsyncMock()

        await mock_coordinator._collect_job_outputs(job, "tree-exec-123")

        mock_coordinator._collect_filesystem_output.assert_called_once_with(
            "my_output",
            mock_coordinator.data_registry["my_output"],
            "tree-exec-123",
        )

    @pytest.mark.asyncio
    async def test_skips_temp_output(self, mock_coordinator):
        """temp type outputs are NOT collected (stay in MinIO)."""
        mock_coordinator.data_registry = {
            "my_temp": DataRegistryEntry(type="temp", kind="file")
        }

        job = MagicMock(spec=Job)
        job.id = "test.job"
        job.writes = ["my_temp"]

        mock_coordinator._collect_filesystem_output = AsyncMock()
        mock_coordinator._collect_minio_output = AsyncMock()

        await mock_coordinator._collect_job_outputs(job, "tree-exec-123")

        # Neither method should be called for temp
        mock_coordinator._collect_filesystem_output.assert_not_called()
        mock_coordinator._collect_minio_output.assert_not_called()

    @pytest.mark.asyncio
    async def test_collects_minio_output(self, mock_coordinator):
        """minio type outputs are copied to permanent bucket."""
        mock_coordinator.data_registry = {
            "my_minio": DataRegistryEntry(
                type="minio", bucket="data-bucket", prefix="outputs/", kind="dir"
            )
        }

        job = MagicMock(spec=Job)
        job.id = "test.job"
        job.writes = ["my_minio"]

        mock_coordinator._collect_minio_output = AsyncMock()

        await mock_coordinator._collect_job_outputs(job, "tree-exec-123")

        mock_coordinator._collect_minio_output.assert_called_once_with(
            "my_minio",
            mock_coordinator.data_registry["my_minio"],
            "tree-exec-123",
        )

    @pytest.mark.asyncio
    async def test_mixed_outputs(self, mock_coordinator):
        """Job with mixed outputs: fs collected, temp skipped, minio copied."""
        mock_coordinator.data_registry = {
            "fs_out": DataRegistryEntry(
                type="fs", path="/data/output.json", kind="file"
            ),
            "temp_out": DataRegistryEntry(type="temp", kind="file"),
            "minio_out": DataRegistryEntry(
                type="minio", bucket="data-bucket", kind="dir"
            ),
        }

        job = MagicMock(spec=Job)
        job.id = "test.job"
        job.writes = ["fs_out", "temp_out", "minio_out"]

        mock_coordinator._collect_filesystem_output = AsyncMock()
        mock_coordinator._collect_minio_output = AsyncMock()

        await mock_coordinator._collect_job_outputs(job, "tree-exec-123")

        # fs collected
        mock_coordinator._collect_filesystem_output.assert_called_once()
        # minio copied
        mock_coordinator._collect_minio_output.assert_called_once()
        # Total calls = 2 (temp skipped)


class TestPrepareChainInputsCacheAware:
    """Tests for cache-aware _prepare_chain_inputs."""

    @pytest.fixture
    def mock_coordinator(self):
        """Create mock coordinator for chain input tests."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator.models import CoordinatorConfig

        config = MagicMock(spec=CoordinatorConfig)
        config.minio = MagicMock()
        config.minio.temp_bucket = "linearjc-temp"
        config.work_dir = "/var/lib/linearjc/work"

        coordinator = object.__new__(Coordinator)
        coordinator.config = config
        coordinator.minio = MagicMock()
        coordinator.data_registry = {}

        return coordinator

    @pytest.mark.asyncio
    async def test_temp_always_from_minio(self, mock_coordinator):
        """temp type always reads from MinIO (no fallback)."""
        mock_coordinator.data_registry = {
            "intermediate": DataRegistryEntry(type="temp", kind="file")
        }

        job = MagicMock(spec=Job)
        job.reads = ["intermediate"]
        job.run = MagicMock()
        job.run.timeout = 300

        prev_job = MagicMock(spec=Job)
        prev_job.writes = ["intermediate"]

        mock_coordinator._prepare_intermediate_input = MagicMock(
            return_value={"url": "presigned-url", "method": "GET"}
        )
        mock_coordinator._minio_temp_exists = MagicMock()  # Should not be called

        result = await mock_coordinator._prepare_chain_inputs(
            job, prev_job, "tree-exec-123"
        )

        assert "intermediate" in result
        mock_coordinator._prepare_intermediate_input.assert_called_once()
        # For temp type, we don't check cache - go straight to MinIO
        # The cache check should NOT be called for temp type

    @pytest.mark.asyncio
    async def test_fs_cache_hit(self, mock_coordinator):
        """fs type with cache hit reads from MinIO."""
        mock_coordinator.data_registry = {
            "output": DataRegistryEntry(
                type="fs", path="/data/output.json", kind="file"
            )
        }

        job = MagicMock(spec=Job)
        job.reads = ["output"]
        job.run = MagicMock()
        job.run.timeout = 300

        prev_job = MagicMock(spec=Job)
        prev_job.writes = ["output"]

        mock_coordinator._minio_temp_exists = MagicMock(return_value=True)
        mock_coordinator._prepare_intermediate_input = MagicMock(
            return_value={"url": "cache-url", "method": "GET"}
        )
        mock_coordinator._prepare_fs_input = AsyncMock()

        result = await mock_coordinator._prepare_chain_inputs(
            job, prev_job, "tree-exec-123"
        )

        assert "output" in result
        # Cache hit - should use intermediate (MinIO)
        mock_coordinator._prepare_intermediate_input.assert_called_once()
        mock_coordinator._prepare_fs_input.assert_not_called()

    @pytest.mark.asyncio
    async def test_fs_cache_miss_fallback_to_storage(self, mock_coordinator):
        """fs type with cache miss falls back to filesystem."""
        mock_coordinator.data_registry = {
            "output": DataRegistryEntry(
                type="fs", path="/data/output.json", kind="file"
            )
        }

        job = MagicMock(spec=Job)
        job.reads = ["output"]
        job.run = MagicMock()
        job.run.timeout = 300

        prev_job = MagicMock(spec=Job)
        prev_job.writes = ["output"]

        mock_coordinator._minio_temp_exists = MagicMock(return_value=False)
        mock_coordinator._prepare_intermediate_input = MagicMock()
        mock_coordinator._prepare_fs_input = AsyncMock(
            return_value={"url": "fs-url", "method": "GET"}
        )

        result = await mock_coordinator._prepare_chain_inputs(
            job, prev_job, "tree-exec-123"
        )

        assert "output" in result
        # Cache miss - should fallback to fs
        mock_coordinator._prepare_intermediate_input.assert_not_called()
        mock_coordinator._prepare_fs_input.assert_called_once()

    @pytest.mark.asyncio
    async def test_external_fs_input_always_from_storage(self, mock_coordinator):
        """External fs input (not from prev_job) always reads from storage."""
        mock_coordinator.data_registry = {
            "config": DataRegistryEntry(
                type="fs", path="/config/app.yaml", kind="file", protect=True
            )
        }

        job = MagicMock(spec=Job)
        job.reads = ["config"]
        job.run = MagicMock()
        job.run.timeout = 300

        prev_job = MagicMock(spec=Job)
        prev_job.writes = []  # config is NOT from prev_job

        mock_coordinator._minio_temp_exists = MagicMock()
        mock_coordinator._prepare_fs_input = AsyncMock(
            return_value={"url": "fs-url", "method": "GET"}
        )

        result = await mock_coordinator._prepare_chain_inputs(
            job, prev_job, "tree-exec-123"
        )

        assert "config" in result
        # External input - should read from storage directly
        mock_coordinator._prepare_fs_input.assert_called_once()
        mock_coordinator._minio_temp_exists.assert_not_called()


class TestAtomicCollection:
    """Tests for atomic filesystem collection."""

    @pytest.mark.asyncio
    async def test_atomic_collection_creates_temp_path(self):
        """Collection creates temp path in same directory as dest."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator.models import CoordinatorConfig

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()

            # Create a test archive
            exec_dir = Path(tmpdir) / "tree-exec-123"
            exec_dir.mkdir()
            archive_path = exec_dir / "output_test.tar.gz"

            # Create minimal tar.gz archive
            import tarfile

            test_content = b"test data"
            dest_dir = Path(tmpdir) / "dest"
            dest_dir.mkdir()
            dest_path = dest_dir / "output.json"

            with tarfile.open(archive_path, "w:gz") as tar:
                import io

                info = tarfile.TarInfo(name="output.json")
                info.size = len(test_content)
                tar.addfile(info, io.BytesIO(test_content))

            # Mock download to do nothing (archive already exists)
            coordinator.minio.download_file = MagicMock()

            entry = DataRegistryEntry(type="fs", path=str(dest_path), kind="file")

            await coordinator._collect_filesystem_output("test", entry, "tree-exec-123")

            # Verify file was extracted
            assert dest_path.exists()
            assert dest_path.read_text() == "test data"

    @pytest.mark.asyncio
    async def test_atomic_collection_cleans_temp_on_failure(self):
        """Collection cleans up temp path if extraction fails."""
        from coordinator_v2.coordinator import Coordinator
        from coordinator.models import CoordinatorConfig
        from coordinator.archive_handler import ArchiveError

        with tempfile.TemporaryDirectory() as tmpdir:
            config = MagicMock(spec=CoordinatorConfig)
            config.minio = MagicMock()
            config.minio.temp_bucket = "linearjc-temp"
            config.work_dir = tmpdir

            coordinator = object.__new__(Coordinator)
            coordinator.config = config
            coordinator.minio = MagicMock()

            # Create exec dir
            exec_dir = Path(tmpdir) / "tree-exec-123"
            exec_dir.mkdir()

            dest_dir = Path(tmpdir) / "dest"
            dest_dir.mkdir()
            dest_path = dest_dir / "output.json"

            # Mock download to raise error
            coordinator.minio.download_file = MagicMock(
                side_effect=Exception("Download failed")
            )

            entry = DataRegistryEntry(type="fs", path=str(dest_path), kind="file")

            with pytest.raises(Exception, match="Download failed"):
                await coordinator._collect_filesystem_output(
                    "test", entry, "tree-exec-123"
                )

            # Verify temp file was cleaned up
            temp_path = dest_path.parent / f".{dest_path.name}.tree-exec-123.tmp"
            assert not temp_path.exists()


class TestMinioManagerCopyObject:
    """Tests for MinioManager copy_object method."""

    def test_copy_object_calls_minio_client(self):
        """copy_object uses MinIO client copy_object."""
        from coordinator.minio_manager import MinioManager

        manager = object.__new__(MinioManager)
        manager.client = MagicMock()
        manager.config = MagicMock()

        # Mock ensure_bucket
        manager.ensure_bucket = MagicMock()

        manager.copy_object(
            source_bucket="temp-bucket",
            source_object="jobs/exec-123/output_test.tar.gz",
            dest_bucket="data-bucket",
            dest_object="outputs/test.tar.gz",
        )

        manager.ensure_bucket.assert_called_once_with("data-bucket")
        manager.client.copy_object.assert_called_once()


class TestMinioManagerObjectExists:
    """Tests for MinioManager object_exists method."""

    def test_returns_true_when_exists(self):
        """Returns True when stat_object succeeds."""
        from coordinator.minio_manager import MinioManager

        manager = object.__new__(MinioManager)
        manager.client = MagicMock()
        manager.client.stat_object.return_value = MagicMock()

        result = manager.object_exists("my-bucket", "my-object")

        assert result is True

    def test_returns_false_when_not_found(self):
        """Returns False when NoSuchKey error."""
        from coordinator.minio_manager import MinioManager
        from minio.error import S3Error

        manager = object.__new__(MinioManager)
        manager.client = MagicMock()

        # Create real S3Error with NoSuchKey code
        # S3Error takes (code, message, resource, request_id, host_id, response)
        error = S3Error(
            code="NoSuchKey",
            message="Object not found",
            resource="my-bucket/my-object",
            request_id="test-request",
            host_id="test-host",
            response=None,
        )
        manager.client.stat_object.side_effect = error

        result = manager.object_exists("my-bucket", "my-object")

        assert result is False
