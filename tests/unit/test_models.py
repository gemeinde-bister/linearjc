"""Unit tests for LinearJC coordinator models (SPEC.md v0.8.0 format)."""
import pytest
import sys
from pathlib import Path
from pydantic import ValidationError

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import (
    Job, JobFile, JobRun, JobSchedule, ResourceLimits,
    DataRegistryEntry, DataRegistry, JobTree
)


class TestJobRun:
    """Tests for JobRun model (run: section)."""

    def test_minimal_job_run(self):
        """JobRun with only required fields."""
        run = JobRun(user="nobody")
        assert run.user == "nobody"
        assert run.timeout == 3600  # default
        assert run.entry == "script.sh"  # default
        assert run.isolation == "none"  # default
        assert run.network is True  # default
        assert run.limits is None
        assert run.extra_read_paths == []
        assert run.binaries == []

    def test_full_job_run(self):
        """JobRun with all fields specified."""
        run = JobRun(
            user="testuser",
            timeout=300,
            entry="main.py",
            binaries=["bin/helper"],
            isolation="strict",
            network=False,
            extra_read_paths=["/etc/hostname"],
            limits=ResourceLimits(cpu_percent=50, memory_mb=512, processes=10)
        )
        assert run.user == "testuser"
        assert run.timeout == 300
        assert run.entry == "main.py"
        assert run.binaries == ["bin/helper"]
        assert run.isolation == "strict"
        assert run.network is False
        assert run.extra_read_paths == ["/etc/hostname"]
        assert run.limits.cpu_percent == 50

    def test_isolation_validation(self):
        """Invalid isolation mode raises error."""
        with pytest.raises(ValueError, match="Invalid isolation mode"):
            JobRun(user="nobody", isolation="invalid")

    def test_valid_isolation_modes(self):
        """All valid isolation modes are accepted."""
        for mode in ["strict", "relaxed", "none"]:
            run = JobRun(user="nobody", isolation=mode)
            assert run.isolation == mode


class TestResourceLimits:
    """Tests for ResourceLimits model."""

    def test_minimal_limits(self):
        """ResourceLimits with no fields."""
        limits = ResourceLimits()
        assert limits.cpu_percent is None
        assert limits.memory_mb is None
        assert limits.processes is None

    def test_full_limits(self):
        """ResourceLimits with all fields."""
        limits = ResourceLimits(
            cpu_percent=75,
            memory_mb=1024,
            processes=50
        )
        assert limits.cpu_percent == 75
        assert limits.memory_mb == 1024
        assert limits.processes == 50

    def test_cpu_percent_bounds(self):
        """CPU percent must be 1-100."""
        with pytest.raises(ValueError):
            ResourceLimits(cpu_percent=0)
        with pytest.raises(ValueError):
            ResourceLimits(cpu_percent=101)


class TestJob:
    """Tests for Job model (SPEC.md v0.5.0 format)."""

    def test_minimal_job(self):
        """Job with only required fields."""
        job = Job(
            id="test.job",
            version="1.0.0",
            schedule=JobSchedule(min_daily=1, max_daily=10),
            run=JobRun(user="nobody")
        )
        assert job.id == "test.job"
        assert job.version == "1.0.0"
        assert job.reads == []
        assert job.writes == []
        assert job.depends == []

    def test_full_job(self):
        """Job with all fields specified."""
        job = Job(
            id="backup.daily",
            version="2.1.0",
            reads=["input_data", "config_file"],
            writes=["output_data"],
            depends=["prepare.data"],
            schedule=JobSchedule(min_daily=1, max_daily=2),
            run=JobRun(
                user="backup",
                timeout=600,
                isolation="strict",
                network=False
            )
        )
        assert job.id == "backup.daily"
        assert job.reads == ["input_data", "config_file"]
        assert job.writes == ["output_data"]
        assert job.depends == ["prepare.data"]
        assert job.run.user == "backup"
        assert job.run.isolation == "strict"

    def test_job_hashable(self):
        """Jobs can be used in sets/dicts."""
        job1 = Job(
            id="test.job",
            version="1.0.0",
            schedule=JobSchedule(min_daily=1, max_daily=10),
            run=JobRun(user="nobody")
        )
        job2 = Job(
            id="test.job",
            version="1.0.0",
            schedule=JobSchedule(min_daily=1, max_daily=10),
            run=JobRun(user="nobody")
        )
        # Same ID = equal
        assert job1 == job2
        assert hash(job1) == hash(job2)

        # Can use in set
        job_set = {job1, job2}
        assert len(job_set) == 1


class TestJobFile:
    """Tests for JobFile model (YAML root structure)."""

    def test_parse_yaml_format(self):
        """Parse job.yaml format from SPEC.md."""
        import yaml

        yaml_content = """
job:
  id: test.job
  version: "1.0.0"

  reads: [input_data]
  writes: [output_data]

  depends: []

  schedule:
    min_daily: 1
    max_daily: 10

  run:
    user: nobody
    timeout: 300
    isolation: strict
    network: false
    limits:
      cpu_percent: 50
      memory_mb: 256
"""
        data = yaml.safe_load(yaml_content)
        job_file = JobFile(**data)

        assert job_file.job.id == "test.job"
        assert job_file.job.reads == ["input_data"]
        assert job_file.job.writes == ["output_data"]
        assert job_file.job.run.isolation == "strict"
        assert job_file.job.run.network is False
        assert job_file.job.run.limits.cpu_percent == 50


class TestDataRegistryEntry:
    """Tests for DataRegistryEntry model (SPEC.md v0.8.0 Phase 15 format)."""

    # ─────────────────────────────────────────────────────────────────────
    # type: fs - Filesystem registers
    # ─────────────────────────────────────────────────────────────────────

    def test_filesystem_entry(self):
        """Parse fs type registry entry."""
        entry = DataRegistryEntry(
            type="fs",
            path="/var/data/input.txt",
            kind="file"
        )
        assert entry.type == "fs"
        assert entry.path == "/var/data/input.txt"
        assert entry.kind == "file"
        assert entry.protect is False  # Default

    def test_filesystem_dir(self):
        """Parse fs type directory entry."""
        entry = DataRegistryEntry(
            type="fs",
            path="/var/data/output",
            kind="dir"
        )
        assert entry.type == "fs"
        assert entry.kind == "dir"

    def test_filesystem_protected(self):
        """Parse fs type with protect=True (external input)."""
        entry = DataRegistryEntry(
            type="fs",
            path="/var/data/external.db",
            kind="file",
            protect=True
        )
        assert entry.type == "fs"
        assert entry.protect is True

    def test_filesystem_requires_path(self):
        """fs type requires path."""
        with pytest.raises(ValidationError, match="path required for type: fs"):
            DataRegistryEntry(type="fs", kind="file")

    def test_filesystem_cannot_have_bucket(self):
        """fs type cannot have bucket."""
        with pytest.raises(ValidationError, match="bucket not allowed for type: fs"):
            DataRegistryEntry(type="fs", path="/data/file", kind="file", bucket="test")

    # ─────────────────────────────────────────────────────────────────────
    # type: temp - Temporary registers (MinIO only)
    # ─────────────────────────────────────────────────────────────────────

    def test_temp_entry(self):
        """Parse temp type registry entry."""
        entry = DataRegistryEntry(type="temp", kind="file")
        assert entry.type == "temp"
        assert entry.kind == "file"
        assert entry.path is None
        assert entry.bucket is None

    def test_temp_dir(self):
        """Parse temp type directory entry."""
        entry = DataRegistryEntry(type="temp", kind="dir")
        assert entry.type == "temp"
        assert entry.kind == "dir"

    def test_temp_cannot_have_path(self):
        """temp type cannot have path."""
        with pytest.raises(ValidationError, match="path not allowed for type: temp"):
            DataRegistryEntry(type="temp", kind="file", path="/data/temp")

    def test_temp_cannot_have_bucket(self):
        """temp type cannot have bucket."""
        with pytest.raises(ValidationError, match="bucket not allowed for type: temp"):
            DataRegistryEntry(type="temp", kind="file", bucket="test")

    def test_temp_cannot_have_protect(self):
        """temp type cannot have protect."""
        with pytest.raises(ValidationError, match="protect not allowed for type: temp"):
            DataRegistryEntry(type="temp", kind="file", protect=True)

    # ─────────────────────────────────────────────────────────────────────
    # type: minio - Permanent MinIO registers
    # ─────────────────────────────────────────────────────────────────────

    def test_minio_entry(self):
        """Parse minio type registry entry."""
        entry = DataRegistryEntry(
            type="minio",
            bucket="artifacts",
            prefix="jobs/output/",
            kind="file"
        )
        assert entry.type == "minio"
        assert entry.bucket == "artifacts"
        assert entry.prefix == "jobs/output/"
        assert entry.kind == "file"

    def test_minio_requires_bucket(self):
        """minio type requires bucket."""
        with pytest.raises(ValidationError, match="bucket required for type: minio"):
            DataRegistryEntry(type="minio", kind="file")

    def test_minio_cannot_have_path(self):
        """minio type cannot have path."""
        with pytest.raises(ValidationError, match="path not allowed for type: minio"):
            DataRegistryEntry(type="minio", bucket="test", kind="file", path="/data")

    def test_minio_cannot_have_protect(self):
        """minio type cannot have protect."""
        with pytest.raises(ValidationError, match="protect not supported for type: minio"):
            DataRegistryEntry(type="minio", bucket="test", kind="file", protect=True)

    def test_minio_bucket_validation(self):
        """minio bucket must follow S3 naming rules."""
        # Valid bucket names
        DataRegistryEntry(type="minio", bucket="my-bucket", kind="file")
        DataRegistryEntry(type="minio", bucket="my.bucket.name", kind="file")

        # Invalid: uppercase
        with pytest.raises(ValidationError, match="Invalid bucket name"):
            DataRegistryEntry(type="minio", bucket="MyBucket", kind="file")

        # Invalid: too short
        with pytest.raises(ValidationError, match="Invalid bucket name"):
            DataRegistryEntry(type="minio", bucket="ab", kind="file")

    def test_minio_prefix_no_traversal(self):
        """minio prefix cannot contain path traversal."""
        with pytest.raises(ValidationError, match="Path traversal in prefix"):
            DataRegistryEntry(type="minio", bucket="test", prefix="../escape", kind="file")

    def test_minio_prefix_no_leading_slash(self):
        """minio prefix cannot start with /."""
        with pytest.raises(ValidationError, match="Prefix must not start with"):
            DataRegistryEntry(type="minio", bucket="test", prefix="/absolute", kind="file")

    # ─────────────────────────────────────────────────────────────────────
    # General validation
    # ─────────────────────────────────────────────────────────────────────

    def test_invalid_type(self):
        """Invalid type raises error."""
        with pytest.raises(ValidationError, match="'fs', 'temp' or 'minio'"):
            DataRegistryEntry(type="s3", kind="file")

    def test_invalid_kind(self):
        """Invalid kind raises error."""
        with pytest.raises(ValidationError, match="'file' or 'dir'"):
            DataRegistryEntry(type="fs", path="/data", kind="folder")

    def test_kind_required(self):
        """kind is required for all types."""
        with pytest.raises(ValidationError, match="Field required"):
            DataRegistryEntry(type="fs", path="/data")

    def test_parse_compact_yaml(self):
        """Parse compact YAML format from Phase 15 spec."""
        import yaml

        yaml_content = """
registry:
  sensor_raw:     {type: fs, path: /var/share/sensors/raw, kind: dir}
  sensor_config:  {type: fs, path: /var/share/sensors/config.json, kind: file, protect: true}
  intermediate:   {type: temp, kind: file}
  sensor_parsed:  {type: minio, bucket: linearjc, prefix: intermediate/parsed/, kind: dir}
"""
        data = yaml.safe_load(yaml_content)

        # Parse each entry
        raw = DataRegistryEntry(**data['registry']['sensor_raw'])
        assert raw.type == "fs"
        assert raw.kind == "dir"
        assert raw.protect is False

        config = DataRegistryEntry(**data['registry']['sensor_config'])
        assert config.type == "fs"
        assert config.kind == "file"
        assert config.protect is True

        intermediate = DataRegistryEntry(**data['registry']['intermediate'])
        assert intermediate.type == "temp"
        assert intermediate.kind == "file"

        parsed = DataRegistryEntry(**data['registry']['sensor_parsed'])
        assert parsed.type == "minio"
        assert parsed.bucket == "linearjc"
        assert parsed.kind == "dir"


class TestJobSchedule:
    """Tests for JobSchedule model."""

    def test_valid_schedule(self):
        """Valid schedule is accepted."""
        schedule = JobSchedule(min_daily=1, max_daily=10)
        assert schedule.min_daily == 1
        assert schedule.max_daily == 10

    def test_max_ge_min(self):
        """max_daily must be >= min_daily."""
        with pytest.raises(ValueError, match="max_daily"):
            JobSchedule(min_daily=10, max_daily=5)

    def test_bounds(self):
        """Schedule values must be within bounds."""
        with pytest.raises(ValueError):
            JobSchedule(min_daily=0, max_daily=10)
        with pytest.raises(ValueError):
            JobSchedule(min_daily=1, max_daily=300)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
