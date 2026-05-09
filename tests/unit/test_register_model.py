"""Unit tests for Phase 15 Register Model validation."""
import pytest
import sys
import tempfile
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import DataRegistryEntry
from coordinator.security_utils import (
    SecurityError,
    validate_job_writes_against_registry,
    validate_protected_registers_exist,
)


class TestValidateJobWritesAgainstRegistry:
    """Tests for validate_job_writes_against_registry function."""

    def test_allows_write_to_unprotected_fs(self):
        """Jobs can write to unprotected fs registers."""
        registry = {
            "output": DataRegistryEntry(type="fs", path="/data/output.json", kind="file")
        }
        # Should not raise
        validate_job_writes_against_registry("my.job", ["output"], registry)

    def test_allows_write_to_temp(self):
        """Jobs can write to temp registers."""
        registry = {
            "intermediate": DataRegistryEntry(type="temp", kind="file")
        }
        # Should not raise
        validate_job_writes_against_registry("my.job", ["intermediate"], registry)

    def test_allows_write_to_minio(self):
        """Jobs can write to minio registers."""
        registry = {
            "big_data": DataRegistryEntry(type="minio", bucket="data", kind="dir")
        }
        # Should not raise
        validate_job_writes_against_registry("my.job", ["big_data"], registry)

    def test_blocks_write_to_protected_fs(self):
        """Jobs cannot write to protected fs registers."""
        registry = {
            "source_db": DataRegistryEntry(
                type="fs", path="/data/source.db", kind="file", protect=True
            )
        }
        with pytest.raises(SecurityError, match="cannot write to protected register"):
            validate_job_writes_against_registry("my.job", ["source_db"], registry)

    def test_blocks_write_to_protected_with_multiple_outputs(self):
        """Job with multiple outputs cannot write to any protected register."""
        registry = {
            "input": DataRegistryEntry(
                type="fs", path="/data/input.json", kind="file", protect=True
            ),
            "output": DataRegistryEntry(
                type="fs", path="/data/output.json", kind="file"
            ),
        }
        # First output is unprotected, second is protected
        with pytest.raises(SecurityError, match="cannot write to protected register 'input'"):
            validate_job_writes_against_registry("my.job", ["output", "input"], registry)

    def test_ignores_missing_registry_keys(self):
        """Missing registry keys are ignored (caught by other validation)."""
        registry = {}
        # Should not raise (missing key validation is elsewhere)
        validate_job_writes_against_registry("my.job", ["nonexistent"], registry)

    def test_empty_writes_list(self):
        """Empty writes list is valid."""
        registry = {
            "input": DataRegistryEntry(
                type="fs", path="/data/input.json", kind="file", protect=True
            )
        }
        # Should not raise
        validate_job_writes_against_registry("my.job", [], registry)


class TestValidateProtectedRegistersExist:
    """Tests for validate_protected_registers_exist function."""

    def test_passes_when_protected_file_exists(self):
        """Validation passes when protected file exists."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            registry = {
                "source": DataRegistryEntry(
                    type="fs", path=temp_path, kind="file", protect=True
                )
            }
            # Should not raise
            validate_protected_registers_exist(registry)
        finally:
            Path(temp_path).unlink()

    def test_passes_when_protected_dir_exists(self):
        """Validation passes when protected directory exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = {
                "config_dir": DataRegistryEntry(
                    type="fs", path=temp_dir, kind="dir", protect=True
                )
            }
            # Should not raise
            validate_protected_registers_exist(registry)

    def test_fails_when_protected_file_missing(self):
        """Validation fails when protected file doesn't exist."""
        registry = {
            "missing": DataRegistryEntry(
                type="fs", path="/nonexistent/file.db", kind="file", protect=True
            )
        }
        with pytest.raises(SecurityError, match="Protected registers not found"):
            validate_protected_registers_exist(registry)

    def test_fails_with_multiple_missing(self):
        """Validation fails listing all missing protected registers."""
        registry = {
            "missing1": DataRegistryEntry(
                type="fs", path="/nonexistent/one.db", kind="file", protect=True
            ),
            "missing2": DataRegistryEntry(
                type="fs", path="/nonexistent/two.db", kind="file", protect=True
            ),
        }
        with pytest.raises(SecurityError) as exc_info:
            validate_protected_registers_exist(registry)

        error_msg = str(exc_info.value)
        assert "missing1" in error_msg
        assert "missing2" in error_msg

    def test_ignores_unprotected_registers(self):
        """Unprotected registers are not checked for existence."""
        registry = {
            "output": DataRegistryEntry(
                type="fs", path="/nonexistent/output.json", kind="file", protect=False
            )
        }
        # Should not raise (unprotected registers can be created)
        validate_protected_registers_exist(registry)

    def test_ignores_temp_registers(self):
        """Temp registers are not checked (no path)."""
        registry = {
            "intermediate": DataRegistryEntry(type="temp", kind="file")
        }
        # Should not raise
        validate_protected_registers_exist(registry)

    def test_ignores_minio_registers(self):
        """MinIO registers are not checked for filesystem existence."""
        registry = {
            "big_data": DataRegistryEntry(
                type="minio", bucket="data", kind="dir"
            )
        }
        # Should not raise
        validate_protected_registers_exist(registry)

    def test_mixed_registry(self):
        """Validation only fails on protected registers that don't exist."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            existing_path = f.name

        try:
            registry = {
                "exists": DataRegistryEntry(
                    type="fs", path=existing_path, kind="file", protect=True
                ),
                "missing": DataRegistryEntry(
                    type="fs", path="/nonexistent/file.db", kind="file", protect=True
                ),
                "output": DataRegistryEntry(
                    type="fs", path="/nonexistent/output.json", kind="file"
                ),
                "temp": DataRegistryEntry(type="temp", kind="file"),
            }
            with pytest.raises(SecurityError) as exc_info:
                validate_protected_registers_exist(registry)

            error_msg = str(exc_info.value)
            assert "missing" in error_msg
            assert "exists" not in error_msg
            assert "output" not in error_msg
            assert "temp" not in error_msg
        finally:
            Path(existing_path).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
