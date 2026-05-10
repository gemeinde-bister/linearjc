"""
Unit tests for registry reload safety validation.

Tests cover:
- Registry change detection (compare_registries)
- Safety validation for each change type
- Blocking vs warning behavior
- Integration with RegisterLock

See SPEC.md, Hot Reload (SIGHUP) section for specification.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import DataRegistryEntry
from coordinator_v2.register_lock import RegisterLock
from coordinator_v2.registry_reload import (
    ChangeType,
    RegistryChange,
    ReloadValidationResult,
    compare_registries,
    validate_registry_reload,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def lock_mgr() -> RegisterLock:
    """Create a fresh RegisterLock instance."""
    return RegisterLock()


@pytest.fixture
def base_registry() -> dict[str, DataRegistryEntry]:
    """Base registry for testing changes."""
    return {
        "input_db": DataRegistryEntry(
            type="fs", path="/data/input.db", kind="file", protect=True
        ),
        "output_json": DataRegistryEntry(
            type="fs", path="/data/output.json", kind="file"
        ),
        "temp_data": DataRegistryEntry(type="temp", kind="file"),
        "minio_archive": DataRegistryEntry(
            type="minio", bucket="archives", prefix="daily/", kind="dir"
        ),
    }


# =============================================================================
# compare_registries Tests
# =============================================================================


class TestCompareRegistries:
    """Tests for compare_registries function."""

    def test_no_changes(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Identical registries produce no changes."""
        changes = compare_registries(base_registry, base_registry)
        assert len(changes) == 0

    def test_added_register(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects newly added register."""
        new_registry = base_registry.copy()
        new_registry["new_entry"] = DataRegistryEntry(
            type="fs", path="/data/new.json", kind="file"
        )

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ADDED
        assert changes[0].key == "new_entry"
        assert changes[0].old_entry is None
        assert changes[0].new_entry is not None

    def test_removed_register(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects removed register."""
        new_registry = {k: v for k, v in base_registry.items() if k != "output_json"}

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.REMOVED
        assert changes[0].key == "output_json"
        assert changes[0].lock_path == "/data/output.json"

    def test_path_changed(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects path change for fs type."""
        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/new_output.json", kind="file"
        )

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.PATH_CHANGED
        assert changes[0].key == "output_json"

    def test_bucket_changed(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects bucket/prefix change for minio type."""
        new_registry = base_registry.copy()
        new_registry["minio_archive"] = DataRegistryEntry(
            type="minio", bucket="new-bucket", prefix="daily/", kind="dir"
        )

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.BUCKET_CHANGED
        assert changes[0].key == "minio_archive"

    def test_type_changed(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects type change."""
        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(type="temp", kind="file")

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.TYPE_CHANGED
        assert changes[0].key == "output_json"

    def test_protect_added(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects adding protect:true."""
        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/output.json", kind="file", protect=True
        )

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.PROTECT_ADDED
        assert changes[0].key == "output_json"

    def test_protect_removed(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects removing protect:true."""
        new_registry = base_registry.copy()
        new_registry["input_db"] = DataRegistryEntry(
            type="fs", path="/data/input.db", kind="file", protect=False
        )

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.PROTECT_REMOVED
        assert changes[0].key == "input_db"

    def test_kind_changed(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects kind change (file -> dir)."""
        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/output.json", kind="dir"
        )

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.KIND_CHANGED
        assert changes[0].key == "output_json"

    def test_multiple_changes(self, base_registry: dict[str, DataRegistryEntry]) -> None:
        """Detects multiple changes in one reload."""
        new_registry = base_registry.copy()
        # Add new
        new_registry["extra"] = DataRegistryEntry(type="temp", kind="file")
        # Remove existing
        del new_registry["temp_data"]
        # Modify path
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/new_output.json", kind="file"
        )

        changes = compare_registries(base_registry, new_registry)

        assert len(changes) == 3
        change_types = {c.change_type for c in changes}
        assert ChangeType.ADDED in change_types
        assert ChangeType.REMOVED in change_types
        assert ChangeType.PATH_CHANGED in change_types


# =============================================================================
# validate_registry_reload Tests - No Locks Held
# =============================================================================


class TestValidationNoLocks:
    """Tests for validation when no locks are held."""

    def test_added_register_allowed(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Adding a register is always allowed."""
        new_registry = base_registry.copy()
        new_registry["new_entry"] = DataRegistryEntry(
            type="fs", path="/data/new.json", kind="file"
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.errors) == 0

    def test_removed_register_allowed_if_no_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Removing a register is allowed if no lock held."""
        new_registry = {k: v for k, v in base_registry.items() if k != "output_json"}

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.errors) == 0

    def test_path_changed_allowed_if_no_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Path change is allowed if no lock held."""
        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/new_output.json", kind="file"
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.warnings) == 0

    def test_type_changed_allowed_if_no_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Type change is allowed if no lock held."""
        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(type="temp", kind="file")

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.errors) == 0

    def test_protect_added_allowed_if_no_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Adding protect is allowed if no lock held."""
        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/output.json", kind="file", protect=True
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.errors) == 0

    def test_protect_removed_allowed_if_no_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Removing protect is allowed if no lock held."""
        new_registry = base_registry.copy()
        new_registry["input_db"] = DataRegistryEntry(
            type="fs", path="/data/input.db", kind="file", protect=False
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.errors) == 0


# =============================================================================
# validate_registry_reload Tests - With Locks Held
# =============================================================================


class TestValidationWithLocks:
    """Tests for validation when locks are held."""

    def test_removed_register_blocked_with_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Removing a register is blocked if lock held."""
        # Acquire exclusive lock on output_json
        lock_path = "/data/output.json"
        lock_mgr.try_acquire_all([(lock_path, "exclusive")], "tree-001")

        new_registry = {k: v for k, v in base_registry.items() if k != "output_json"}

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert result.blocked
        assert len(result.errors) == 1
        assert "output_json" in result.errors[0]
        assert "locked" in result.errors[0].lower()

    def test_type_changed_blocked_with_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Type change is blocked if lock held."""
        # Acquire lock
        lock_path = "/data/output.json"
        lock_mgr.try_acquire_all([(lock_path, "exclusive")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(type="temp", kind="file")

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert result.blocked
        assert len(result.errors) == 1
        assert "output_json" in result.errors[0]

    def test_protect_added_blocked_with_exclusive_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Adding protect:true is blocked if exclusive (writer) lock held."""
        # Acquire exclusive lock (writer)
        lock_path = "/data/output.json"
        lock_mgr.try_acquire_all([(lock_path, "exclusive")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/output.json", kind="file", protect=True
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert result.blocked
        assert len(result.errors) == 1
        assert "protect:true" in result.errors[0]
        assert "exclusive lock" in result.errors[0]

    def test_protect_added_allowed_with_shared_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Adding protect:true is allowed if only shared (reader) lock held."""
        # Acquire shared lock (reader)
        lock_path = "/data/output.json"
        lock_mgr.try_acquire_all([(lock_path, "shared")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/output.json", kind="file", protect=True
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        # Shared lock = readers, protect:true means no new writers
        # Readers can continue, so this is OK
        assert not result.blocked

    def test_protect_removed_blocked_with_shared_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Removing protect:true is blocked if shared (reader) lock held."""
        # Acquire shared lock on protected register
        lock_path = "/data/input.db"
        lock_mgr.try_acquire_all([(lock_path, "shared")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["input_db"] = DataRegistryEntry(
            type="fs", path="/data/input.db", kind="file", protect=False
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert result.blocked
        assert len(result.errors) == 1
        assert "protect:true" in result.errors[0]
        assert "shared lock" in result.errors[0]

    def test_protect_removed_allowed_with_exclusive_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Removing protect:true is allowed if exclusive lock held (shouldn't happen for protected)."""
        # Note: Protected registers shouldn't have exclusive locks in normal operation
        # But if somehow it does, removing protect is OK (writer already has access)
        lock_path = "/data/input.db"
        lock_mgr.try_acquire_all([(lock_path, "exclusive")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["input_db"] = DataRegistryEntry(
            type="fs", path="/data/input.db", kind="file", protect=False
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked

    def test_path_changed_warns_with_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Path change warns if lock held (but doesn't block)."""
        # Acquire lock
        lock_path = "/data/output.json"
        lock_mgr.try_acquire_all([(lock_path, "exclusive")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/new_output.json", kind="file"
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.warnings) == 1
        assert "orphaned" in result.warnings[0].lower()

    def test_bucket_changed_warns_with_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Bucket change warns if lock held (but doesn't block)."""
        # Acquire lock on minio path
        lock_path = "minio:archives/daily/"
        lock_mgr.try_acquire_all([(lock_path, "exclusive")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["minio_archive"] = DataRegistryEntry(
            type="minio", bucket="new-bucket", prefix="daily/", kind="dir"
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.warnings) == 1
        assert "orphaned" in result.warnings[0].lower()

    def test_kind_changed_warns_with_lock(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Kind change warns if lock held."""
        # Acquire lock
        lock_path = "/data/output.json"
        lock_mgr.try_acquire_all([(lock_path, "exclusive")], "tree-001")

        new_registry = base_registry.copy()
        new_registry["output_json"] = DataRegistryEntry(
            type="fs", path="/data/output.json", kind="dir"
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        assert len(result.warnings) == 1
        assert "kind" in result.warnings[0].lower()


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Edge cases and special scenarios."""

    def test_temp_register_no_lock_path(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Temp registers don't have lock paths - can be removed freely."""
        new_registry = {k: v for k, v in base_registry.items() if k != "temp_data"}

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked
        # The removed change should have lock_path=None
        removed = [c for c in result.changes if c.change_type == ChangeType.REMOVED]
        assert len(removed) == 1
        assert removed[0].lock_path is None

    def test_multiple_errors_all_reported(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Multiple blocking changes produce multiple errors."""
        # Acquire locks on two registers
        lock_mgr.try_acquire_all(
            [("/data/output.json", "exclusive"), ("/data/input.db", "shared")],
            "tree-001",
        )

        new_registry = base_registry.copy()
        # Remove output_json (blocked - exclusive lock)
        del new_registry["output_json"]
        # Remove protect from input_db (blocked - shared lock)
        new_registry["input_db"] = DataRegistryEntry(
            type="fs", path="/data/input.db", kind="file", protect=False
        )

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert result.blocked
        assert len(result.errors) == 2

    def test_empty_registries(self, lock_mgr: RegisterLock) -> None:
        """Empty registries produce no changes."""
        result = validate_registry_reload({}, {}, lock_mgr)

        assert not result.blocked
        assert len(result.changes) == 0

    def test_unrelated_lock_doesnt_block(
        self, lock_mgr: RegisterLock, base_registry: dict[str, DataRegistryEntry]
    ) -> None:
        """Lock on different path doesn't block unrelated changes."""
        # Lock a different path
        lock_mgr.try_acquire_all([("/other/path.txt", "exclusive")], "tree-001")

        # Modify output_json (different path)
        new_registry = base_registry.copy()
        del new_registry["output_json"]

        result = validate_registry_reload(base_registry, new_registry, lock_mgr)

        assert not result.blocked


# =============================================================================
# RegistryChange String Representation
# =============================================================================


class TestChangeStrings:
    """Tests for RegistryChange.__str__() method."""

    def test_added_str(self) -> None:
        """ADDED change has readable string."""
        change = RegistryChange(
            key="new_entry",
            change_type=ChangeType.ADDED,
            new_entry=DataRegistryEntry(type="fs", path="/data/new.json", kind="file"),
        )
        assert "Added" in str(change)
        assert "new_entry" in str(change)

    def test_removed_str(self) -> None:
        """REMOVED change has readable string."""
        change = RegistryChange(
            key="old_entry",
            change_type=ChangeType.REMOVED,
            old_entry=DataRegistryEntry(type="fs", path="/data/old.json", kind="file"),
        )
        assert "Removed" in str(change)
        assert "old_entry" in str(change)

    def test_path_changed_str(self) -> None:
        """PATH_CHANGED change shows old and new paths."""
        change = RegistryChange(
            key="my_data",
            change_type=ChangeType.PATH_CHANGED,
            old_entry=DataRegistryEntry(type="fs", path="/old/path.json", kind="file"),
            new_entry=DataRegistryEntry(type="fs", path="/new/path.json", kind="file"),
        )
        s = str(change)
        assert "/old/path.json" in s
        assert "/new/path.json" in s

    def test_type_changed_str(self) -> None:
        """TYPE_CHANGED change shows old and new types."""
        change = RegistryChange(
            key="my_data",
            change_type=ChangeType.TYPE_CHANGED,
            old_entry=DataRegistryEntry(type="fs", path="/data.json", kind="file"),
            new_entry=DataRegistryEntry(type="temp", kind="file"),
        )
        s = str(change)
        assert "fs" in s
        assert "temp" in s
