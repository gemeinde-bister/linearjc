"""
Registry reload validation for hot reload safety.

Validates that registry changes during SIGHUP reload are safe given
current lock state. Prevents data corruption and lock inconsistencies.

Design follows SPEC.md, Hot Reload (SIGHUP) section:
- Add new register: OK
- Modify register path: Warn if trees active (locks on old path)
- Remove register: Block if active trees reference it
- Add protect: true: Block if exclusive lock held
- Remove protect: true: Block if shared lock held

Example:
    result = validate_registry_reload(old_reg, new_reg, register_lock)
    if result.blocked:
        for error in result.errors:
            logger.error(error)
        # Abort reload
    else:
        for warning in result.warnings:
            logger.warning(warning)
        # Proceed with reload
"""

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

from coordinator.models import DataRegistryEntry

if TYPE_CHECKING:
    from coordinator_v2.register_lock import RegisterLock

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Types of registry entry changes."""

    ADDED = auto()  # New entry (not in old)
    REMOVED = auto()  # Entry deleted (not in new)
    PATH_CHANGED = auto()  # type=fs, path differs
    BUCKET_CHANGED = auto()  # type=minio, bucket/prefix differs
    TYPE_CHANGED = auto()  # Entry type changed (fs->temp, etc.)
    PROTECT_ADDED = auto()  # protect: false -> true
    PROTECT_REMOVED = auto()  # protect: true -> false
    KIND_CHANGED = auto()  # kind: file -> dir or vice versa


@dataclass
class RegistryChange:
    """A detected change in the registry."""

    key: str  # Registry key
    change_type: ChangeType
    old_entry: DataRegistryEntry | None = None
    new_entry: DataRegistryEntry | None = None
    lock_path: str | None = None  # Canonical lock path (if applicable)

    def __str__(self) -> str:
        """Human-readable description of the change."""
        if self.change_type == ChangeType.ADDED:
            return f"Added register '{self.key}'"
        elif self.change_type == ChangeType.REMOVED:
            return f"Removed register '{self.key}'"
        elif self.change_type == ChangeType.PATH_CHANGED:
            old_path = self.old_entry.path if self.old_entry else "?"
            new_path = self.new_entry.path if self.new_entry else "?"
            return f"Path changed for '{self.key}': {old_path} -> {new_path}"
        elif self.change_type == ChangeType.BUCKET_CHANGED:
            return f"Bucket/prefix changed for '{self.key}'"
        elif self.change_type == ChangeType.TYPE_CHANGED:
            old_type = self.old_entry.type if self.old_entry else "?"
            new_type = self.new_entry.type if self.new_entry else "?"
            return f"Type changed for '{self.key}': {old_type} -> {new_type}"
        elif self.change_type == ChangeType.PROTECT_ADDED:
            return f"Added protect:true to '{self.key}'"
        elif self.change_type == ChangeType.PROTECT_REMOVED:
            return f"Removed protect:true from '{self.key}'"
        elif self.change_type == ChangeType.KIND_CHANGED:
            old_kind = self.old_entry.kind if self.old_entry else "?"
            new_kind = self.new_entry.kind if self.new_entry else "?"
            return f"Kind changed for '{self.key}': {old_kind} -> {new_kind}"
        else:
            return f"Unknown change for '{self.key}'"


@dataclass
class ReloadValidationResult:
    """Result of registry reload validation."""

    blocked: bool = False  # If True, reload should be aborted
    warnings: list[str] = field(default_factory=list)  # Non-blocking warnings
    errors: list[str] = field(default_factory=list)  # Blocking errors
    changes: list[RegistryChange] = field(default_factory=list)  # All detected changes


def _get_lock_path(entry: DataRegistryEntry, key: str) -> str | None:
    """
    Get canonical lock path for a registry entry.

    Mirrors register_lock.lock_path() but doesn't raise on unknown types.
    Returns None for types that don't use locking.
    """
    from pathlib import Path

    if entry.type == "fs":
        if not entry.path:
            return None
        return str(Path(entry.path).resolve())
    elif entry.type == "minio":
        if not entry.bucket:
            return None
        prefix = entry.prefix or ""
        return f"minio:{entry.bucket}/{prefix}"
    elif entry.type == "temp":
        # Temp registers don't use locking
        return None
    else:
        logger.warning(f"Unknown register type for '{key}': {entry.type}")
        return None


def compare_registries(
    old_registry: dict[str, DataRegistryEntry],
    new_registry: dict[str, DataRegistryEntry],
) -> list[RegistryChange]:
    """
    Compare old and new registries to detect changes.

    Args:
        old_registry: Current registry (before reload)
        new_registry: New registry (being loaded)

    Returns:
        List of RegistryChange objects describing all changes
    """
    changes: list[RegistryChange] = []

    all_keys = set(old_registry.keys()) | set(new_registry.keys())

    for key in all_keys:
        old_entry = old_registry.get(key)
        new_entry = new_registry.get(key)

        if old_entry is None and new_entry is not None:
            # New entry added
            changes.append(
                RegistryChange(
                    key=key,
                    change_type=ChangeType.ADDED,
                    new_entry=new_entry,
                )
            )

        elif old_entry is not None and new_entry is None:
            # Entry removed
            lock_path = _get_lock_path(old_entry, key)
            changes.append(
                RegistryChange(
                    key=key,
                    change_type=ChangeType.REMOVED,
                    old_entry=old_entry,
                    lock_path=lock_path,
                )
            )

        elif old_entry is not None and new_entry is not None:
            # Entry exists in both - check for modifications
            lock_path = _get_lock_path(old_entry, key)

            # Type changed
            if old_entry.type != new_entry.type:
                changes.append(
                    RegistryChange(
                        key=key,
                        change_type=ChangeType.TYPE_CHANGED,
                        old_entry=old_entry,
                        new_entry=new_entry,
                        lock_path=lock_path,
                    )
                )
                # Skip other checks if type changed - they're not comparable
                continue

            # Path changed (fs type)
            if old_entry.type == "fs":
                old_path = old_entry.path
                new_path = new_entry.path
                if old_path != new_path:
                    changes.append(
                        RegistryChange(
                            key=key,
                            change_type=ChangeType.PATH_CHANGED,
                            old_entry=old_entry,
                            new_entry=new_entry,
                            lock_path=lock_path,
                        )
                    )

            # Bucket/prefix changed (minio type)
            if old_entry.type == "minio":
                old_loc = (old_entry.bucket, old_entry.prefix)
                new_loc = (new_entry.bucket, new_entry.prefix)
                if old_loc != new_loc:
                    changes.append(
                        RegistryChange(
                            key=key,
                            change_type=ChangeType.BUCKET_CHANGED,
                            old_entry=old_entry,
                            new_entry=new_entry,
                            lock_path=lock_path,
                        )
                    )

            # protect flag changed
            old_protect = getattr(old_entry, "protect", False)
            new_protect = getattr(new_entry, "protect", False)
            if not old_protect and new_protect:
                changes.append(
                    RegistryChange(
                        key=key,
                        change_type=ChangeType.PROTECT_ADDED,
                        old_entry=old_entry,
                        new_entry=new_entry,
                        lock_path=lock_path,
                    )
                )
            elif old_protect and not new_protect:
                changes.append(
                    RegistryChange(
                        key=key,
                        change_type=ChangeType.PROTECT_REMOVED,
                        old_entry=old_entry,
                        new_entry=new_entry,
                        lock_path=lock_path,
                    )
                )

            # kind changed
            if old_entry.kind != new_entry.kind:
                changes.append(
                    RegistryChange(
                        key=key,
                        change_type=ChangeType.KIND_CHANGED,
                        old_entry=old_entry,
                        new_entry=new_entry,
                        lock_path=lock_path,
                    )
                )

    return changes


def validate_registry_reload(
    old_registry: dict[str, DataRegistryEntry],
    new_registry: dict[str, DataRegistryEntry],
    register_lock: "RegisterLock",
) -> ReloadValidationResult:
    """
    Validate that registry changes are safe to apply during hot reload.

    Checks each change against current lock state:
    - ADDED: Always OK (no existing state to conflict with)
    - REMOVED: Block if lock held (would orphan the lock)
    - PATH_CHANGED: Warn if lock held (lock on old path becomes stale)
    - BUCKET_CHANGED: Warn if lock held (same concern as path)
    - TYPE_CHANGED: Block if lock held (type determines lock path format)
    - PROTECT_ADDED: Block if exclusive lock held (writers can't be blocked mid-write)
    - PROTECT_REMOVED: Block if shared lock held (readers assume immutability)
    - KIND_CHANGED: Warn if lock held (might affect I/O behavior)

    Args:
        old_registry: Current registry (before reload)
        new_registry: New registry (being loaded)
        register_lock: Current lock manager with active locks

    Returns:
        ReloadValidationResult with blocked status, warnings, and errors
    """
    result = ReloadValidationResult()
    changes = compare_registries(old_registry, new_registry)
    result.changes = changes

    for change in changes:
        if change.change_type == ChangeType.ADDED:
            # New registers are always safe
            logger.debug(f"Hot reload: {change}")
            continue

        if change.change_type == ChangeType.REMOVED:
            # Block if any lock held on this register
            if change.lock_path and register_lock.is_held(change.lock_path):
                lock_state = register_lock.get_lock_state(change.lock_path)
                holders = lock_state.holders if lock_state else set()
                error = (
                    f"Cannot remove register '{change.key}': "
                    f"locked by {len(holders)} tree(s)"
                )
                result.errors.append(error)
                result.blocked = True
            continue

        if change.change_type == ChangeType.PATH_CHANGED:
            # Warn if lock held on old path (lock becomes stale)
            if change.lock_path and register_lock.is_held(change.lock_path):
                lock_state = register_lock.get_lock_state(change.lock_path)
                mode = lock_state.mode if lock_state else "unknown"
                warning = (
                    f"Path changed for '{change.key}' while {mode} lock held. "
                    f"Lock on old path will be orphaned."
                )
                result.warnings.append(warning)
            continue

        if change.change_type == ChangeType.BUCKET_CHANGED:
            # Warn if lock held on old bucket/prefix
            if change.lock_path and register_lock.is_held(change.lock_path):
                lock_state = register_lock.get_lock_state(change.lock_path)
                mode = lock_state.mode if lock_state else "unknown"
                warning = (
                    f"Bucket/prefix changed for '{change.key}' while {mode} lock held. "
                    f"Lock on old location will be orphaned."
                )
                result.warnings.append(warning)
            continue

        if change.change_type == ChangeType.TYPE_CHANGED:
            # Block if lock held (type determines lock path format)
            if change.lock_path and register_lock.is_held(change.lock_path):
                lock_state = register_lock.get_lock_state(change.lock_path)
                holders = lock_state.holders if lock_state else set()
                error = (
                    f"Cannot change type of '{change.key}': "
                    f"locked by {len(holders)} tree(s)"
                )
                result.errors.append(error)
                result.blocked = True
            continue

        if change.change_type == ChangeType.PROTECT_ADDED:
            # Block if exclusive lock held (active writers)
            if change.lock_path:
                lock_state = register_lock.get_lock_state(change.lock_path)
                if lock_state and lock_state.mode == "exclusive":
                    error = (
                        f"Cannot add protect:true to '{change.key}': "
                        f"exclusive lock held by writer"
                    )
                    result.errors.append(error)
                    result.blocked = True
            continue

        if change.change_type == ChangeType.PROTECT_REMOVED:
            # Block if shared lock held (readers assume immutability)
            if change.lock_path:
                lock_state = register_lock.get_lock_state(change.lock_path)
                if lock_state and lock_state.mode == "shared":
                    holders = lock_state.holders
                    error = (
                        f"Cannot remove protect:true from '{change.key}': "
                        f"shared lock held by {len(holders)} reader(s)"
                    )
                    result.errors.append(error)
                    result.blocked = True
            continue

        if change.change_type == ChangeType.KIND_CHANGED:
            # Warn if lock held (might affect I/O behavior)
            if change.lock_path and register_lock.is_held(change.lock_path):
                lock_state = register_lock.get_lock_state(change.lock_path)
                mode = lock_state.mode if lock_state else "unknown"
                warning = (
                    f"Kind changed for '{change.key}' while {mode} lock held. "
                    f"May affect I/O behavior for active trees."
                )
                result.warnings.append(warning)
            continue

    # Log summary
    if result.blocked:
        logger.error(
            f"Hot reload blocked: {len(result.errors)} error(s), "
            f"{len(result.warnings)} warning(s)"
        )
    elif result.warnings:
        logger.warning(
            f"Hot reload proceeding with {len(result.warnings)} warning(s)"
        )
    elif changes:
        logger.info(f"Hot reload: {len(changes)} change(s), all safe")
    else:
        logger.debug("Hot reload: no registry changes detected")

    return result
