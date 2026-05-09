"""
RegisterLock - ENQ/DEQ style locking for permanent registers.

Implements readers-writer locks with FIFO wait queue for fair scheduling.
Prevents RAW, WAW, and WAR hazards across concurrent tree executions.

Design follows phase15-register-model-SPEC.md Section 3.

Key invariants:
- All-or-nothing acquisition: A tree holds ALL its locks or NONE
- FIFO queue: Prevents writer starvation from continuous readers
- Sorted lock order: Prevents deadlock via canonical ordering
- Single-threaded async: No mutex needed (coordinator is single-threaded)

Example:
    lock_mgr = RegisterLock()

    # Extract sorted locks from tree
    locks = sorted_lock_paths(tree, registry)

    # Try to acquire - returns True if granted, False if waiting
    if lock_mgr.try_acquire_all(locks, tree_exec_id):
        # Tree can execute
        ...
    else:
        # Tree added to wait queue
        ...

    # On completion
    woken = lock_mgr.dequeue(tree_exec_id)
    # woken contains tree_exec_ids that should retry acquisition
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from coordinator.models import DataRegistryEntry, JobTree

logger = logging.getLogger(__name__)


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class LockState:
    """
    State of a single lock.

    Attributes:
        mode: "shared" (multiple readers) or "exclusive" (single writer)
        holders: Set of tree_exec_ids currently holding this lock
    """

    mode: str  # "shared" or "exclusive"
    holders: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        """Validate mode."""
        if self.mode not in ("shared", "exclusive"):
            raise ValueError(f"Invalid lock mode: {self.mode}")


@dataclass
class LockRequest:
    """
    A pending lock request in the wait queue.

    Attributes:
        tree_exec_id: Tree execution requesting the lock
        mode: Requested lock mode
    """

    tree_exec_id: str
    mode: str  # "shared" or "exclusive"


# =============================================================================
# Lock Path Resolution
# =============================================================================


def lock_path(entry: DataRegistryEntry, registry_key: str) -> str | None:
    """
    Resolve register entry to canonical lock path.

    Different register types use different lock paths:
    - fs: Absolute filesystem path
    - minio: minio:{bucket}/{prefix}
    - temp: None (no locking needed)

    Args:
        entry: Data registry entry
        registry_key: Registry key (for error messages)

    Returns:
        Lock path string, or None if type doesn't require locking.

    Raises:
        ValueError: If entry type is unknown
    """
    if entry.type == "fs":
        if not entry.path:
            raise ValueError(f"fs entry '{registry_key}' missing path")
        return str(Path(entry.path).resolve())

    elif entry.type == "minio":
        if not entry.bucket:
            raise ValueError(f"minio entry '{registry_key}' missing bucket")
        prefix = entry.prefix or ""
        return f"minio:{entry.bucket}/{prefix}"

    elif entry.type == "temp":
        # Temp registers don't need locks - unique MinIO path per tree_exec_id
        return None

    else:
        raise ValueError(f"Unknown register type: {entry.type}")


def sorted_lock_paths(
    tree: JobTree, registry: dict[str, DataRegistryEntry]
) -> list[tuple[str, str]]:
    """
    Extract and sort all lock paths for a tree.

    Scans ALL jobs in the tree (not just leaf) to find all required locks.
    Sorting by path prevents deadlock (canonical lock ordering).

    Args:
        tree: JobTree to analyze
        registry: Data registry mapping

    Returns:
        List of (lock_path, mode) tuples sorted by lock_path.
        Mode is "exclusive" for writes, "shared" for reads.

    Example:
        >>> tree = JobTree(jobs=[job_a, job_b], ...)
        >>> sorted_lock_paths(tree, registry)
        [('/data/input.txt', 'shared'), ('/data/output.json', 'exclusive')]
    """
    # Collect locks with their modes
    # Use dict to handle case where same path appears in both reads and writes
    # (writes takes precedence - exclusive > shared)
    path_to_mode: dict[str, str] = {}

    for job in tree.jobs:
        # Process writes first (exclusive locks)
        for key in job.writes:
            entry = registry.get(key)
            if entry is None:
                logger.warning(f"Unknown registry key in writes: {key}")
                continue

            # Protected registers cannot be written to
            if getattr(entry, "protect", False):
                raise ValueError(
                    f"Job '{job.id}' cannot write to protected register '{key}'"
                )

            path = lock_path(entry, key)
            if path:
                path_to_mode[path] = "exclusive"

        # Process reads (shared locks, unless already exclusive)
        for key in job.reads:
            entry = registry.get(key)
            if entry is None:
                logger.warning(f"Unknown registry key in reads: {key}")
                continue

            path = lock_path(entry, key)
            if path and path not in path_to_mode:
                # Only add shared if not already exclusive
                path_to_mode[path] = "shared"

    # Sort by path for consistent ordering (deadlock prevention)
    sorted_locks = sorted(path_to_mode.items(), key=lambda x: x[0])

    logger.debug(
        f"Tree {tree.root.id}: {len(sorted_locks)} locks "
        f"({sum(1 for _, m in sorted_locks if m == 'exclusive')} exclusive, "
        f"{sum(1 for _, m in sorted_locks if m == 'shared')} shared)"
    )

    return sorted_locks


# =============================================================================
# RegisterLock - Main Lock Manager
# =============================================================================


class RegisterLock:
    """
    ENQ/DEQ style locking for permanent registers.

    Single-threaded (async), no mutex needed. All operations are synchronous
    and called from the coordinator's event loop.

    Implements readers-writer locks with FIFO wait queue:
    - Shared locks are compatible with other shared locks
    - Exclusive locks are incompatible with all other locks
    - FIFO queue prevents writer starvation

    All-or-nothing semantics:
    - try_acquire_all() either grants ALL locks or NONE
    - A waiting tree holds ZERO locks
    - This prevents unnecessary blocking of other trees

    Attributes:
        _locks: Current lock state per path
        _held_by_tree: Paths held by each tree (for fast dequeue)
        _wait_queue: FIFO queue of waiting trees per path
        _wake_callback: Called when a tree should retry acquisition
    """

    def __init__(
        self, wake_callback: Callable[[str], None] | None = None
    ) -> None:
        """
        Initialize lock manager.

        Args:
            wake_callback: Function called with tree_exec_id when a tree
                           should retry lock acquisition (woken from queue).
                           If None, woken trees are returned from dequeue().
        """
        # path -> LockState
        self._locks: dict[str, LockState] = {}

        # tree_exec_id -> set of held paths
        self._held_by_tree: dict[str, set[str]] = {}

        # path -> list of waiting LockRequests (FIFO order)
        self._wait_queue: dict[str, list[LockRequest]] = {}

        # Callback for waking trees
        self._wake_callback = wake_callback

    def try_acquire_all(
        self, locks: list[tuple[str, str]], tree_exec_id: str
    ) -> bool:
        """
        Atomically acquire all locks or none.

        Args:
            locks: List of (path, mode) tuples - MUST be sorted by path
            tree_exec_id: Tree execution requesting locks

        Returns:
            True if all locks granted (tree can dispatch)
            False if any lock denied (tree added to wait queue, holds NO locks)

        Critical: This is all-or-nothing. A tree waiting in queue holds
        zero locks, preventing unnecessary blocking of other trees.
        """
        if not locks:
            # No locks needed
            return True

        # Phase 1: Check all locks (no acquisition yet)
        first_blocker: tuple[str, str] | None = None

        for path, mode in locks:
            if not self._can_grant(path, mode, tree_exec_id):
                first_blocker = (path, mode)
                break

        if first_blocker:
            # At least one lock unavailable - add to wait queue for first blocker
            path, mode = first_blocker
            self._add_to_wait_queue(path, tree_exec_id, mode)

            logger.info(
                f"Tree {tree_exec_id}: waiting for {mode} lock on {path}"
            )
            return False

        # Phase 2: All available - acquire atomically
        for path, mode in locks:
            self._grant(path, mode, tree_exec_id)

        logger.info(
            f"Tree {tree_exec_id}: acquired {len(locks)} lock(s)"
        )
        return True

    def dequeue(self, tree_exec_id: str) -> list[str]:
        """
        Release all locks held by tree.

        Called when tree completes (success) or fails (error/timeout/crash).

        Args:
            tree_exec_id: Tree execution to release

        Returns:
            List of tree_exec_ids that were woken and should retry acquisition.
            Empty if wake_callback is set (callback handles notification).
        """
        woken_trees: set[str] = set()
        held_paths = self._held_by_tree.pop(tree_exec_id, set())

        if not held_paths:
            # Tree wasn't holding any locks (maybe never acquired or already released)
            # Also remove from any wait queues
            self._remove_from_wait_queues(tree_exec_id)
            return []

        for path in held_paths:
            lock = self._locks.get(path)
            if not lock:
                continue

            # Remove this tree from holders
            lock.holders.discard(tree_exec_id)

            if not lock.holders:
                # Lock is now free - delete it
                del self._locks[path]

                # Process wait queue for this path
                woken = self._process_wait_queue(path)
                woken_trees.update(woken)

        logger.info(
            f"Tree {tree_exec_id}: released {len(held_paths)} lock(s)"
        )

        # Notify woken trees
        if self._wake_callback:
            for tid in woken_trees:
                self._wake_callback(tid)
            return []

        return list(woken_trees)

    def get_held_locks(self, tree_exec_id: str) -> set[str]:
        """
        Get paths currently held by a tree.

        Args:
            tree_exec_id: Tree execution to query

        Returns:
            Set of lock paths held by the tree
        """
        return self._held_by_tree.get(tree_exec_id, set()).copy()

    def get_lock_state(self, path: str) -> LockState | None:
        """
        Get current state of a lock.

        Args:
            path: Lock path to query

        Returns:
            LockState if locked, None if free
        """
        return self._locks.get(path)

    def is_held(self, path: str) -> bool:
        """
        Check if a lock is currently held.

        Args:
            path: Lock path to check

        Returns:
            True if lock is held (by any tree), False if free
        """
        return path in self._locks

    def get_wait_queue(self, path: str) -> list[LockRequest]:
        """
        Get waiting trees for a path.

        Args:
            path: Lock path to query

        Returns:
            List of waiting LockRequests (FIFO order)
        """
        return self._wait_queue.get(path, []).copy()

    def is_waiting(self, tree_exec_id: str) -> bool:
        """
        Check if a tree is waiting for any lock.

        Args:
            tree_exec_id: Tree execution to check

        Returns:
            True if tree is in any wait queue
        """
        for queue in self._wait_queue.values():
            if any(req.tree_exec_id == tree_exec_id for req in queue):
                return True
        return False

    def get_all_locks(self) -> dict[str, LockState]:
        """
        Get all current locks (for debugging/status).

        Returns:
            Dict mapping path -> LockState
        """
        return {p: LockState(l.mode, l.holders.copy()) for p, l in self._locks.items()}

    def get_statistics(self) -> dict[str, int]:
        """
        Get lock manager statistics.

        Returns:
            Dict with counts of locks, waiters, etc.
        """
        total_waiters = sum(len(q) for q in self._wait_queue.values())
        exclusive_locks = sum(1 for l in self._locks.values() if l.mode == "exclusive")
        shared_locks = sum(1 for l in self._locks.values() if l.mode == "shared")

        return {
            "total_locks": len(self._locks),
            "exclusive_locks": exclusive_locks,
            "shared_locks": shared_locks,
            "trees_holding_locks": len(self._held_by_tree),
            "paths_with_waiters": len(self._wait_queue),
            "total_waiters": total_waiters,
        }

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _can_grant(self, path: str, mode: str, tree_exec_id: str) -> bool:
        """
        Check if lock CAN be granted (without actually granting).

        FIFO enforcement: If anyone is waiting in queue, new requests
        must wait behind them - even if the lock mode would be compatible.
        This prevents writer starvation.

        Args:
            path: Lock path
            mode: Requested mode ("shared" or "exclusive")
            tree_exec_id: Requesting tree (for self-check)

        Returns:
            True if lock can be granted, False if must wait
        """
        # Check if tree already holds this lock (re-entrant check)
        held = self._held_by_tree.get(tree_exec_id, set())
        if path in held:
            # Already holding - check mode compatibility
            current = self._locks.get(path)
            if current and current.mode == mode:
                return True  # Same mode, OK
            # Mode upgrade/downgrade not supported
            logger.warning(
                f"Tree {tree_exec_id} already holds {path} as {current.mode}, "
                f"cannot acquire as {mode}"
            )
            return False

        # FIFO: Must wait behind anyone already in queue
        queue = self._wait_queue.get(path, [])
        if queue:
            # Someone is waiting - must join queue
            return False

        # Check current lock state
        current = self._locks.get(path)

        if current is None:
            # Lock is free
            return True

        if mode == "shared" and current.mode == "shared":
            # Multiple readers allowed
            return True

        # Exclusive requested, or current is exclusive
        return False

    def _grant(self, path: str, mode: str, tree_exec_id: str) -> None:
        """
        Grant lock to tree.

        Precondition: _can_grant() returned True.

        Args:
            path: Lock path
            mode: Lock mode
            tree_exec_id: Tree to grant lock to
        """
        current = self._locks.get(path)

        if current and mode == "shared" and current.mode == "shared":
            # Add to existing shared lock
            current.holders.add(tree_exec_id)
        else:
            # New lock (or upgrading from empty)
            self._locks[path] = LockState(mode=mode, holders={tree_exec_id})

        # Track in held_by_tree for fast dequeue
        self._held_by_tree.setdefault(tree_exec_id, set()).add(path)

        logger.debug(f"Granted {mode} lock on {path} to {tree_exec_id}")

    def _add_to_wait_queue(self, path: str, tree_exec_id: str, mode: str) -> None:
        """
        Add tree to wait queue for a path.

        Args:
            path: Lock path to wait for
            tree_exec_id: Tree to add
            mode: Requested mode
        """
        queue = self._wait_queue.setdefault(path, [])

        # Check if already in queue (shouldn't happen, but defensive)
        if any(req.tree_exec_id == tree_exec_id for req in queue):
            logger.warning(f"Tree {tree_exec_id} already in wait queue for {path}")
            return

        queue.append(LockRequest(tree_exec_id=tree_exec_id, mode=mode))
        logger.debug(f"Added {tree_exec_id} to wait queue for {path} ({mode})")

    def _remove_from_wait_queues(self, tree_exec_id: str) -> None:
        """
        Remove tree from all wait queues.

        Called when tree is cancelled or fails before acquiring locks.

        Args:
            tree_exec_id: Tree to remove
        """
        for path, queue in list(self._wait_queue.items()):
            original_len = len(queue)
            self._wait_queue[path] = [
                req for req in queue if req.tree_exec_id != tree_exec_id
            ]

            if len(self._wait_queue[path]) < original_len:
                logger.debug(f"Removed {tree_exec_id} from wait queue for {path}")

            # Clean up empty queues
            if not self._wait_queue[path]:
                del self._wait_queue[path]

    def _process_wait_queue(self, path: str) -> set[str]:
        """
        Process waiting trees after lock release.

        FIFO with batching: Grant to consecutive compatible waiters.
        Once an exclusive waiter is encountered, stop (they get next turn).

        Args:
            path: Lock path that was released

        Returns:
            Set of tree_exec_ids that were woken (should retry full acquisition)
        """
        queue = self._wait_queue.get(path)
        if not queue:
            return set()

        woken: set[str] = set()
        current = self._locks.get(path)  # Should be None after release

        indices_to_remove: list[int] = []

        for i, req in enumerate(queue):
            if current is None:
                # Lock is free - grant to first waiter
                self._grant(path, req.mode, req.tree_exec_id)
                current = self._locks[path]
                indices_to_remove.append(i)
                woken.add(req.tree_exec_id)

            elif req.mode == "shared" and current.mode == "shared":
                # Batch consecutive shared waiters
                current.holders.add(req.tree_exec_id)
                self._held_by_tree.setdefault(req.tree_exec_id, set()).add(path)
                indices_to_remove.append(i)
                woken.add(req.tree_exec_id)
                logger.debug(
                    f"Batched shared lock on {path} to {req.tree_exec_id}"
                )

            else:
                # Incompatible waiter (exclusive after shared, or any after exclusive)
                # Stop here - FIFO means this waiter gets next turn
                break

        # Remove woken from queue (reverse to preserve indices)
        for i in reversed(indices_to_remove):
            queue.pop(i)

        # Clean up empty queue
        if not queue:
            del self._wait_queue[path]

        if woken:
            logger.info(f"Woke {len(woken)} tree(s) waiting for {path}")

        return woken
