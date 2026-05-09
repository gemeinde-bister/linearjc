"""
Unit tests for RegisterLock - ENQ/DEQ style locking.

Tests cover:
- Basic lock acquisition (shared/exclusive)
- Compatibility matrix (shared+shared OK, exclusive+any blocks)
- FIFO wait queue behavior
- All-or-nothing semantics
- Deadlock prevention via sorted ordering
- Lock path resolution

See phase15-register-model-SPEC.md Section 3 for specification.
"""
import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import DataRegistryEntry, Job, JobSchedule, JobRun, JobTree
from coordinator_v2.register_lock import (
    RegisterLock,
    LockState,
    LockRequest,
    lock_path,
    sorted_lock_paths,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def lock_mgr() -> RegisterLock:
    """Create a fresh RegisterLock instance."""
    return RegisterLock()


@pytest.fixture
def sample_registry() -> dict[str, DataRegistryEntry]:
    """Sample data registry for testing."""
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


def make_job(
    job_id: str, reads: list[str], writes: list[str], depends: list[str] = None
) -> Job:
    """Create a minimal Job for testing."""
    return Job(
        id=job_id,
        version="1.0.0",
        reads=reads,
        writes=writes,
        depends=depends or [],
        schedule=JobSchedule(min_daily=1, max_daily=1),
        run=JobRun(user="test"),
    )


def make_tree(jobs: list[Job]) -> JobTree:
    """Create a JobTree from list of jobs."""
    return JobTree(
        root=jobs[0],
        jobs=jobs,
        min_daily=1,
        max_daily=1,
    )


# =============================================================================
# Test: LockState and LockRequest
# =============================================================================


class TestLockState:
    """Tests for LockState dataclass."""

    def test_shared_mode_valid(self):
        """Shared mode is valid."""
        state = LockState(mode="shared", holders={"tree1"})
        assert state.mode == "shared"
        assert "tree1" in state.holders

    def test_exclusive_mode_valid(self):
        """Exclusive mode is valid."""
        state = LockState(mode="exclusive", holders={"tree1"})
        assert state.mode == "exclusive"

    def test_invalid_mode_raises(self):
        """Invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="Invalid lock mode"):
            LockState(mode="invalid", holders=set())

    def test_default_holders_empty_set(self):
        """Default holders is empty set."""
        state = LockState(mode="shared")
        assert state.holders == set()


# =============================================================================
# Test: lock_path() function
# =============================================================================


class TestLockPath:
    """Tests for lock_path resolution."""

    def test_fs_type_returns_resolved_path(self):
        """FS type returns absolute resolved path."""
        entry = DataRegistryEntry(type="fs", path="/data/file.txt", kind="file")
        result = lock_path(entry, "test_key")
        assert result == "/data/file.txt"

    def test_minio_type_returns_bucket_prefix(self):
        """MinIO type returns minio:bucket/prefix format."""
        entry = DataRegistryEntry(
            type="minio", bucket="mybucket", prefix="path/to/", kind="dir"
        )
        result = lock_path(entry, "test_key")
        assert result == "minio:mybucket/path/to/"

    def test_minio_no_prefix(self):
        """MinIO with no prefix returns bucket only."""
        entry = DataRegistryEntry(type="minio", bucket="mybucket", kind="file")
        result = lock_path(entry, "test_key")
        assert result == "minio:mybucket/"

    def test_temp_type_returns_none(self):
        """Temp type returns None (no locking needed)."""
        entry = DataRegistryEntry(type="temp", kind="file")
        result = lock_path(entry, "test_key")
        assert result is None

    def test_unknown_type_raises(self):
        """Unknown type raises ValueError."""
        from unittest.mock import MagicMock

        # Create mock entry with invalid type
        entry = MagicMock()
        entry.type = "unknown"
        entry.path = None
        entry.bucket = None
        entry.prefix = None
        entry.protect = False
        entry.kind = "file"

        with pytest.raises(ValueError, match="Unknown register type"):
            lock_path(entry, "test_key")


# =============================================================================
# Test: sorted_lock_paths() function
# =============================================================================


class TestSortedLockPaths:
    """Tests for sorted_lock_paths extraction."""

    def test_extracts_reads_as_shared(self, sample_registry):
        """Reads are extracted as shared locks."""
        job = make_job("reader", reads=["input_db"], writes=[])
        tree = make_tree([job])

        locks = sorted_lock_paths(tree, sample_registry)

        assert len(locks) == 1
        path, mode = locks[0]
        assert path == "/data/input.db"
        assert mode == "shared"

    def test_extracts_writes_as_exclusive(self, sample_registry):
        """Writes are extracted as exclusive locks."""
        job = make_job("writer", reads=[], writes=["output_json"])
        tree = make_tree([job])

        locks = sorted_lock_paths(tree, sample_registry)

        assert len(locks) == 1
        path, mode = locks[0]
        assert path == "/data/output.json"
        assert mode == "exclusive"

    def test_writes_override_reads(self, sample_registry):
        """When same register is in both reads and writes, exclusive wins."""
        job = make_job("rw", reads=["output_json"], writes=["output_json"])
        tree = make_tree([job])

        locks = sorted_lock_paths(tree, sample_registry)

        assert len(locks) == 1
        path, mode = locks[0]
        assert path == "/data/output.json"
        assert mode == "exclusive"

    def test_temp_registers_excluded(self, sample_registry):
        """Temp registers don't appear in lock list."""
        job = make_job("temp_user", reads=["temp_data"], writes=["temp_data"])
        tree = make_tree([job])

        locks = sorted_lock_paths(tree, sample_registry)

        assert len(locks) == 0

    def test_minio_registers_included(self, sample_registry):
        """MinIO registers are included with minio: prefix."""
        job = make_job("minio_user", reads=[], writes=["minio_archive"])
        tree = make_tree([job])

        locks = sorted_lock_paths(tree, sample_registry)

        assert len(locks) == 1
        path, mode = locks[0]
        assert path == "minio:archives/daily/"
        assert mode == "exclusive"

    def test_sorted_by_path(self, sample_registry):
        """Locks are sorted by path for deadlock prevention."""
        job = make_job(
            "multi", reads=["input_db"], writes=["output_json", "minio_archive"]
        )
        tree = make_tree([job])

        locks = sorted_lock_paths(tree, sample_registry)

        paths = [p for p, _ in locks]
        assert paths == sorted(paths)

    def test_multi_job_tree_collects_all_locks(self, sample_registry):
        """All jobs in tree contribute to lock list."""
        job1 = make_job("job1", reads=["input_db"], writes=[], depends=[])
        job2 = make_job("job2", reads=[], writes=["output_json"], depends=["job1"])
        tree = make_tree([job1, job2])

        locks = sorted_lock_paths(tree, sample_registry)

        assert len(locks) == 2
        paths = [p for p, _ in locks]
        assert "/data/input.db" in paths
        assert "/data/output.json" in paths

    def test_protected_register_in_writes_raises(self, sample_registry):
        """Writing to protected register raises ValueError."""
        job = make_job("bad_writer", reads=[], writes=["input_db"])
        tree = make_tree([job])

        with pytest.raises(ValueError, match="cannot write to protected register"):
            sorted_lock_paths(tree, sample_registry)


# =============================================================================
# Test: Basic Lock Acquisition
# =============================================================================


class TestBasicLockAcquisition:
    """Tests for basic try_acquire_all behavior."""

    def test_single_exclusive_lock_granted(self, lock_mgr):
        """Single exclusive lock on free path is granted."""
        locks = [("/data/file.txt", "exclusive")]

        result = lock_mgr.try_acquire_all(locks, "tree1")

        assert result is True
        assert lock_mgr.get_held_locks("tree1") == {"/data/file.txt"}

    def test_single_shared_lock_granted(self, lock_mgr):
        """Single shared lock on free path is granted."""
        locks = [("/data/file.txt", "shared")]

        result = lock_mgr.try_acquire_all(locks, "tree1")

        assert result is True
        assert lock_mgr.get_held_locks("tree1") == {"/data/file.txt"}

    def test_empty_locks_returns_true(self, lock_mgr):
        """Empty lock list returns True (no locks needed)."""
        result = lock_mgr.try_acquire_all([], "tree1")

        assert result is True
        assert lock_mgr.get_held_locks("tree1") == set()

    def test_multiple_locks_all_granted(self, lock_mgr):
        """Multiple locks on free paths are all granted."""
        locks = [
            ("/data/a.txt", "exclusive"),
            ("/data/b.txt", "shared"),
            ("/data/c.txt", "exclusive"),
        ]

        result = lock_mgr.try_acquire_all(locks, "tree1")

        assert result is True
        held = lock_mgr.get_held_locks("tree1")
        assert len(held) == 3


# =============================================================================
# Test: Lock Compatibility Matrix
# =============================================================================


class TestLockCompatibility:
    """Tests for lock compatibility rules."""

    def test_shared_plus_shared_compatible(self, lock_mgr):
        """Two shared locks on same path are compatible."""
        locks = [("/data/file.txt", "shared")]

        lock_mgr.try_acquire_all(locks, "tree1")
        result = lock_mgr.try_acquire_all(locks, "tree2")

        assert result is True
        state = lock_mgr.get_lock_state("/data/file.txt")
        assert state.mode == "shared"
        assert "tree1" in state.holders
        assert "tree2" in state.holders

    def test_exclusive_blocks_exclusive(self, lock_mgr):
        """Exclusive lock blocks another exclusive on same path."""
        locks = [("/data/file.txt", "exclusive")]

        lock_mgr.try_acquire_all(locks, "tree1")
        result = lock_mgr.try_acquire_all(locks, "tree2")

        assert result is False
        assert lock_mgr.is_waiting("tree2")

    def test_exclusive_blocks_shared(self, lock_mgr):
        """Exclusive lock blocks shared lock on same path."""
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")
        result = lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree2")

        assert result is False
        assert lock_mgr.is_waiting("tree2")

    def test_shared_blocks_exclusive(self, lock_mgr):
        """Shared lock blocks exclusive lock on same path."""
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree1")
        result = lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree2")

        assert result is False
        assert lock_mgr.is_waiting("tree2")


# =============================================================================
# Test: All-or-Nothing Semantics
# =============================================================================


class TestAllOrNothing:
    """Tests for all-or-nothing lock acquisition."""

    def test_partial_block_acquires_nothing(self, lock_mgr):
        """If any lock blocked, tree holds NO locks."""
        # Tree1 holds B
        lock_mgr.try_acquire_all([("/data/b.txt", "exclusive")], "tree1")

        # Tree2 wants A and B - B is blocked
        locks = [("/data/a.txt", "exclusive"), ("/data/b.txt", "exclusive")]
        result = lock_mgr.try_acquire_all(locks, "tree2")

        assert result is False
        # Tree2 should hold NOTHING, not even A
        assert lock_mgr.get_held_locks("tree2") == set()
        # A should still be free
        assert lock_mgr.get_lock_state("/data/a.txt") is None

    def test_success_acquires_all(self, lock_mgr):
        """On success, all locks are acquired atomically."""
        locks = [
            ("/data/a.txt", "exclusive"),
            ("/data/b.txt", "exclusive"),
            ("/data/c.txt", "shared"),
        ]

        result = lock_mgr.try_acquire_all(locks, "tree1")

        assert result is True
        held = lock_mgr.get_held_locks("tree1")
        assert len(held) == 3


# =============================================================================
# Test: FIFO Wait Queue
# =============================================================================


class TestFIFOWaitQueue:
    """Tests for FIFO wait queue behavior."""

    def test_waiter_added_to_queue(self, lock_mgr):
        """Blocked tree is added to wait queue."""
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree2")

        queue = lock_mgr.get_wait_queue("/data/file.txt")
        assert len(queue) == 1
        assert queue[0].tree_exec_id == "tree2"
        assert queue[0].mode == "exclusive"

    def test_queue_is_fifo(self, lock_mgr):
        """Wait queue is processed in FIFO order."""
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree2")
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree3")

        queue = lock_mgr.get_wait_queue("/data/file.txt")
        assert [q.tree_exec_id for q in queue] == ["tree2", "tree3"]

        # Release tree1 - tree2 should get lock, not tree3
        woken = lock_mgr.dequeue("tree1")
        assert "tree2" in woken
        assert "tree3" not in woken

        # tree2 now holds lock
        state = lock_mgr.get_lock_state("/data/file.txt")
        assert "tree2" in state.holders

    def test_fifo_prevents_starvation(self, lock_mgr):
        """FIFO prevents writer starvation by continuous readers."""
        # Tree1 holds shared
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree1")

        # Tree2 wants exclusive - must wait
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree2")

        # Tree3 wants shared - must wait BEHIND tree2 (FIFO)
        result = lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree3")
        assert result is False  # Not granted despite compatible mode

        queue = lock_mgr.get_wait_queue("/data/file.txt")
        assert [q.tree_exec_id for q in queue] == ["tree2", "tree3"]


# =============================================================================
# Test: Shared Lock Batching
# =============================================================================


class TestSharedBatching:
    """Tests for batching consecutive shared waiters."""

    def test_consecutive_shared_waiters_batched(self, lock_mgr):
        """Consecutive shared waiters are woken together."""
        # Tree1 holds exclusive
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")

        # Tree2, tree3, tree4 all want shared
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree2")
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree3")
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree4")

        # Release tree1 - all shared waiters should be woken
        woken = lock_mgr.dequeue("tree1")
        assert set(woken) == {"tree2", "tree3", "tree4"}

        # All should hold shared lock
        state = lock_mgr.get_lock_state("/data/file.txt")
        assert state.mode == "shared"
        assert state.holders == {"tree2", "tree3", "tree4"}

    def test_exclusive_stops_shared_batching(self, lock_mgr):
        """Exclusive waiter stops shared batching."""
        # Tree1 holds exclusive
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")

        # Queue: shared, shared, exclusive, shared
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree2")
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree3")
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree4")
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree5")

        # Release tree1 - only tree2 and tree3 woken (stop at exclusive)
        woken = lock_mgr.dequeue("tree1")
        assert set(woken) == {"tree2", "tree3"}

        # tree4 and tree5 still waiting
        queue = lock_mgr.get_wait_queue("/data/file.txt")
        assert [q.tree_exec_id for q in queue] == ["tree4", "tree5"]


# =============================================================================
# Test: Lock Release (dequeue)
# =============================================================================


class TestDequeue:
    """Tests for lock release via dequeue."""

    def test_releases_all_held_locks(self, lock_mgr):
        """Dequeue releases all locks held by tree."""
        locks = [
            ("/data/a.txt", "exclusive"),
            ("/data/b.txt", "shared"),
            ("/data/c.txt", "exclusive"),
        ]
        lock_mgr.try_acquire_all(locks, "tree1")

        lock_mgr.dequeue("tree1")

        assert lock_mgr.get_held_locks("tree1") == set()
        assert lock_mgr.get_lock_state("/data/a.txt") is None
        assert lock_mgr.get_lock_state("/data/b.txt") is None
        assert lock_mgr.get_lock_state("/data/c.txt") is None

    def test_removes_from_wait_queue(self, lock_mgr):
        """Dequeue removes tree from wait queues if waiting."""
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree2")

        # tree2 is waiting - dequeue should remove from queue
        lock_mgr.dequeue("tree2")

        assert not lock_mgr.is_waiting("tree2")
        queue = lock_mgr.get_wait_queue("/data/file.txt")
        assert len(queue) == 0

    def test_dequeue_nonexistent_tree_safe(self, lock_mgr):
        """Dequeue on tree with no locks is safe."""
        woken = lock_mgr.dequeue("nonexistent")
        assert woken == []

    def test_shared_lock_persists_after_one_holder_releases(self, lock_mgr):
        """Shared lock persists while other holders remain."""
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree1")
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree2")

        lock_mgr.dequeue("tree1")

        # Lock should still exist with tree2
        state = lock_mgr.get_lock_state("/data/file.txt")
        assert state is not None
        assert state.mode == "shared"
        assert state.holders == {"tree2"}


# =============================================================================
# Test: Wake Callback
# =============================================================================


class TestWakeCallback:
    """Tests for wake callback behavior."""

    def test_callback_called_on_wake(self):
        """Wake callback is called for woken trees."""
        woken_trees = []

        def callback(tree_exec_id: str):
            woken_trees.append(tree_exec_id)

        lock_mgr = RegisterLock(wake_callback=callback)

        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree2")

        lock_mgr.dequeue("tree1")

        assert woken_trees == ["tree2"]

    def test_callback_prevents_return_value(self):
        """When callback is set, dequeue returns empty list."""
        lock_mgr = RegisterLock(wake_callback=lambda x: None)

        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree2")

        woken = lock_mgr.dequeue("tree1")

        assert woken == []


# =============================================================================
# Test: Statistics
# =============================================================================


class TestStatistics:
    """Tests for lock manager statistics."""

    def test_statistics_empty(self, lock_mgr):
        """Statistics for empty lock manager."""
        stats = lock_mgr.get_statistics()
        assert stats["total_locks"] == 0
        assert stats["exclusive_locks"] == 0
        assert stats["shared_locks"] == 0
        assert stats["trees_holding_locks"] == 0
        assert stats["paths_with_waiters"] == 0
        assert stats["total_waiters"] == 0

    def test_statistics_with_locks(self, lock_mgr):
        """Statistics with various locks."""
        lock_mgr.try_acquire_all([("/data/a.txt", "exclusive")], "tree1")
        lock_mgr.try_acquire_all([("/data/b.txt", "shared")], "tree2")
        lock_mgr.try_acquire_all([("/data/b.txt", "shared")], "tree3")
        lock_mgr.try_acquire_all([("/data/a.txt", "exclusive")], "tree4")  # Waits

        stats = lock_mgr.get_statistics()
        assert stats["total_locks"] == 2
        assert stats["exclusive_locks"] == 1
        assert stats["shared_locks"] == 1
        assert stats["trees_holding_locks"] == 3
        assert stats["paths_with_waiters"] == 1
        assert stats["total_waiters"] == 1


# =============================================================================
# Test: Deadlock Prevention
# =============================================================================


class TestDeadlockPrevention:
    """Tests demonstrating deadlock prevention via sorted ordering."""

    def test_sorted_ordering_prevents_deadlock(self, lock_mgr):
        """
        Sorted lock ordering prevents deadlock.

        Without sorting:
          Tree1: acquires A, waits for B
          Tree2: acquires B, waits for A
          -> Deadlock!

        With sorting (both acquire A first, then B):
          Tree1: acquires A, acquires B
          Tree2: waits for A (Tree1 holds it)
          Tree1 finishes, releases A, B
          Tree2: acquires A, acquires B
          -> No deadlock!
        """
        # Both trees want A and B, but in sorted order
        locks_sorted = [("/data/a.txt", "exclusive"), ("/data/b.txt", "exclusive")]

        # Tree1 gets both
        result1 = lock_mgr.try_acquire_all(locks_sorted, "tree1")
        assert result1 is True

        # Tree2 must wait (all-or-nothing, and A is held)
        result2 = lock_mgr.try_acquire_all(locks_sorted, "tree2")
        assert result2 is False
        assert lock_mgr.get_held_locks("tree2") == set()  # Holds nothing

        # Release tree1
        woken = lock_mgr.dequeue("tree1")
        assert "tree2" in woken

        # Now tree2 can get both locks
        result3 = lock_mgr.try_acquire_all(locks_sorted, "tree2")
        assert result3 is True


# =============================================================================
# Test: Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and unusual scenarios."""

    def test_same_tree_acquires_same_lock_twice(self, lock_mgr):
        """Tree trying to acquire already-held lock fails."""
        locks = [("/data/file.txt", "exclusive")]

        lock_mgr.try_acquire_all(locks, "tree1")

        # Same tree, same locks - should still succeed (re-entrant for same mode)
        result = lock_mgr.try_acquire_all(locks, "tree1")
        assert result is True  # Already holds it

    def test_tree_upgrade_mode_fails(self, lock_mgr):
        """Tree cannot upgrade from shared to exclusive."""
        lock_mgr.try_acquire_all([("/data/file.txt", "shared")], "tree1")

        # Try to upgrade to exclusive
        result = lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")
        assert result is False

    def test_many_trees_waiting(self, lock_mgr):
        """Many trees can wait on same path."""
        lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], "tree1")

        for i in range(10):
            lock_mgr.try_acquire_all([("/data/file.txt", "exclusive")], f"tree{i+2}")

        queue = lock_mgr.get_wait_queue("/data/file.txt")
        assert len(queue) == 10

    def test_different_paths_no_conflict(self, lock_mgr):
        """Different paths don't conflict."""
        lock_mgr.try_acquire_all([("/data/a.txt", "exclusive")], "tree1")

        result = lock_mgr.try_acquire_all([("/data/b.txt", "exclusive")], "tree2")
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
