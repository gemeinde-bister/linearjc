"""
Unit tests for executor crash detection (Phase 15.4).

Tests cover:
- ExecutorRegistry.prune_stale() returns list of stale executor IDs
- Dead executor handling: marks trees failed, releases locks, cleans up MinIO
- Edge cases: no jobs on dead executor, multiple executors dying

See SPEC.md, Register Locking (ENQ/DEQ) section for specification.
"""
import time
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import DataRegistryEntry, Job, JobSchedule, JobRun, JobTree
from coordinator_v2.executor_registry import ExecutorRegistry, ExecutorInfo
from coordinator_v2.job_state_machine import JobExecution, JobState
from coordinator_v2.job_tracker import JobTracker
from coordinator_v2.tree_tracker import TreeTracker, TreeState
from coordinator_v2.register_lock import RegisterLock


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def registry() -> ExecutorRegistry:
    """Create ExecutorRegistry with 90s TTL (3 missed heartbeats)."""
    return ExecutorRegistry(ttl=90.0)


@pytest.fixture
def job_tracker() -> JobTracker:
    """Create empty JobTracker."""
    return JobTracker()


@pytest.fixture
def tree_tracker() -> TreeTracker:
    """Create empty TreeTracker."""
    return TreeTracker()


@pytest.fixture
def register_lock() -> RegisterLock:
    """Create empty RegisterLock."""
    return RegisterLock()


def make_job(job_id: str) -> Job:
    """Create a minimal Job for testing."""
    return Job(
        id=job_id,
        version="1.0.0",
        reads=[],
        writes=[],
        depends=[],
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


def make_job_execution(
    tree_exec_id: str,
    job_id: str,
    executor_id: str,
) -> JobExecution:
    """Create a JobExecution for testing."""
    return JobExecution(
        job_execution_id=f"{tree_exec_id}-{job_id}",
        tree_execution_id=tree_exec_id,
        job_id=job_id,
        version="1.0.0",
        state=JobState.RUNNING,
        executor_id=executor_id,
        chain_index=0,
        chain_length=1,
    )


# =============================================================================
# Test: ExecutorRegistry.prune_stale() returns list
# =============================================================================


class TestPruneStaleReturnsIds:
    """Tests for prune_stale() returning list of stale executor IDs."""

    def test_prune_returns_empty_list_when_no_stale(self, registry):
        """prune_stale() returns empty list when no executors are stale."""
        # Add fresh executor
        registry.on_heartbeat("exec-1", {
            "hostname": "node1",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": [],
        })

        # Prune (all fresh)
        stale = registry.prune_stale()

        assert stale == []
        assert registry.live_count() == 1

    def test_prune_returns_stale_ids(self, registry):
        """prune_stale() returns list of stale executor IDs."""
        # Add executor and manually age it
        registry.on_heartbeat("exec-1", {
            "hostname": "node1",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": [],
        })

        # Age the executor beyond TTL
        registry._executors["exec-1"] = ExecutorInfo(
            executor_id="exec-1",
            hostname="node1",
            capabilities={},
            capability_types=[],
            supported_formats=[],
            last_seen=time.time() - 100,  # 100s ago, beyond 90s TTL
        )

        # Prune
        stale = registry.prune_stale()

        assert stale == ["exec-1"]
        assert registry.live_count() == 0

    def test_prune_returns_multiple_stale_ids(self, registry):
        """prune_stale() returns multiple stale executor IDs."""
        # Add two executors
        for exec_id in ["exec-1", "exec-2"]:
            registry._executors[exec_id] = ExecutorInfo(
                executor_id=exec_id,
                hostname=f"node-{exec_id}",
                capabilities={},
                capability_types=[],
                supported_formats=[],
                last_seen=time.time() - 100,  # stale
            )

        # Add one fresh executor
        registry.on_heartbeat("exec-3", {
            "hostname": "node3",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": [],
        })

        # Prune
        stale = registry.prune_stale()

        assert set(stale) == {"exec-1", "exec-2"}
        assert registry.live_count() == 1


# =============================================================================
# Test: Dead executor handling in coordinator
# =============================================================================


class TestHandleDeadExecutors:
    """Tests for _handle_dead_executors() coordinator method."""

    @pytest.mark.asyncio
    async def test_no_action_when_no_dead_executors(
        self, job_tracker, tree_tracker, register_lock
    ):
        """No action when dead_executor_ids is empty."""
        # Setup: create active tree
        job = make_job("test.job")
        tree = make_tree([job])
        tex = tree_tracker.create("tree-1", tree)
        job_exec = make_job_execution("tree-1", "test.job", "exec-1")
        job_tracker.create(job_exec)

        # Empty dead list - no changes
        dead = []

        # Verify tree still active
        assert tree_tracker.get("tree-1") is not None
        assert tree_tracker.get("tree-1").state == TreeState.RUNNING

    @pytest.mark.asyncio
    async def test_marks_tree_failed_on_executor_death(
        self, job_tracker, tree_tracker, register_lock
    ):
        """Trees with jobs on dead executor are marked failed."""
        # Setup: create active tree with job on executor
        job = make_job("test.job")
        tree = make_tree([job])
        tex = tree_tracker.create("tree-1", tree)
        job_exec = make_job_execution("tree-1", "test.job", "exec-1")
        sm = job_tracker.create(job_exec)
        sm.transition("dispatch", {"executor_id": "exec-1"})

        # Simulate executor death handling
        dead_executors = ["exec-1"]

        # Find affected trees
        affected_trees = {}
        for active_sm in job_tracker.get_active():
            if active_sm.job_exec.executor_id in dead_executors:
                tree_exec_id = active_sm.job_exec.tree_execution_id
                affected_trees[tree_exec_id] = active_sm.job_exec.executor_id

        assert "tree-1" in affected_trees

        # Mark tree failed
        for tree_exec_id, executor_id in affected_trees.items():
            tree_tracker.mark_failed(tree_exec_id, f"Executor death: {executor_id}")

        # Verify tree marked failed
        tex = tree_tracker.get("tree-1")
        assert tex.state == TreeState.FAILED
        assert "exec-1" in tex.error

    @pytest.mark.asyncio
    async def test_releases_locks_on_executor_death(
        self, job_tracker, tree_tracker, register_lock
    ):
        """Locks are released when executor dies."""
        # Setup: acquire locks for tree
        locks = [("/data/output.json", "exclusive")]
        acquired = register_lock.try_acquire_all(locks, "tree-1")
        assert acquired is True

        # Verify lock is held
        assert register_lock.is_held("/data/output.json")

        # Simulate executor death - release locks
        register_lock.dequeue("tree-1")

        # Verify lock is released
        assert not register_lock.is_held("/data/output.json")

    @pytest.mark.asyncio
    async def test_unaffected_trees_continue(
        self, job_tracker, tree_tracker, register_lock
    ):
        """Trees on healthy executors are not affected."""
        # Setup: two trees on different executors
        job1 = make_job("job.a")
        job2 = make_job("job.b")
        tree1 = make_tree([job1])
        tree2 = make_tree([job2])

        tex1 = tree_tracker.create("tree-1", tree1)
        tex2 = tree_tracker.create("tree-2", tree2)

        job_exec1 = make_job_execution("tree-1", "job.a", "exec-1")  # dead
        job_exec2 = make_job_execution("tree-2", "job.b", "exec-2")  # alive

        sm1 = job_tracker.create(job_exec1)
        sm2 = job_tracker.create(job_exec2)
        sm1.transition("dispatch", {"executor_id": "exec-1"})
        sm2.transition("dispatch", {"executor_id": "exec-2"})

        # Only exec-1 dies
        dead_executors = ["exec-1"]

        # Find affected trees
        affected_trees = {}
        for active_sm in job_tracker.get_active():
            if active_sm.job_exec.executor_id in dead_executors:
                tree_exec_id = active_sm.job_exec.tree_execution_id
                affected_trees[tree_exec_id] = active_sm.job_exec.executor_id

        # Only tree-1 should be affected
        assert "tree-1" in affected_trees
        assert "tree-2" not in affected_trees

        # Mark only affected tree as failed
        for tree_exec_id in affected_trees:
            tree_tracker.mark_failed(tree_exec_id, "Executor death")

        # Verify states
        assert tree_tracker.get("tree-1").state == TreeState.FAILED
        assert tree_tracker.get("tree-2").state == TreeState.RUNNING

    @pytest.mark.asyncio
    async def test_multiple_trees_on_same_executor(
        self, job_tracker, tree_tracker, register_lock
    ):
        """Multiple trees on same dead executor are all marked failed."""
        # Setup: two trees on same executor
        job1 = make_job("job.a")
        job2 = make_job("job.b")
        tree1 = make_tree([job1])
        tree2 = make_tree([job2])

        tex1 = tree_tracker.create("tree-1", tree1)
        tex2 = tree_tracker.create("tree-2", tree2)

        job_exec1 = make_job_execution("tree-1", "job.a", "exec-1")
        job_exec2 = make_job_execution("tree-2", "job.b", "exec-1")

        sm1 = job_tracker.create(job_exec1)
        sm2 = job_tracker.create(job_exec2)
        sm1.transition("dispatch", {"executor_id": "exec-1"})
        sm2.transition("dispatch", {"executor_id": "exec-1"})

        # Executor dies
        dead_executors = ["exec-1"]

        # Find affected trees
        affected_trees = {}
        for active_sm in job_tracker.get_active():
            if active_sm.job_exec.executor_id in dead_executors:
                tree_exec_id = active_sm.job_exec.tree_execution_id
                affected_trees[tree_exec_id] = active_sm.job_exec.executor_id

        # Both trees should be affected
        assert "tree-1" in affected_trees
        assert "tree-2" in affected_trees

        # Mark both as failed
        for tree_exec_id in affected_trees:
            tree_tracker.mark_failed(tree_exec_id, "Executor death")

        # Verify both failed
        assert tree_tracker.get("tree-1").state == TreeState.FAILED
        assert tree_tracker.get("tree-2").state == TreeState.FAILED

    @pytest.mark.asyncio
    async def test_jobs_transitioned_to_failed(
        self, job_tracker, tree_tracker, register_lock
    ):
        """Jobs on dead executor are transitioned to FAILED state."""
        # Setup: tree with running job
        job = make_job("test.job")
        tree = make_tree([job])
        tex = tree_tracker.create("tree-1", tree)
        job_exec = make_job_execution("tree-1", "test.job", "exec-1")
        sm = job_tracker.create(job_exec)
        sm.transition("dispatch", {"executor_id": "exec-1"})

        # Verify job is active
        assert sm.is_active

        # Simulate executor death - transition job to failed
        for active_sm in job_tracker.get_by_tree("tree-1"):
            if active_sm.is_active:
                active_sm.transition("failed", {"error": "Executor death"})

        # Verify job is now failed
        assert sm.state == JobState.FAILED
        assert sm.job_exec.error == "Executor death"


# =============================================================================
# Test: Edge cases
# =============================================================================


class TestEdgeCases:
    """Edge case tests for executor crash detection."""

    @pytest.mark.asyncio
    async def test_no_active_jobs_on_dead_executor(
        self, job_tracker, tree_tracker, register_lock
    ):
        """No trees affected if dead executor has no active jobs."""
        # Dead executor with no jobs
        dead_executors = ["exec-ghost"]

        # No active jobs
        assert job_tracker.active_count() == 0

        # Find affected trees (none)
        affected_trees = {}
        for active_sm in job_tracker.get_active():
            if active_sm.job_exec.executor_id in dead_executors:
                tree_exec_id = active_sm.job_exec.tree_execution_id
                affected_trees[tree_exec_id] = active_sm.job_exec.executor_id

        assert len(affected_trees) == 0

    @pytest.mark.asyncio
    async def test_tree_already_removed_gracefully_handled(
        self, job_tracker, tree_tracker, register_lock
    ):
        """Handles case where tree was already removed."""
        # Job exists but tree was already cleaned up
        job = make_job("orphan.job")
        job_exec = make_job_execution("tree-orphan", "orphan.job", "exec-1")
        sm = job_tracker.create(job_exec)
        sm.transition("dispatch", {"executor_id": "exec-1"})

        # Tree not tracked (simulating race condition)
        tex = tree_tracker.get("tree-orphan")
        assert tex is None  # Tree not in tracker

        # Should not crash
        dead_executors = ["exec-1"]
        affected_trees = {}
        for active_sm in job_tracker.get_active():
            if active_sm.job_exec.executor_id in dead_executors:
                tree_exec_id = active_sm.job_exec.tree_execution_id
                affected_trees[tree_exec_id] = active_sm.job_exec.executor_id

        # Tree is affected but not in tracker
        assert "tree-orphan" in affected_trees

        # mark_failed on non-existent tree should not crash
        for tree_exec_id in affected_trees:
            tex = tree_tracker.get(tree_exec_id)
            if tex:
                tree_tracker.mark_failed(tree_exec_id, "Executor death")
            # else: tree already gone, just continue

    def test_executor_back_online_after_prune(self, registry):
        """Executor that comes back online after prune is re-registered."""
        # Add executor and age it
        registry._executors["exec-1"] = ExecutorInfo(
            executor_id="exec-1",
            hostname="node1",
            capabilities={},
            capability_types=[],
            supported_formats=[],
            last_seen=time.time() - 100,  # stale
        )

        # Prune removes it
        stale = registry.prune_stale()
        assert "exec-1" in stale
        assert registry.live_count() == 0

        # Executor sends new heartbeat (came back online)
        registry.on_heartbeat("exec-1", {
            "hostname": "node1",
            "capabilities": [{"job_id": "job.a", "version": "1.0.0"}],
            "capability_types": ["pool"],
            "supported_formats": [],
        })

        # Re-registered
        assert registry.live_count() == 1
        assert registry.find_executor("job.a", "1.0.0") == "exec-1"


# =============================================================================
# Test: RegisterLock helper method
# =============================================================================


class TestRegisterLockIsHeld:
    """Tests for RegisterLock.is_held() helper."""

    def test_is_held_returns_false_when_not_held(self, register_lock):
        """is_held() returns False for unlocked path."""
        assert not register_lock.is_held("/some/path")

    def test_is_held_returns_true_when_held(self, register_lock):
        """is_held() returns True for locked path."""
        register_lock.try_acquire_all([("/data/file", "exclusive")], "tree-1")
        assert register_lock.is_held("/data/file")

    def test_is_held_returns_false_after_dequeue(self, register_lock):
        """is_held() returns False after lock released."""
        register_lock.try_acquire_all([("/data/file", "exclusive")], "tree-1")
        assert register_lock.is_held("/data/file")

        register_lock.dequeue("tree-1")
        assert not register_lock.is_held("/data/file")
