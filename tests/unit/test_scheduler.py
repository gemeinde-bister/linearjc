"""
Unit tests for multi-tree scheduler logic.

Tests the coordinator's ability to:
- Schedule multiple trees with non-conflicting outputs
- Block trees with conflicting outputs
- Enforce min_daily/max_daily schedule constraints
- Track chain execution locking
"""
import time
import pytest
from typing import List, Dict
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import Job, JobTree, JobSchedule, JobRun, DataRegistryEntry
from coordinator.tree_validation import TreeOutputValidator, OutputConflictError
from coordinator.job_tracker import JobTracker, JobExecution, JobState


def make_job(
    job_id: str,
    reads: List[str] = None,
    writes: List[str] = None,
    depends: List[str] = None,
    min_daily: int = 1,
    max_daily: int = 10,
    timeout: int = 30,
) -> Job:
    """Helper to create test Job objects."""
    return Job(
        id=job_id,
        version="1.0.0",
        reads=reads or [],
        writes=writes or [],
        depends=depends or [],
        schedule=JobSchedule(min_daily=min_daily, max_daily=max_daily),
        run=JobRun(user="test", timeout=timeout),
    )


def make_tree(
    job: Job,
    jobs: List[Job] = None,
    last_execution: float = None,
) -> JobTree:
    """Helper to create test JobTree objects."""
    return JobTree(
        root=job,
        jobs=jobs or [job],
        min_daily=job.schedule.min_daily,
        max_daily=job.schedule.max_daily,
        last_execution=last_execution,
    )


class TestOutputConflictDetection:
    """Tests for output conflict detection between trees."""

    def test_no_conflict_different_outputs(self):
        """Two trees with different outputs can both execute."""
        registry = {
            'output_a': DataRegistryEntry(type='fs', path='/data/output_a', kind='dir'),
            'output_b': DataRegistryEntry(type='fs', path='/data/output_b', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        tree_a = make_tree(make_job('job.a', writes=['output_a']))
        tree_b = make_tree(make_job('job.b', writes=['output_b']))

        # Both should validate successfully
        validator.validate_tree(tree_a)
        validator.register_tree(tree_a)

        validator.validate_tree(tree_b)
        validator.register_tree(tree_b)

        # Both are now active
        active = validator.get_active_outputs()
        assert len(active) == 2

    def test_conflict_same_output(self):
        """Two trees writing to same output - second is blocked."""
        registry = {
            'shared_output': DataRegistryEntry(type='fs', path='/data/shared', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        tree_a = make_tree(make_job('job.a', writes=['shared_output']))
        tree_b = make_tree(make_job('job.b', writes=['shared_output']))

        # First tree validates and registers
        validator.validate_tree(tree_a)
        validator.register_tree(tree_a)

        # Second tree should fail validation
        with pytest.raises(OutputConflictError) as exc_info:
            validator.validate_tree(tree_b)

        assert "job.a" in str(exc_info.value)
        assert "shared_output" in str(exc_info.value)

    def test_conflict_resolved_after_unregister(self):
        """After first tree completes, second can execute."""
        registry = {
            'shared_output': DataRegistryEntry(type='fs', path='/data/shared', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        tree_a = make_tree(make_job('job.a', writes=['shared_output']))
        tree_b = make_tree(make_job('job.b', writes=['shared_output']))

        # First tree executes
        validator.validate_tree(tree_a)
        validator.register_tree(tree_a)

        # Second blocked
        with pytest.raises(OutputConflictError):
            validator.validate_tree(tree_b)

        # First completes
        validator.unregister_tree(tree_a)

        # Now second can proceed
        validator.validate_tree(tree_b)
        validator.register_tree(tree_b)

        active = validator.get_active_outputs()
        assert '/data/shared' in active
        assert active['/data/shared'] == 'job.b'

    def test_minio_outputs_no_conflict(self):
        """MinIO outputs don't cause conflicts (isolated by object keys)."""
        registry = {
            'minio_output': DataRegistryEntry(
                type='minio',
                bucket='linearjc',
                prefix='outputs/shared',
            ),
        }
        validator = TreeOutputValidator(registry)

        tree_a = make_tree(make_job('job.a', writes=['minio_output']))
        tree_b = make_tree(make_job('job.b', writes=['minio_output']))

        # Both should validate - MinIO outputs are isolated by tree_execution_id
        validator.validate_tree(tree_a)
        validator.register_tree(tree_a)

        validator.validate_tree(tree_b)
        validator.register_tree(tree_b)

        # No filesystem outputs tracked
        assert len(validator.get_active_outputs()) == 0

    def test_mixed_outputs_partial_conflict(self):
        """Tree with both conflicting and non-conflicting outputs."""
        registry = {
            'shared': DataRegistryEntry(type='fs', path='/data/shared', kind='dir'),
            'unique_a': DataRegistryEntry(type='fs', path='/data/unique_a', kind='dir'),
            'unique_b': DataRegistryEntry(type='fs', path='/data/unique_b', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        tree_a = make_tree(make_job('job.a', writes=['shared', 'unique_a']))
        tree_b = make_tree(make_job('job.b', writes=['shared', 'unique_b']))

        validator.validate_tree(tree_a)
        validator.register_tree(tree_a)

        # Conflict on 'shared', even though 'unique_b' is free
        with pytest.raises(OutputConflictError) as exc_info:
            validator.validate_tree(tree_b)

        assert "shared" in str(exc_info.value)

    def test_reads_dont_conflict(self):
        """Multiple trees can read from same location."""
        registry = {
            'shared_input': DataRegistryEntry(type='fs', path='/data/input', kind='dir'),
            'output_a': DataRegistryEntry(type='fs', path='/data/output_a', kind='dir'),
            'output_b': DataRegistryEntry(type='fs', path='/data/output_b', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        # Both read from shared_input, write to different outputs
        tree_a = make_tree(make_job('job.a', reads=['shared_input'], writes=['output_a']))
        tree_b = make_tree(make_job('job.b', reads=['shared_input'], writes=['output_b']))

        validator.validate_tree(tree_a)
        validator.register_tree(tree_a)

        validator.validate_tree(tree_b)
        validator.register_tree(tree_b)

        # Both active, no conflict
        assert len(validator.get_active_outputs()) == 2


class TestStaticConfigValidation:
    """Tests for static configuration validation at load time."""

    def test_static_validation_detects_config_conflict(self):
        """Static validation catches configuration-level conflicts."""
        registry = {
            'shared': DataRegistryEntry(type='fs', path='/data/shared', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        trees = [
            make_tree(make_job('job.a', writes=['shared'])),
            make_tree(make_job('job.b', writes=['shared'])),
        ]

        with pytest.raises(OutputConflictError) as exc_info:
            validator.validate_tree_configurations(trees)

        assert "CONFIGURATION ERROR" in str(exc_info.value)
        assert "job.a" in str(exc_info.value)
        assert "job.b" in str(exc_info.value)

    def test_static_validation_passes_non_conflicting(self):
        """Static validation passes for non-conflicting configs."""
        registry = {
            'output_a': DataRegistryEntry(type='fs', path='/data/a', kind='dir'),
            'output_b': DataRegistryEntry(type='fs', path='/data/b', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        trees = [
            make_tree(make_job('job.a', writes=['output_a'])),
            make_tree(make_job('job.b', writes=['output_b'])),
        ]

        # Should not raise
        validator.validate_tree_configurations(trees)


class TestScheduleEnforcement:
    """Tests for min_daily/max_daily schedule constraints."""

    def test_first_execution_always_runs(self):
        """Tree with no previous execution should run immediately."""
        job = make_job('job.a', min_daily=2, max_daily=4)
        tree = make_tree(job, last_execution=None)

        assert tree.last_execution is None
        # First execution should be allowed

    def test_max_daily_prevents_over_execution(self):
        """Tree at max_daily should not run again within window."""
        now = time.time()
        job = make_job('job.a', min_daily=1, max_daily=4)
        tree = make_tree(job)

        # Record 4 executions in last 24h (at max)
        for i in range(4):
            tree.record_execution(now - (i * 3600))  # Every hour

        count = tree.get_executions_last_24h(now)
        assert count == 4

        # At max_daily, is_within_bounds returns True but shouldn't schedule more
        # The scheduler checks: count < max_daily before executing
        assert count >= tree.max_daily

    def test_min_daily_forces_execution(self):
        """Tree below min_daily should be forced to run."""
        now = time.time()
        job = make_job('job.a', min_daily=2, max_daily=4)
        tree = make_tree(job, last_execution=now - (24 * 3600))  # 24h ago

        # Only 1 execution in window (the one from 24h ago is now outside)
        tree.record_execution(now - (20 * 3600))  # 20h ago

        count = tree.get_executions_last_24h(now)
        assert count == 1
        assert count < tree.min_daily  # Below minimum

    def test_ideal_interval_calculation(self):
        """Verify ideal interval calculation for max_daily."""
        job = make_job('job.a', min_daily=1, max_daily=4)
        tree = make_tree(job)

        # max_daily=4 means run every 6 hours (24/4)
        ideal_interval = (24 * 3600) / tree.max_daily
        assert ideal_interval == 6 * 3600  # 6 hours in seconds

    def test_min_interval_calculation(self):
        """Verify minimum interval calculation for min_daily."""
        job = make_job('job.a', min_daily=2, max_daily=4)
        tree = make_tree(job)

        # min_daily=2 means MUST run every 12 hours (24/2)
        min_interval = (24 * 3600) / tree.min_daily
        assert min_interval == 12 * 3600  # 12 hours in seconds

    def test_execution_history_pruning(self):
        """Old executions are pruned from history."""
        now = time.time()
        job = make_job('job.a', min_daily=1, max_daily=10)
        tree = make_tree(job)

        # Add old execution (26h ago - outside 25h window)
        tree.record_execution(now - (26 * 3600))

        # Add recent execution
        tree.record_execution(now - (1 * 3600))

        # Only recent one should count
        count = tree.get_executions_last_24h(now)
        assert count == 1

    def test_is_within_bounds_true(self):
        """is_within_bounds returns True when within min/max."""
        now = time.time()
        job = make_job('job.a', min_daily=2, max_daily=4)
        tree = make_tree(job)

        # Record 3 executions (between 2 and 4)
        for i in range(3):
            tree.record_execution(now - (i * 3600))

        assert tree.is_within_bounds(now) is True

    def test_is_within_bounds_below_min(self):
        """is_within_bounds returns False when below min_daily."""
        now = time.time()
        job = make_job('job.a', min_daily=2, max_daily=4)
        tree = make_tree(job)

        # Record only 1 execution (below min_daily=2)
        tree.record_execution(now - (3600))

        assert tree.is_within_bounds(now) is False

    def test_is_within_bounds_above_max(self):
        """is_within_bounds returns False when above max_daily."""
        now = time.time()
        job = make_job('job.a', min_daily=2, max_daily=4)
        tree = make_tree(job)

        # Record 5 executions (above max_daily=4)
        for i in range(5):
            tree.record_execution(now - (i * 3600))

        assert tree.is_within_bounds(now) is False


class TestChainTreeLocking:
    """Tests for chain execution locking behavior."""

    def test_chain_tree_registers_root_outputs(self):
        """Chain tree registers root job's outputs (tree-level outputs)."""
        registry = {
            'intermediate': DataRegistryEntry(type='fs', path='/data/intermediate', kind='dir'),
            'final': DataRegistryEntry(type='fs', path='/data/final', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        # Chain: step1 -> step2
        step1 = make_job('chain.step1', writes=['intermediate'])
        step2 = make_job('chain.step2', reads=['intermediate'], writes=['final'], depends=['chain.step1'])

        # Tree outputs are root job's outputs (step1.writes)
        tree = make_tree(step1, jobs=[step1, step2])

        validator.validate_tree(tree)
        validator.register_tree(tree)

        # Only root's outputs are locked
        active = validator.get_active_outputs()
        assert '/data/intermediate' in active
        assert '/data/final' not in active  # step2's output not locked at tree level

    def test_chain_blocks_conflicting_tree(self):
        """Running chain blocks other trees with conflicting outputs."""
        registry = {
            'shared': DataRegistryEntry(type='fs', path='/data/shared', kind='dir'),
            'final': DataRegistryEntry(type='fs', path='/data/final', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        # Chain writes to 'shared'
        chain_step1 = make_job('chain.step1', writes=['shared'])
        chain_step2 = make_job('chain.step2', reads=['shared'], writes=['final'], depends=['chain.step1'])
        chain_tree = make_tree(chain_step1, jobs=[chain_step1, chain_step2])

        # Single job also wants 'shared'
        single_job = make_job('single.job', writes=['shared'])
        single_tree = make_tree(single_job)

        # Chain starts first
        validator.validate_tree(chain_tree)
        validator.register_tree(chain_tree)

        # Single job blocked
        with pytest.raises(OutputConflictError):
            validator.validate_tree(single_tree)

    def test_chain_unregister_frees_outputs(self):
        """When chain completes, outputs are freed."""
        registry = {
            'output': DataRegistryEntry(type='fs', path='/data/output', kind='dir'),
        }
        validator = TreeOutputValidator(registry)

        step1 = make_job('chain.step1', writes=['output'])
        step2 = make_job('chain.step2', reads=['output'], depends=['chain.step1'])
        chain_tree = make_tree(step1, jobs=[step1, step2])

        # Chain runs
        validator.validate_tree(chain_tree)
        validator.register_tree(chain_tree)
        assert len(validator.get_active_outputs()) == 1

        # Chain completes (both jobs done)
        validator.unregister_tree(chain_tree)
        assert len(validator.get_active_outputs()) == 0


class TestJobTrackerConcurrency:
    """Tests for job tracker concurrent execution tracking."""

    def test_multiple_trees_tracked_separately(self):
        """Job tracker tracks executions from different trees."""
        tracker = JobTracker()

        # Tree A execution
        exec_a = JobExecution(
            job_execution_id="job.a-20251203-120000-aaaa1111",
            tree_execution_id="job.a-20251203-120000-aaaa1111",
            job_id="job.a",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1000.0,
            job_index=0,
        )
        tracker.add(exec_a)

        # Tree B execution (concurrent)
        exec_b = JobExecution(
            job_execution_id="job.b-20251203-120000-bbbb2222",
            tree_execution_id="job.b-20251203-120000-bbbb2222",
            job_id="job.b",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="exec-2",
            assigned_at=1001.0,
            timeout_at=1031.0,
            last_progress=1001.0,
            job_index=0,
        )
        tracker.add(exec_b)

        # Both tracked
        assert tracker.get_job("job.a-20251203-120000-aaaa1111") is not None
        assert tracker.get_job("job.b-20251203-120000-bbbb2222") is not None

    def test_chain_jobs_same_tree_execution_id(self):
        """All jobs in a chain share the same tree_execution_id."""
        tracker = JobTracker()
        tree_exec_id = "chain.step1-20251203-120000-cccc3333"

        # Step 1
        exec_1 = JobExecution(
            job_execution_id="chain.step1-20251203-120000-cccc3333",
            tree_execution_id=tree_exec_id,
            job_id="chain.step1",
            job_version="1.0.0",
            state=JobState.COMPLETED,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1010.0,
            job_index=0,
        )
        tracker.add(exec_1)

        # Step 2 (same tree_execution_id)
        exec_2 = JobExecution(
            job_execution_id="chain.step2-20251203-120000-cccc3333",
            tree_execution_id=tree_exec_id,
            job_id="chain.step2",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="exec-1",
            assigned_at=1011.0,
            timeout_at=1041.0,
            last_progress=1015.0,
            job_index=1,
        )
        tracker.add(exec_2)

        # Find by tree_execution_id returns both
        chain_jobs = tracker.find_by_tree_execution_id(tree_exec_id)
        assert len(chain_jobs) == 2
        assert chain_jobs[0].job_id == "chain.step1"
        assert chain_jobs[1].job_id == "chain.step2"

    def test_active_job_count(self):
        """Count only non-terminal jobs as active."""
        tracker = JobTracker()

        # Active job
        exec_running = JobExecution(
            job_execution_id="running-001",
            tree_execution_id="tree-001",
            job_id="job.running",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1000.0,
        )
        tracker.add(exec_running)

        # Completed job (terminal)
        exec_done = JobExecution(
            job_execution_id="done-002",
            tree_execution_id="tree-002",
            job_id="job.done",
            job_version="1.0.0",
            state=JobState.COMPLETED,
            executor_id="exec-1",
            assigned_at=900.0,
            timeout_at=930.0,
            last_progress=910.0,
        )
        tracker.add(exec_done)

        # Failed job (terminal)
        exec_failed = JobExecution(
            job_execution_id="failed-003",
            tree_execution_id="tree-003",
            job_id="job.failed",
            job_version="1.0.0",
            state=JobState.FAILED,
            executor_id="exec-1",
            assigned_at=800.0,
            timeout_at=830.0,
            last_progress=810.0,
            error_message="Test error",
        )
        tracker.add(exec_failed)

        # Only running job is "active"
        active_jobs = [
            j for j in tracker.get_all_jobs().values()
            if j.state not in (JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT)
        ]
        assert len(active_jobs) == 1
        assert active_jobs[0].job_id == "job.running"
