"""
Unit tests for multi-job chain execution (Phase 12).

Tests the coordinator's chain continuation logic without requiring
full E2E infrastructure.
"""
import pytest
from dataclasses import dataclass
from typing import List, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.job_tracker import JobTracker, JobExecution, JobState
from coordinator.models import Job, JobTree, JobSchedule, JobRun


def make_job(
    job_id: str,
    reads: List[str] = None,
    writes: List[str] = None,
    depends: List[str] = None
) -> Job:
    """Helper to create test Job objects."""
    return Job(
        id=job_id,
        version="1.0.0",
        reads=reads or [],
        writes=writes or [],
        depends=depends or [],
        schedule=JobSchedule(min_daily=1, max_daily=10),
        run=JobRun(user="test", timeout=30),
    )


class TestJobExecutionIndex:
    """Tests for job_index tracking in JobExecution."""

    def test_job_index_default_zero(self):
        """Root job should default to index 0."""
        exec = JobExecution(
            job_execution_id="test-001",
            tree_execution_id="tree-001",
            job_id="root.job",
            job_version="1.0.0",
            state=JobState.QUEUED,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1000.0,
        )
        assert exec.job_index == 0

    def test_job_index_explicit(self):
        """Chain job should have explicit index."""
        exec = JobExecution(
            job_execution_id="test-002",
            tree_execution_id="tree-001",
            job_id="chain.job",
            job_version="1.0.0",
            state=JobState.QUEUED,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1000.0,
            job_index=2,
        )
        assert exec.job_index == 2


class TestJobTrackerChainSupport:
    """Tests for JobTracker chain-related methods."""

    def test_find_by_tree_execution_id_empty(self):
        """Empty tracker returns empty list."""
        tracker = JobTracker()
        result = tracker.find_by_tree_execution_id("nonexistent")
        assert result == []

    def test_find_by_tree_execution_id_single(self):
        """Find single job in tree."""
        tracker = JobTracker()
        tree_id = "root-20251203-120000-abcd1234"

        exec = JobExecution(
            job_execution_id="root-20251203-120000-abcd1234",
            tree_execution_id=tree_id,
            job_id="root",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1000.0,
            job_index=0,
        )
        tracker.add(exec)

        result = tracker.find_by_tree_execution_id(tree_id)
        assert len(result) == 1
        assert result[0].job_id == "root"

    def test_find_by_tree_execution_id_chain_ordered(self):
        """Find chain jobs in correct order by job_index."""
        tracker = JobTracker()
        tree_id = "chain-20251203-120000-abcd1234"

        # Add in reverse order to verify sorting
        exec2 = JobExecution(
            job_execution_id="step2-20251203-120000-abcd1234",
            tree_execution_id=tree_id,
            job_id="step2",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="exec-1",
            assigned_at=1010.0,
            timeout_at=1040.0,
            last_progress=1015.0,
            job_index=1,
        )
        tracker.add(exec2)

        exec1 = JobExecution(
            job_execution_id="step1-20251203-120000-abcd1234",
            tree_execution_id=tree_id,
            job_id="step1",
            job_version="1.0.0",
            state=JobState.COMPLETED,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1005.0,
            job_index=0,
        )
        tracker.add(exec1)

        result = tracker.find_by_tree_execution_id(tree_id)
        assert len(result) == 2
        assert result[0].job_index == 0
        assert result[0].job_id == "step1"
        assert result[1].job_index == 1
        assert result[1].job_id == "step2"

    def test_find_by_tree_execution_id_multiple_trees(self):
        """Only return jobs from specified tree."""
        tracker = JobTracker()

        tree1_id = "tree1-20251203-120000-aaaa1111"
        tree2_id = "tree2-20251203-120000-bbbb2222"

        exec1 = JobExecution(
            job_execution_id="job1-exec",
            tree_execution_id=tree1_id,
            job_id="job1",
            job_version="1.0.0",
            state=JobState.COMPLETED,
            executor_id="exec-1",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1005.0,
            job_index=0,
        )
        tracker.add(exec1)

        exec2 = JobExecution(
            job_execution_id="job2-exec",
            tree_execution_id=tree2_id,
            job_id="job2",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="exec-1",
            assigned_at=1010.0,
            timeout_at=1040.0,
            last_progress=1015.0,
            job_index=0,
        )
        tracker.add(exec2)

        result1 = tracker.find_by_tree_execution_id(tree1_id)
        assert len(result1) == 1
        assert result1[0].job_id == "job1"

        result2 = tracker.find_by_tree_execution_id(tree2_id)
        assert len(result2) == 1
        assert result2[0].job_id == "job2"


class TestChainLogic:
    """Tests for chain continuation logic."""

    def test_is_last_job_single_job_tree(self):
        """Single job tree: job 0 is last."""
        job = make_job("single.job")
        tree = JobTree(
            root=job,
            jobs=[job],
            min_daily=1,
            max_daily=10,
        )

        current_index = 0
        is_last = (current_index >= len(tree.jobs) - 1)
        assert is_last is True

    def test_is_last_job_two_job_chain_root(self):
        """Two job chain: job 0 is not last."""
        job1 = make_job("chain.step1", writes=["intermediate"])
        job2 = make_job("chain.step2", reads=["intermediate"], depends=["chain.step1"])
        tree = JobTree(
            root=job1,
            jobs=[job1, job2],
            min_daily=1,
            max_daily=10,
        )

        current_index = 0
        is_last = (current_index >= len(tree.jobs) - 1)
        assert is_last is False

    def test_is_last_job_two_job_chain_leaf(self):
        """Two job chain: job 1 is last."""
        job1 = make_job("chain.step1", writes=["intermediate"])
        job2 = make_job("chain.step2", reads=["intermediate"], depends=["chain.step1"])
        tree = JobTree(
            root=job1,
            jobs=[job1, job2],
            min_daily=1,
            max_daily=10,
        )

        current_index = 1
        is_last = (current_index >= len(tree.jobs) - 1)
        assert is_last is True

    def test_is_last_job_three_job_chain(self):
        """Three job chain: only job 2 is last."""
        job1 = make_job("chain.a", writes=["data_a"])
        job2 = make_job("chain.b", reads=["data_a"], writes=["data_b"], depends=["chain.a"])
        job3 = make_job("chain.c", reads=["data_b"], depends=["chain.b"])
        tree = JobTree(
            root=job1,
            jobs=[job1, job2, job3],
            min_daily=1,
            max_daily=10,
        )

        assert (0 >= len(tree.jobs) - 1) is False  # job 0 not last
        assert (1 >= len(tree.jobs) - 1) is False  # job 1 not last
        assert (2 >= len(tree.jobs) - 1) is True   # job 2 is last

    def test_next_job_index(self):
        """Get next job index in chain."""
        job1 = make_job("chain.step1", writes=["intermediate"])
        job2 = make_job("chain.step2", reads=["intermediate"], depends=["chain.step1"])
        tree = JobTree(
            root=job1,
            jobs=[job1, job2],
            min_daily=1,
            max_daily=10,
        )

        current_index = 0
        next_index = current_index + 1

        assert next_index == 1
        assert tree.jobs[next_index].id == "chain.step2"

    def test_prev_job_for_intermediate_input(self):
        """Get previous job to check for intermediate outputs."""
        job1 = make_job("chain.step1", writes=["intermediate"])
        job2 = make_job("chain.step2", reads=["intermediate"], depends=["chain.step1"])
        tree = JobTree(
            root=job1,
            jobs=[job1, job2],
            min_daily=1,
            max_daily=10,
        )

        current_index = 1
        prev_job = tree.jobs[current_index - 1]

        assert prev_job.id == "chain.step1"
        assert "intermediate" in prev_job.writes

        # Check if current job's reads come from previous job's writes
        current_job = tree.jobs[current_index]
        for read_key in current_job.reads:
            if read_key in prev_job.writes:
                # This is intermediate data
                assert read_key == "intermediate"


class TestChainIntermediateData:
    """Tests for intermediate data detection between chain jobs."""

    def test_detect_intermediate_from_prev_writes(self):
        """Input is intermediate if it's in previous job's writes."""
        job1 = make_job("step1", writes=["data_a", "data_b"])
        job2 = make_job("step2", reads=["data_a", "external"], depends=["step1"])

        # data_a should be detected as intermediate
        # external should NOT be detected as intermediate
        for input_key in job2.reads:
            is_intermediate = input_key in job1.writes
            if input_key == "data_a":
                assert is_intermediate is True
            elif input_key == "external":
                assert is_intermediate is False

    def test_all_inputs_external_for_root_job(self):
        """Root job (no previous) has all external inputs."""
        job1 = make_job("root", reads=["input1", "input2"])

        # For root job, there's no previous job
        prev_job = None

        for input_key in job1.reads:
            if prev_job:
                is_intermediate = input_key in prev_job.writes
            else:
                is_intermediate = False

            assert is_intermediate is False
