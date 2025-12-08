"""Unit tests for tree_builder module."""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from coordinator.models import Job, JobSchedule, JobRun, JobTree
from coordinator.tree_builder import (
    build_dependency_graph,
    detect_cycles,
    build_trees,
    build_linear_tree,
    TreeBuilderError
)


def make_job(id: str, depends: list = None, min_daily: int = 1, max_daily: int = 10) -> Job:
    """Helper to create a Job with minimal config."""
    return Job(
        id=id,
        version="1.0.0",
        reads=[],
        writes=[],
        depends=depends or [],
        schedule=JobSchedule(min_daily=min_daily, max_daily=max_daily),
        run=JobRun(user="nobody")
    )


class TestBuildDependencyGraph:
    """Tests for build_dependency_graph function."""

    def test_single_job(self):
        """Single job with no dependencies."""
        jobs = [make_job("a")]
        graph = build_dependency_graph(jobs)
        assert graph == {"a": set()}

    def test_linear_chain(self):
        """Linear chain: a -> b -> c."""
        jobs = [
            make_job("a"),
            make_job("b", depends=["a"]),
            make_job("c", depends=["b"]),
        ]
        graph = build_dependency_graph(jobs)
        assert graph["a"] == {"b"}  # b depends on a
        assert graph["b"] == {"c"}  # c depends on b
        assert graph["c"] == set()  # nothing depends on c

    def test_multiple_roots(self):
        """Two separate chains."""
        jobs = [
            make_job("a"),
            make_job("b"),
        ]
        graph = build_dependency_graph(jobs)
        assert graph["a"] == set()
        assert graph["b"] == set()


class TestDetectCycles:
    """Tests for detect_cycles function."""

    def test_no_cycles(self):
        """Linear chain has no cycles."""
        jobs = [
            make_job("a"),
            make_job("b", depends=["a"]),
            make_job("c", depends=["b"]),
        ]
        cycles = detect_cycles(jobs)
        assert cycles == []

    def test_self_cycle(self):
        """Job depends on itself."""
        jobs = [make_job("a", depends=["a"])]
        cycles = detect_cycles(jobs)
        assert len(cycles) == 1
        assert "a" in cycles[0]

    def test_two_job_cycle(self):
        """a -> b -> a cycle."""
        jobs = [
            make_job("a", depends=["b"]),
            make_job("b", depends=["a"]),
        ]
        cycles = detect_cycles(jobs)
        assert len(cycles) >= 1


class TestBuildTrees:
    """Tests for build_trees function."""

    def test_single_job(self):
        """Single root job becomes a tree."""
        jobs = [make_job("a")]
        trees = build_trees(jobs)
        assert len(trees) == 1
        assert trees[0].root.id == "a"
        assert len(trees[0].jobs) == 1

    def test_linear_chain(self):
        """Linear chain becomes one tree."""
        jobs = [
            make_job("a"),
            make_job("b", depends=["a"]),
            make_job("c", depends=["b"]),
        ]
        trees = build_trees(jobs)
        assert len(trees) == 1
        assert trees[0].root.id == "a"
        assert len(trees[0].jobs) == 3
        assert [j.id for j in trees[0].jobs] == ["a", "b", "c"]

    def test_two_separate_chains(self):
        """Two root jobs become two trees."""
        jobs = [
            make_job("a"),
            make_job("b"),
        ]
        trees = build_trees(jobs)
        assert len(trees) == 2

    def test_cycle_raises(self):
        """Circular dependency raises error."""
        jobs = [
            make_job("a", depends=["b"]),
            make_job("b", depends=["a"]),
        ]
        with pytest.raises(TreeBuilderError, match="Circular"):
            build_trees(jobs)

    def test_no_roots_raises(self):
        """All jobs have dependencies raises error."""
        jobs = [
            make_job("a", depends=["b"]),
        ]
        with pytest.raises(TreeBuilderError, match="No root"):
            build_trees(jobs)

    def test_branching_raises(self):
        """Branching (one job with multiple dependents) raises error."""
        jobs = [
            make_job("a"),
            make_job("b", depends=["a"]),
            make_job("c", depends=["a"]),  # Both b and c depend on a
        ]
        with pytest.raises(TreeBuilderError, match="multiple dependents"):
            build_trees(jobs)

    def test_schedule_calculation(self):
        """Tree schedule is most restrictive."""
        jobs = [
            make_job("a", min_daily=1, max_daily=10),
            make_job("b", depends=["a"], min_daily=2, max_daily=8),
            make_job("c", depends=["b"], min_daily=3, max_daily=6),
        ]
        trees = build_trees(jobs)
        # min = max(1,2,3) = 3, max = min(10,8,6) = 6
        assert trees[0].min_daily == 3
        assert trees[0].max_daily == 6

    def test_incompatible_schedules_raises(self):
        """Incompatible schedules raise error."""
        jobs = [
            make_job("a", min_daily=5, max_daily=10),
            make_job("b", depends=["a"], min_daily=1, max_daily=3),
        ]
        # min=5, max=3 is impossible
        with pytest.raises(TreeBuilderError, match="incompatible schedules"):
            build_trees(jobs)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
