"""
Tree builder - construct dependency graphs and detect linear job trees.
"""
import logging
from typing import List, Dict, Set
from collections import defaultdict

from coordinator.models import Job, JobTree

logger = logging.getLogger(__name__)


class TreeBuilderError(Exception):
    """Error during tree building."""
    pass


def build_dependency_graph(jobs: List[Job]) -> Dict[str, Set[str]]:
    """
    Build a dependency graph from jobs.

    Args:
        jobs: List of Job objects

    Returns:
        Dictionary mapping job_id -> set of dependent job IDs
        (i.e., jobs that depend on this job)
    """
    graph = defaultdict(set)

    for job in jobs:
        # Ensure job is in graph even if it has no dependents
        if job.id not in graph:
            graph[job.id] = set()

        # Add edges from dependencies to this job
        for dep_id in job.depends_on:
            graph[dep_id].add(job.id)

    return dict(graph)


def detect_cycles(jobs: List[Job]) -> List[List[str]]:
    """
    Detect circular dependencies in job graph.

    Args:
        jobs: List of Job objects

    Returns:
        List of cycles (each cycle is a list of job IDs)
    """
    job_map = {job.id: job for job in jobs}
    visited = set()
    rec_stack = set()
    cycles = []

    def visit(job_id: str, path: List[str]):
        if job_id in rec_stack:
            # Found a cycle
            cycle_start = path.index(job_id)
            cycles.append(path[cycle_start:] + [job_id])
            return

        if job_id in visited:
            return

        visited.add(job_id)
        rec_stack.add(job_id)

        if job_id in job_map:
            for dep_id in job_map[job_id].depends_on:
                visit(dep_id, path + [job_id])

        rec_stack.remove(job_id)

    for job in jobs:
        if job.id not in visited:
            visit(job.id, [])

    return cycles


def build_trees(jobs: List[Job]) -> List[JobTree]:
    """
    Build linear job trees from jobs.

    Algorithm:
    1. Check for cycles
    2. Find root jobs (no dependencies)
    3. For each root, build a linear tree by following dependencies
    4. Validate that each tree is truly linear (no branching)
    5. Calculate tree schedule (most restrictive)

    Args:
        jobs: List of Job objects

    Returns:
        List of JobTree objects

    Raises:
        TreeBuilderError: If cycles exist or trees are non-linear
    """
    # Check for cycles
    cycles = detect_cycles(jobs)
    if cycles:
        cycle_strs = [" → ".join(cycle) for cycle in cycles]
        raise TreeBuilderError(f"Circular dependencies detected: {', '.join(cycle_strs)}")

    # Build dependency graph
    graph = build_dependency_graph(jobs)
    job_map = {job.id: job for job in jobs}

    # Find root jobs (no dependencies)
    roots = [job for job in jobs if not job.depends_on]

    if not roots:
        raise TreeBuilderError("No root jobs found (all jobs have dependencies)")

    logger.info(f"Found {len(roots)} root jobs: {[r.id for r in roots]}")

    # Build tree for each root
    trees = []
    for root in roots:
        try:
            tree = build_linear_tree(root, graph, job_map)
            trees.append(tree)
            logger.info(f"Built tree: {tree}")
        except TreeBuilderError as e:
            logger.error(f"Failed to build tree from root {root.id}: {e}")
            raise

    # Validate that all jobs are in exactly one tree
    all_tree_jobs = set()
    for tree in trees:
        for job in tree.jobs:
            if job.id in all_tree_jobs:
                raise TreeBuilderError(f"Job {job.id} appears in multiple trees")
            all_tree_jobs.add(job.id)

    missing_jobs = set(job_map.keys()) - all_tree_jobs
    if missing_jobs:
        raise TreeBuilderError(f"Jobs not in any tree: {missing_jobs}")

    return trees


def build_linear_tree(root: Job, graph: Dict[str, Set[str]], job_map: Dict[str, Job]) -> JobTree:
    """
    Build a linear tree starting from a root job.

    Args:
        root: Root job
        graph: Dependency graph (job_id -> dependents)
        job_map: Mapping of job_id -> Job

    Returns:
        JobTree object

    Raises:
        TreeBuilderError: If tree has branching
    """
    jobs = []
    current = root

    while current:
        jobs.append(current)

        # Get dependents (jobs that depend on current)
        dependents = graph.get(current.id, set())

        if len(dependents) > 1:
            raise TreeBuilderError(
                f"Job {current.id} has multiple dependents: {dependents}. "
                f"Linear trees cannot have branching."
            )

        if len(dependents) == 0:
            # Reached leaf
            break

        # Move to next job
        next_id = next(iter(dependents))
        current = job_map[next_id]

    # Calculate tree schedule (most restrictive)
    min_daily = max(job.schedule.min_daily for job in jobs)
    max_daily = min(job.schedule.max_daily for job in jobs)

    if min_daily > max_daily:
        job_schedules = ", ".join([
            f"{job.id}(min={job.schedule.min_daily},max={job.schedule.max_daily})"
            for job in jobs
        ])
        raise TreeBuilderError(
            f"Tree has incompatible schedules: {job_schedules}. "
            f"Calculated tree_min={min_daily}, tree_max={max_daily} (impossible)."
        )

    return JobTree(
        root=root,
        jobs=jobs,
        min_daily=min_daily,
        max_daily=max_daily
    )


def validate_tree_linearity(jobs: List[Job], graph: Dict[str, Set[str]]) -> bool:
    """
    Validate that jobs form a linear tree (no branching).

    Args:
        jobs: List of jobs in tree
        graph: Dependency graph

    Returns:
        True if linear, False otherwise
    """
    for job in jobs[:-1]:  # All except last
        dependents = graph.get(job.id, set())
        if len(dependents) != 1:
            return False
    return True
