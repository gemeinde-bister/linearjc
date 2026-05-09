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
        for dep_id in job.depends:
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
            for dep_id in job_map[job_id].depends:
                visit(dep_id, path + [job_id])

        rec_stack.remove(job_id)

    for job in jobs:
        if job.id not in visited:
            visit(job.id, [])

    return cycles


def build_trees(jobs: List[Job]) -> List[JobTree]:
    """
    Build linear job trees from jobs.

    Supports fan-out: if job A has multiple dependents (B and C), two separate
    trees are created, each containing A. A executes once per tree.

    Algorithm:
    1. Check for cycles
    2. Find leaf jobs (no dependents)
    3. For each leaf, trace back to root to build a linear path
    4. Each unique path becomes a tree (fan-out creates multiple trees)
    5. Calculate tree schedule (most restrictive)

    Args:
        jobs: List of Job objects

    Returns:
        List of JobTree objects

    Raises:
        TreeBuilderError: If cycles exist or jobs have multiple dependencies
    """
    # Check for cycles
    cycles = detect_cycles(jobs)
    if cycles:
        cycle_strs = [" → ".join(cycle) for cycle in cycles]
        raise TreeBuilderError(f"Circular dependencies detected: {', '.join(cycle_strs)}")

    # Validate single dependency constraint (no merge points)
    for job in jobs:
        if len(job.depends) > 1:
            raise TreeBuilderError(
                f"Job {job.id} has multiple dependencies: {job.depends}. "
                f"Merge points are not supported (each job can depend on at most one other job)."
            )

    # Build dependency graph (job_id -> set of dependents)
    graph = build_dependency_graph(jobs)
    job_map = {job.id: job for job in jobs}

    # Find leaf jobs (no dependents in the graph)
    leaves = [job for job in jobs if not graph.get(job.id)]

    if not leaves:
        raise TreeBuilderError("No leaf jobs found (circular or incomplete graph)")

    logger.info(f"Found {len(leaves)} leaf jobs: {[l.id for l in leaves]}")

    # Build tree for each leaf by tracing back to root
    trees = []
    for leaf in leaves:
        try:
            tree = build_tree_from_leaf(leaf, job_map)
            trees.append(tree)
            logger.info(f"Built tree: {tree}")
        except TreeBuilderError as e:
            logger.error(f"Failed to build tree from leaf {leaf.id}: {e}")
            raise

    # Note: With fan-out, jobs CAN appear in multiple trees (that's the feature!)
    # But every job must appear in at least one tree
    all_tree_jobs = set()
    for tree in trees:
        for job in tree.jobs:
            all_tree_jobs.add(job.id)

    missing_jobs = set(job_map.keys()) - all_tree_jobs
    if missing_jobs:
        raise TreeBuilderError(f"Jobs not in any tree: {missing_jobs}")

    # Log fan-out info
    job_tree_count = {}
    for tree in trees:
        for job in tree.jobs:
            job_tree_count[job.id] = job_tree_count.get(job.id, 0) + 1

    duplicated = {jid: count for jid, count in job_tree_count.items() if count > 1}
    if duplicated:
        logger.info(f"Fan-out detected - jobs in multiple trees: {duplicated}")

    return trees


def build_tree_from_leaf(leaf: Job, job_map: Dict[str, Job]) -> JobTree:
    """
    Build a linear tree by tracing from leaf back to root.

    Args:
        leaf: Leaf job (no dependents)
        job_map: Mapping of job_id -> Job

    Returns:
        JobTree with jobs ordered from root to leaf

    Raises:
        TreeBuilderError: If dependency chain is broken
    """
    # Trace back from leaf to root
    path = []
    current = leaf

    while current:
        path.append(current)

        if not current.depends:
            # Reached root
            break

        # Move to dependency (single dependency guaranteed by validation)
        dep_id = current.depends[0]
        if dep_id not in job_map:
            raise TreeBuilderError(
                f"Job {current.id} depends on {dep_id} which does not exist"
            )
        current = job_map[dep_id]

    # Reverse to get root-to-leaf order
    jobs = list(reversed(path))
    root = jobs[0]

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
