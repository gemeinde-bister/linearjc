"""
JobTracker - Job execution registry with state machine integration.

Tracks all job executions and provides lookup by various criteria.
Single-threaded async design - no locks needed.
"""

import logging
import time

from coordinator_v2.job_state_machine import JobExecution, JobState, JobStateMachine

logger = logging.getLogger(__name__)


class JobTracker:
    """
    Tracks all job executions.

    Provides O(1) lookup by job_execution_id and O(n) lookup by tree_execution_id.
    Single-threaded, no locks needed.
    """

    def __init__(self) -> None:
        """Initialize empty job tracker."""
        self._jobs: dict[str, JobStateMachine] = {}
        self._by_tree: dict[str, list[str]] = {}  # tree_exec_id -> [job_exec_ids]

    def create(self, job_exec: JobExecution) -> JobStateMachine:
        """
        Create new job execution with state machine.

        Args:
            job_exec: JobExecution to track

        Returns:
            JobStateMachine wrapping the execution
        """
        sm = JobStateMachine(job_exec)
        self._jobs[job_exec.job_execution_id] = sm

        # Index by tree
        tree_id = job_exec.tree_execution_id
        if tree_id not in self._by_tree:
            self._by_tree[tree_id] = []
        self._by_tree[tree_id].append(job_exec.job_execution_id)

        logger.debug(
            f"Tracking job {job_exec.job_execution_id} "
            f"(tree: {tree_id}, index: {job_exec.chain_index})"
        )
        return sm

    def get(self, job_execution_id: str) -> JobStateMachine | None:
        """Get state machine by job_execution_id."""
        return self._jobs.get(job_execution_id)

    def get_by_tree(self, tree_execution_id: str) -> list[JobStateMachine]:
        """
        Get all jobs for a tree execution.

        Args:
            tree_execution_id: Tree execution ID

        Returns:
            List of JobStateMachines in chain order
        """
        job_ids = self._by_tree.get(tree_execution_id, [])
        result = [self._jobs[jid] for jid in job_ids if jid in self._jobs]
        # Sort by chain_index for predictable order
        result.sort(key=lambda sm: sm.job_exec.chain_index)
        return result

    def get_active(self) -> list[JobStateMachine]:
        """Get all active (non-terminal, non-queued) jobs."""
        return [sm for sm in self._jobs.values() if sm.is_active]

    def get_all(self) -> dict[str, JobStateMachine]:
        """Get all tracked jobs (returns dict reference)."""
        return self._jobs

    def get_by_state(self, state: JobState) -> list[JobStateMachine]:
        """Get all jobs in a specific state."""
        return [sm for sm in self._jobs.values() if sm.state == state]

    def check_timeouts(self) -> list[JobStateMachine]:
        """
        Check for timed out jobs.

        Returns:
            List of jobs that were transitioned to TIMEOUT state
        """
        now = time.time()
        timed_out = []

        for sm in self._jobs.values():
            if not sm.is_active:
                continue
            if sm.job_exec.timeout_at is None:
                continue
            if now > sm.job_exec.timeout_at:
                if sm.transition("timeout", {"error": "Execution timeout"}):
                    timed_out.append(sm)
                    logger.warning(
                        f"Job {sm.job_exec.job_execution_id} timed out "
                        f"(state was {sm.job_exec.state.value})"
                    )

        return timed_out

    def prune_completed(self, max_age: float = 3600) -> int:
        """
        Remove old completed jobs from memory.

        Args:
            max_age: Maximum age in seconds for terminal jobs

        Returns:
            Number of jobs removed
        """
        now = time.time()
        to_remove = []

        for job_id, sm in self._jobs.items():
            if not sm.is_terminal:
                continue
            # Use finished_at if available, otherwise current time
            finished = sm.job_exec.finished_at or now
            if now - finished > max_age:
                to_remove.append(job_id)

        for job_id in to_remove:
            tree_id = self._jobs[job_id].job_exec.tree_execution_id
            del self._jobs[job_id]
            if tree_id in self._by_tree:
                self._by_tree[tree_id] = [
                    j for j in self._by_tree[tree_id] if j != job_id
                ]
                # Clean up empty tree entries
                if not self._by_tree[tree_id]:
                    del self._by_tree[tree_id]

        if to_remove:
            logger.info(f"Pruned {len(to_remove)} completed jobs")

        return len(to_remove)

    def remove_tree(self, tree_execution_id: str) -> int:
        """
        Remove all jobs for a tree execution.

        Args:
            tree_execution_id: Tree to remove

        Returns:
            Number of jobs removed
        """
        job_ids = self._by_tree.pop(tree_execution_id, [])
        for job_id in job_ids:
            self._jobs.pop(job_id, None)
        return len(job_ids)

    def active_count(self) -> int:
        """Get count of active jobs."""
        return sum(1 for sm in self._jobs.values() if sm.is_active)

    def __len__(self) -> int:
        """Return total number of tracked jobs."""
        return len(self._jobs)

    def get_recent(self, limit: int = 20) -> list[JobStateMachine]:
        """
        Get most recent jobs (by started_at or creation order).

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of JobStateMachines, most recent first
        """
        jobs = list(self._jobs.values())
        # Sort by started_at descending (None values last)
        jobs.sort(
            key=lambda sm: sm.job_exec.started_at or 0,
            reverse=True,
        )
        return jobs[:limit]
