"""
Job execution state tracking and timeout management.

Tracks all in-flight jobs, handles progress updates, detects timeouts.
"""
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class JobState(Enum):
    """Job execution states."""
    QUEUED = "queued"
    ASSIGNED = "assigned"
    DOWNLOADING = "downloading"
    READY = "ready"
    RUNNING = "running"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class JobExecution:
    """State of a job execution."""
    job_execution_id: str
    tree_execution_id: str
    job_id: str
    job_version: str
    state: JobState
    executor_id: Optional[str]
    assigned_at: Optional[float]
    timeout_at: Optional[float]
    last_progress: Optional[float]
    error_message: Optional[str] = None
    # If set, forward progress updates to this developer client via MQTT
    dev_client_id: Optional[str] = None
    # Position in tree.jobs[] for multi-job chains (0 = root, 1 = second job, etc.)
    job_index: int = 0

    def is_timed_out(self, now: float) -> bool:
        """Check if job has timed out."""
        if self.timeout_at is None:
            return False
        return now > self.timeout_at

    def can_timeout(self) -> bool:
        """Check if job is in a state where timeout is possible."""
        return self.state in [
            JobState.ASSIGNED,
            JobState.DOWNLOADING,
            JobState.READY,
            JobState.RUNNING,
            JobState.UPLOADING
        ]


class JobTracker:
    """
    Tracks all job executions and handles timeouts.

    Maintains state of all in-flight jobs, processes progress updates,
    detects timeouts.
    """

    def __init__(self):
        """Initialize job tracker."""
        self._jobs: Dict[str, JobExecution] = {}
        logger.info("Job tracker initialized")

    def add(self, job_exec: JobExecution) -> None:
        """
        Add a job execution to tracker.

        Args:
            job_exec: Job execution to track
        """
        self._jobs[job_exec.job_execution_id] = job_exec
        logger.info(
            f"Tracking job {job_exec.job_execution_id} "
            f"(job={job_exec.job_id}, executor={job_exec.executor_id})"
        )

    def update_state(
        self,
        job_execution_id: str,
        new_state: JobState,
        message: Optional[str] = None
    ) -> bool:
        """
        Update job execution state.

        Args:
            job_execution_id: Job execution ID
            new_state: New state
            message: Optional error message (for FAILED state)

        Returns:
            True if updated, False if job not found
        """
        if job_execution_id not in self._jobs:
            logger.warning(f"Cannot update state for unknown job: {job_execution_id}")
            return False

        job = self._jobs[job_execution_id]
        old_state = job.state
        job.state = new_state
        job.last_progress = time.time()

        if new_state == JobState.FAILED and message:
            job.error_message = message

        logger.info(
            f"Job {job_execution_id} state: {old_state.value} → {new_state.value}"
        )
        return True

    def handle_progress_update(self, job_execution_id: str, progress_msg: Dict) -> None:
        """
        Handle progress update from executor.

        Args:
            job_execution_id: Job execution ID
            progress_msg: Progress message payload
        """
        if job_execution_id not in self._jobs:
            logger.warning(f"Received progress for unknown job: {job_execution_id}")
            return

        state_str = progress_msg.get("state", "unknown")
        message = progress_msg.get("message")

        try:
            new_state = JobState(state_str)
            self.update_state(job_execution_id, new_state, message)
        except ValueError:
            logger.error(f"Invalid state in progress update: {state_str}")

    def check_timeouts(self) -> List[JobExecution]:
        """
        Check for timed-out jobs.

        Returns:
            List of jobs that timed out
        """
        now = time.time()
        timed_out = []

        for job_id, job in self._jobs.items():
            if job.can_timeout() and job.is_timed_out(now):
                logger.error(
                    f"Job {job_id} timed out "
                    f"(executor={job.executor_id}, state={job.state.value})"
                )
                job.state = JobState.TIMEOUT
                timed_out.append(job)

        return timed_out

    def get_job(self, job_execution_id: str) -> Optional[JobExecution]:
        """Get job execution by ID."""
        return self._jobs.get(job_execution_id)

    def get_all_jobs(self) -> Dict[str, JobExecution]:
        """Get all tracked jobs."""
        return self._jobs.copy()

    def remove_completed(self) -> int:
        """
        Remove completed/failed/timeout jobs from tracker.

        Returns:
            Number of jobs removed
        """
        terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
        to_remove = [
            job_id for job_id, job in self._jobs.items()
            if job.state in terminal_states
        ]

        for job_id in to_remove:
            del self._jobs[job_id]

        if to_remove:
            logger.info(f"Removed {len(to_remove)} completed jobs from tracker")

        return len(to_remove)

    def get_active_job_count(self) -> int:
        """Get number of active (non-terminal) jobs."""
        terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
        return sum(1 for job in self._jobs.values() if job.state not in terminal_states)

    def find_active_by_job_id(self, job_id: str) -> Optional[JobExecution]:
        """
        Find active (non-terminal) execution for a job.

        Args:
            job_id: Job ID to search for

        Returns:
            Most recent active JobExecution, or None if no active execution
        """
        terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
        candidates = [
            job for job in self._jobs.values()
            if job.job_id == job_id and job.state not in terminal_states
        ]

        if not candidates:
            return None

        # Return most recent by job_execution_id (contains timestamp)
        return max(candidates, key=lambda j: j.job_execution_id)

    def attach_dev_client(self, job_execution_id: str, dev_client_id: str) -> bool:
        """
        Attach a developer client for progress forwarding.

        Args:
            job_execution_id: Execution to attach to
            dev_client_id: Developer client ID for progress forwarding

        Returns:
            True if attached successfully, False if execution not found
        """
        if job_execution_id not in self._jobs:
            logger.warning(f"Cannot attach to unknown job: {job_execution_id}")
            return False

        job = self._jobs[job_execution_id]
        old_client = job.dev_client_id
        job.dev_client_id = dev_client_id

        if old_client:
            logger.info(
                f"Replaced dev client for {job_execution_id}: "
                f"{old_client} → {dev_client_id}"
            )
        else:
            logger.info(f"Attached dev client {dev_client_id} to {job_execution_id}")

        return True

    def find_by_tree_execution_id(self, tree_execution_id: str) -> List[JobExecution]:
        """
        Find all job executions for a tree.

        Args:
            tree_execution_id: Tree execution ID

        Returns:
            List of JobExecution objects for this tree, ordered by job_index
        """
        executions = [
            job for job in self._jobs.values()
            if job.tree_execution_id == tree_execution_id
        ]
        return sorted(executions, key=lambda j: j.job_index)
