"""
JobStateMachine - Explicit state machine for job execution.

All state transitions go through transition() which validates
the transition is legal. Invalid transitions are rejected.

State ownership:
- Coordinator reports: queued, timeout, collecting
- Executor reports: assigned, downloading, ready, running, uploading, completed, failed
"""

import logging
import time
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class JobState(Enum):
    """
    Canonical job states - matches Unified Protocol Specification.

    States are monotonic - no rollback to earlier states.
    """

    QUEUED = "queued"  # Coordinator: in queue, not yet dispatched
    ASSIGNED = "assigned"  # Executor: job received
    DOWNLOADING = "downloading"  # Executor: downloading inputs
    READY = "ready"  # Executor: inputs ready, about to execute
    RUNNING = "running"  # Executor: script executing
    UPLOADING = "uploading"  # Executor: uploading outputs
    COLLECTING = "collecting"  # Coordinator: downloading outputs to filesystem
    COMPLETED = "completed"  # Terminal: success
    FAILED = "failed"  # Terminal: failure
    TIMEOUT = "timeout"  # Terminal: coordinator timeout


class JobExecution(BaseModel):
    """
    Represents a single job execution.

    Tracks all metadata needed for the job lifecycle.
    """

    job_execution_id: str
    tree_execution_id: str
    job_id: str
    version: str
    state: JobState = JobState.QUEUED
    chain_index: int = 0  # Position in chain (0 = root)
    chain_length: int = 1  # Total jobs in tree
    executor_id: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    timeout_at: float | None = None
    error: str | None = None
    exit_code: int | None = None
    duration_ms: int | None = None
    dev_client_id: str | None = None  # For progress forwarding

    model_config = ConfigDict(use_enum_values=False)  # Keep JobState as enum


# Transition table: (current_state, event) -> new_state
# Events come from executor progress updates or coordinator actions
TRANSITIONS: dict[tuple[JobState, str], JobState] = {
    # Happy path (coordinator dispatch + executor progress)
    (JobState.QUEUED, "dispatch"): JobState.ASSIGNED,
    (JobState.ASSIGNED, "assigned"): JobState.ASSIGNED,  # Executor ack (idempotent)
    (JobState.ASSIGNED, "downloading"): JobState.DOWNLOADING,
    (JobState.DOWNLOADING, "downloading"): JobState.DOWNLOADING,  # Idempotent
    (JobState.DOWNLOADING, "ready"): JobState.READY,
    (JobState.READY, "ready"): JobState.READY,  # Idempotent
    (JobState.READY, "running"): JobState.RUNNING,
    (JobState.RUNNING, "running"): JobState.RUNNING,  # Idempotent
    (JobState.RUNNING, "uploading"): JobState.UPLOADING,
    (JobState.UPLOADING, "uploading"): JobState.UPLOADING,  # Idempotent
    (JobState.UPLOADING, "completed"): JobState.COLLECTING,
    # Direct completion (no outputs or already uploaded)
    (JobState.RUNNING, "completed"): JobState.COLLECTING,
    # Final collection to filesystem
    (JobState.COLLECTING, "collected"): JobState.COMPLETED,
    # Failures from any active state
    (JobState.ASSIGNED, "failed"): JobState.FAILED,
    (JobState.DOWNLOADING, "failed"): JobState.FAILED,
    (JobState.READY, "failed"): JobState.FAILED,
    (JobState.RUNNING, "failed"): JobState.FAILED,
    (JobState.UPLOADING, "failed"): JobState.FAILED,
    (JobState.COLLECTING, "failed"): JobState.FAILED,
    # Timeouts (coordinator-initiated)
    (JobState.ASSIGNED, "timeout"): JobState.TIMEOUT,
    (JobState.DOWNLOADING, "timeout"): JobState.TIMEOUT,
    (JobState.READY, "timeout"): JobState.TIMEOUT,
    (JobState.RUNNING, "timeout"): JobState.TIMEOUT,
    (JobState.UPLOADING, "timeout"): JobState.TIMEOUT,
    (JobState.COLLECTING, "timeout"): JobState.TIMEOUT,  # Fix: collecting can timeout
}


class JobStateMachine:
    """
    Explicit state machine for job execution.

    All transitions go through transition() - single point of control.
    Invalid transitions are rejected with a warning.
    """

    def __init__(self, job_exec: JobExecution) -> None:
        """
        Initialize state machine for job execution.

        Args:
            job_exec: The JobExecution instance to manage
        """
        self.job_exec = job_exec

    def transition(self, event: str, data: dict | None = None) -> bool:
        """
        Attempt state transition.

        Args:
            event: The event triggering the transition
            data: Optional data associated with the event

        Returns:
            True if transition successful, False if invalid
        """
        data = data or {}
        key = (self.job_exec.state, event)

        if key not in TRANSITIONS:
            logger.warning(
                f"Invalid transition: {self.job_exec.state.value} + {event} "
                f"for job {self.job_exec.job_execution_id}"
            )
            return False

        old_state = self.job_exec.state
        new_state = TRANSITIONS[key]

        # Update state
        self.job_exec.state = new_state

        # Update metadata based on event
        if event == "dispatch":
            self.job_exec.executor_id = data.get("executor_id")
        elif event == "running":
            if self.job_exec.started_at is None:
                self.job_exec.started_at = time.time()
        elif event in ("failed", "timeout"):
            self.job_exec.error = data.get("error")
            self.job_exec.exit_code = data.get("exit_code")
            self.job_exec.finished_at = time.time()
            if data.get("duration_ms"):
                self.job_exec.duration_ms = data["duration_ms"]
        elif event in ("completed", "collected"):
            self.job_exec.exit_code = data.get("exit_code", 0)
            self.job_exec.finished_at = time.time()
            if data.get("duration_ms"):
                self.job_exec.duration_ms = data["duration_ms"]

        logger.info(
            f"Job {self.job_exec.job_execution_id}: "
            f"{old_state.value} -> {new_state.value}"
        )
        return True

    @property
    def is_terminal(self) -> bool:
        """Check if job is in terminal state."""
        return self.job_exec.state in (
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.TIMEOUT,
        )

    @property
    def is_active(self) -> bool:
        """Check if job is active (dispatched but not terminal)."""
        return not self.is_terminal and self.job_exec.state != JobState.QUEUED

    @property
    def state(self) -> JobState:
        """Get current job state."""
        return self.job_exec.state

    def can_transition(self, event: str) -> bool:
        """Check if transition is valid without applying it."""
        key = (self.job_exec.state, event)
        return key in TRANSITIONS

    def get_valid_events(self) -> list[str]:
        """Get list of valid events from current state."""
        current = self.job_exec.state
        return [event for (state, event) in TRANSITIONS.keys() if state == current]
