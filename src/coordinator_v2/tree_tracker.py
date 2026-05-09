"""
TreeTracker - Chain execution state tracking.

Tracks tree-level execution state for multi-job chains.
Critical for chain continuation after job completion.
"""

import logging
import time
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

# Import from existing coordinator module
from coordinator.models import Job, JobTree

logger = logging.getLogger(__name__)


class TreeState(Enum):
    """Tree execution states."""

    RUNNING = "running"  # Jobs executing
    WAITING_EXECUTOR = "waiting_executor"  # Waiting for airdrop completion
    COLLECTING = "collecting"  # Leaf job done, collecting outputs
    COMPLETED = "completed"  # All jobs done, outputs collected
    FAILED = "failed"  # Chain failed


class TreeExecution(BaseModel):
    """
    Represents a single tree execution.

    Tracks chain progress through multiple jobs.
    """

    tree_execution_id: str
    tree: JobTree  # Reference to tree definition
    current_index: int = 0  # Current job index in tree.jobs[]
    state: TreeState = TreeState.RUNNING
    job_exec_ids: list[str] = Field(default_factory=list)  # All job_execution_ids
    dev_client_id: str | None = None  # For progress forwarding
    started_at: float = Field(default_factory=time.time)
    finished_at: float | None = None
    error: str | None = None

    # WAITING_EXECUTOR state tracking
    waiting_job_id: str | None = None  # Job we're waiting for executor to install
    waiting_since: float | None = None  # When we started waiting

    model_config = ConfigDict(
        arbitrary_types_allowed=True,  # Allow JobTree
        use_enum_values=False,  # Keep TreeState as enum
    )


class TreeTracker:
    """
    Tracks tree-level execution state for chain management.

    Single-threaded async design - no locks needed.
    """

    def __init__(self) -> None:
        """Initialize empty tree tracker."""
        self._trees: dict[str, TreeExecution] = {}

    def create(
        self,
        tree_exec_id: str,
        tree: JobTree,
        dev_client_id: str | None = None,
    ) -> TreeExecution:
        """
        Start tracking a new tree execution.

        Args:
            tree_exec_id: Unique execution ID
            tree: JobTree definition
            dev_client_id: Client ID for progress forwarding (if triggered by ljc exec)

        Returns:
            Created TreeExecution
        """
        tex = TreeExecution(
            tree_execution_id=tree_exec_id,
            tree=tree,
            dev_client_id=dev_client_id,
        )
        self._trees[tree_exec_id] = tex

        logger.info(
            f"Tracking tree execution: {tree_exec_id} "
            f"({len(tree.jobs)} jobs in chain)"
        )
        return tex

    def get(self, tree_exec_id: str) -> TreeExecution | None:
        """Get tree execution by ID."""
        return self._trees.get(tree_exec_id)

    def advance(self, tree_exec_id: str) -> int | None:
        """
        Advance to next job in chain.

        Args:
            tree_exec_id: Tree execution ID

        Returns:
            New job index, or None if chain complete
        """
        tex = self._trees.get(tree_exec_id)
        if not tex:
            logger.warning(f"Cannot advance unknown tree: {tree_exec_id}")
            return None

        tex.current_index += 1

        if tex.current_index >= len(tex.tree.jobs):
            tex.state = TreeState.COLLECTING
            logger.info(
                f"Tree {tree_exec_id} reached leaf job, transitioning to COLLECTING"
            )
            return None

        logger.debug(
            f"Tree {tree_exec_id} advanced to job index {tex.current_index}"
        )
        return tex.current_index

    def get_current_job(self, tree_exec_id: str) -> Job | None:
        """
        Get currently executing job in chain.

        Args:
            tree_exec_id: Tree execution ID

        Returns:
            Current Job or None if not found or chain complete
        """
        tex = self._trees.get(tree_exec_id)
        if not tex:
            return None
        if tex.current_index >= len(tex.tree.jobs):
            return None
        return tex.tree.jobs[tex.current_index]

    def get_previous_job(self, tree_exec_id: str) -> Job | None:
        """
        Get previous job (for intermediate input preparation).

        Args:
            tree_exec_id: Tree execution ID

        Returns:
            Previous Job or None if at root
        """
        tex = self._trees.get(tree_exec_id)
        if not tex or tex.current_index == 0:
            return None
        return tex.tree.jobs[tex.current_index - 1]

    def is_last_job(self, tree_exec_id: str) -> bool:
        """
        Check if current job is the leaf job.

        Args:
            tree_exec_id: Tree execution ID

        Returns:
            True if current job is the last in chain
        """
        tex = self._trees.get(tree_exec_id)
        if not tex:
            return True  # Defensive default
        return tex.current_index >= len(tex.tree.jobs) - 1

    def mark_waiting(self, tree_exec_id: str, job_id: str) -> None:
        """
        Mark tree as waiting for executor to install job.

        Called when chain continuation finds no executor has the next job.
        Chain will be resumed by heartbeat handler when job becomes available.

        Args:
            tree_exec_id: Tree execution ID
            job_id: Job ID we're waiting for
        """
        tex = self._trees.get(tree_exec_id)
        if tex:
            tex.state = TreeState.WAITING_EXECUTOR
            tex.waiting_job_id = job_id
            tex.waiting_since = time.time()
            logger.info(
                f"Tree {tree_exec_id} waiting for executor to install {job_id}"
            )

    def clear_waiting(self, tree_exec_id: str) -> None:
        """
        Clear waiting state (when resuming chain).

        Args:
            tree_exec_id: Tree execution ID
        """
        tex = self._trees.get(tree_exec_id)
        if tex:
            tex.state = TreeState.RUNNING
            tex.waiting_job_id = None
            tex.waiting_since = None

    def get_waiting_trees(self) -> list[TreeExecution]:
        """
        Get all trees waiting for executor.

        Returns:
            List of TreeExecutions in WAITING_EXECUTOR state
        """
        return [
            tex
            for tex in self._trees.values()
            if tex.state == TreeState.WAITING_EXECUTOR
        ]

    def mark_collecting(self, tree_exec_id: str) -> None:
        """Mark tree as collecting outputs."""
        tex = self._trees.get(tree_exec_id)
        if tex:
            tex.state = TreeState.COLLECTING

    def mark_failed(self, tree_exec_id: str, error: str) -> None:
        """
        Mark tree execution as failed.

        Args:
            tree_exec_id: Tree execution ID
            error: Error message
        """
        tex = self._trees.get(tree_exec_id)
        if tex:
            tex.state = TreeState.FAILED
            tex.error = error
            tex.finished_at = time.time()
            logger.warning(f"Tree {tree_exec_id} failed: {error}")

    def mark_completed(self, tree_exec_id: str) -> None:
        """Mark tree execution as completed."""
        tex = self._trees.get(tree_exec_id)
        if tex:
            tex.state = TreeState.COMPLETED
            tex.finished_at = time.time()
            logger.info(f"Tree {tree_exec_id} completed successfully")

    def remove(self, tree_exec_id: str) -> TreeExecution | None:
        """
        Remove completed/failed tree from tracking.

        Args:
            tree_exec_id: Tree execution ID

        Returns:
            Removed TreeExecution or None if not found
        """
        tex = self._trees.pop(tree_exec_id, None)
        if tex:
            logger.debug(f"Removed tree execution: {tree_exec_id}")
        return tex

    def get_active(self) -> list[TreeExecution]:
        """
        Get all active tree executions.

        Returns:
            Trees in RUNNING, WAITING_EXECUTOR, or COLLECTING state
        """
        active_states = {TreeState.RUNNING, TreeState.WAITING_EXECUTOR, TreeState.COLLECTING}
        return [
            tex
            for tex in self._trees.values()
            if tex.state in active_states
        ]

    def get_by_state(self, state: TreeState) -> list[TreeExecution]:
        """Get all trees in a specific state."""
        return [tex for tex in self._trees.values() if tex.state == state]

    def prune_completed(self, max_age: float = 3600) -> int:
        """
        Remove old completed/failed trees.

        Args:
            max_age: Maximum age in seconds for terminal trees

        Returns:
            Number of trees removed
        """
        now = time.time()
        terminal_states = {TreeState.COMPLETED, TreeState.FAILED}
        to_remove = []

        for tree_id, tex in self._trees.items():
            if tex.state not in terminal_states:
                continue
            finished = tex.finished_at or now
            if now - finished > max_age:
                to_remove.append(tree_id)

        for tree_id in to_remove:
            del self._trees[tree_id]

        if to_remove:
            logger.info(f"Pruned {len(to_remove)} completed/failed trees")

        return len(to_remove)

    def __len__(self) -> int:
        """Return number of tracked trees."""
        return len(self._trees)

    def active_count(self) -> int:
        """Return number of active trees."""
        return len(self.get_active())
