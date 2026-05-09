"""
TreeValidator - Output conflict detection.

Prevents multiple trees from writing to the same filesystem outputs
simultaneously. MinIO outputs don't conflict (unique object paths).
"""

import logging
from pathlib import Path

# Import from existing coordinator module
from coordinator.models import DataRegistryEntry, JobTree

logger = logging.getLogger(__name__)


class TreeValidator:
    """
    Validates trees can execute without output conflicts.

    Tracks active filesystem output paths to prevent concurrent writes
    to the same locations. Single-threaded, no locks needed.
    """

    def __init__(self, data_registry: dict[str, DataRegistryEntry]) -> None:
        """
        Initialize validator with data registry.

        Args:
            data_registry: Mapping of registry keys to DataRegistryEntry
        """
        self._registry = data_registry
        self._active_outputs: dict[str, str] = {}  # path -> tree_exec_id

    def update_registry(self, data_registry: dict[str, DataRegistryEntry]) -> None:
        """
        Update the data registry reference.

        Called after hot reload to use new registry.

        Args:
            data_registry: New data registry
        """
        self._registry = data_registry

    def can_execute(
        self, tree: JobTree, tree_exec_id: str
    ) -> tuple[bool, str | None]:
        """
        Check if tree can execute without conflicts.

        Args:
            tree: JobTree to check
            tree_exec_id: ID for this execution

        Returns:
            (True, None) if can execute
            (False, error_message) if blocked
        """
        outputs = self._get_fs_outputs(tree)

        for path in outputs:
            if path in self._active_outputs:
                blocking_tree = self._active_outputs[path]
                error = f"Output {path} locked by {blocking_tree}"
                logger.debug(f"Tree {tree.root.id} blocked: {error}")
                return False, error

        return True, None

    def register(self, tree: JobTree, tree_exec_id: str) -> None:
        """
        Register tree's outputs as locked.

        Must be called before dispatching the tree's root job.

        Args:
            tree: JobTree starting execution
            tree_exec_id: Unique execution ID
        """
        outputs = self._get_fs_outputs(tree)

        for path in outputs:
            self._active_outputs[path] = tree_exec_id
            logger.debug(f"Locked output {path} for {tree_exec_id}")

        if outputs:
            logger.info(
                f"Registered {len(outputs)} output lock(s) for tree {tree.root.id}"
            )

    def unregister(self, tree_exec_id: str) -> int:
        """
        Release tree's output locks.

        Must be called when tree completes or fails.

        Args:
            tree_exec_id: Tree execution to release

        Returns:
            Number of locks released
        """
        to_remove = [
            path
            for path, tid in self._active_outputs.items()
            if tid == tree_exec_id
        ]

        for path in to_remove:
            del self._active_outputs[path]
            logger.debug(f"Released output lock: {path}")

        if to_remove:
            logger.info(
                f"Released {len(to_remove)} output lock(s) for {tree_exec_id}"
            )

        return len(to_remove)

    def _get_fs_outputs(self, tree: JobTree) -> list[str]:
        """
        Extract filesystem output paths from tree's leaf job.

        Only leaf job outputs matter - intermediate outputs go to MinIO.

        Args:
            tree: JobTree to analyze

        Returns:
            List of absolute filesystem paths
        """
        outputs = []
        leaf_job = tree.jobs[-1]  # Last job in chain

        for key in leaf_job.writes:
            entry = self._registry.get(key)
            if entry and entry.type == "fs" and entry.path:
                # Resolve to absolute path
                resolved = str(Path(entry.path).resolve())
                outputs.append(resolved)

        return outputs

    def get_active_locks(self) -> dict[str, str]:
        """
        Get all active output locks.

        Returns:
            Dict mapping path -> tree_exec_id
        """
        return self._active_outputs.copy()

    def is_locked(self, path: str) -> str | None:
        """
        Check if a path is locked.

        Args:
            path: Filesystem path to check

        Returns:
            tree_exec_id if locked, None otherwise
        """
        resolved = str(Path(path).resolve())
        return self._active_outputs.get(resolved)

    def __len__(self) -> int:
        """Return number of active locks."""
        return len(self._active_outputs)
