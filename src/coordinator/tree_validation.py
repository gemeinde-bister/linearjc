"""
Job tree validation for output conflict detection.

This module validates that job trees don't have conflicting output destinations,
implementing a fail-fast approach to prevent race conditions at validation time
rather than allowing silent data corruption at runtime.

Design Principle:
- Write-by-one, read-by-many: Only one tree can write to a filesystem location
- Reads are unrestricted: Multiple trees can read from same location
- Validation happens before execution: Fail fast with clear error messages
"""

import logging
from typing import Dict, List, Set
from pathlib import Path

from coordinator.models import JobTree, DataRegistryEntry

logger = logging.getLogger(__name__)


class OutputConflictError(Exception):
    """
    Raised when a tree has output conflicts with currently active trees.

    This is a validation error that should prevent the tree from being scheduled.
    """
    pass


class TreeOutputValidator:
    """
    Validates job trees for output destination conflicts.

    This class tracks which filesystem outputs are currently being written to
    by active trees and prevents new trees from writing to the same locations.
    """

    def __init__(self, data_registry: Dict[str, DataRegistryEntry]):
        """
        Initialize validator with data registry.

        Args:
            data_registry: Map of registry key -> registry entry
        """
        self.data_registry = data_registry
        # Track which filesystem paths are currently locked by which tree
        self._active_fs_outputs: Dict[str, str] = {}  # path -> tree_id
        logger.info("TreeOutputValidator initialized")

    def _normalize_path(self, fs_path: str) -> str:
        """Normalize filesystem path for comparison."""
        return str(Path(fs_path).resolve())

    def _get_tree_fs_outputs(self, tree: JobTree) -> Set[str]:
        """
        Extract all filesystem output paths from a tree.

        Args:
            tree: Job tree to analyze

        Returns:
            Set of normalized filesystem paths this tree writes to

        Raises:
            ValueError: If tree references unknown registry keys
        """
        fs_outputs = set()

        # Only check root job writes (tree outputs)
        # Individual job writes within the tree are transient
        if tree.root.writes:
            for registry_key in tree.root.writes:
                if registry_key not in self.data_registry:
                    raise ValueError(
                        f"Tree '{tree.root.id}' references unknown registry key: "
                        f"'{registry_key}'"
                    )

                registry_entry = self.data_registry[registry_key]

                # Only filesystem outputs can conflict
                # MinIO outputs are isolated by unique object keys
                if registry_entry.type == "fs":
                    if registry_entry.path:
                        normalized = self._normalize_path(registry_entry.path)
                        fs_outputs.add(normalized)
                        logger.debug(
                            f"Tree '{tree.root.id}' writes to filesystem: {normalized}"
                        )

        return fs_outputs

    def validate_tree(self, tree: JobTree) -> None:
        """
        Validate that tree doesn't conflict with currently active trees.

        Args:
            tree: Job tree to validate

        Raises:
            OutputConflictError: If tree would write to a location currently
                being written to by another active tree
            ValueError: If tree references unknown registry keys

        Example:
            validator = TreeOutputValidator(data_registry)

            try:
                validator.validate_tree(new_tree)
                validator.register_tree(new_tree)
                # ... execute tree ...
            except OutputConflictError as e:
                logger.error(f"Cannot schedule tree: {e}")
        """
        tree_id = tree.root.id
        fs_outputs = self._get_tree_fs_outputs(tree)

        if not fs_outputs:
            logger.debug(f"Tree '{tree_id}' has no filesystem outputs, validation skipped")
            return

        # Check for conflicts with active trees
        conflicts = []
        for fs_path in fs_outputs:
            if fs_path in self._active_fs_outputs:
                conflicting_tree = self._active_fs_outputs[fs_path]
                conflicts.append((fs_path, conflicting_tree))

        if conflicts:
            # Build detailed error message
            error_lines = [
                f"Output conflict detected for tree '{tree_id}':",
                "",
            ]

            for fs_path, conflicting_tree in conflicts:
                # Find registry key for this path (for better error message)
                registry_key = None
                for key, entry in self.data_registry.items():
                    if entry.type == "fs" and entry.path:
                        if self._normalize_path(entry.path) == fs_path:
                            registry_key = key
                            break

                error_lines.append(
                    f"  • Path: {fs_path}"
                )
                if registry_key:
                    error_lines.append(f"    Registry key: '{registry_key}'")
                error_lines.append(
                    f"    Conflict with: tree '{conflicting_tree}' (currently active)"
                )
                error_lines.append("")

            error_lines.extend([
                "Resolution options:",
                "  1. Wait for conflicting tree(s) to complete, then retry",
                "  2. Change output destination to avoid conflict",
                "  3. Use versioned outputs (e.g., output_v1, output_v2)",
                "  4. Combine trees into single pipeline with dependencies",
            ])

            raise OutputConflictError("\n".join(error_lines))

        logger.info(f"Tree '{tree_id}' validated successfully (no conflicts)")

    def register_tree(self, tree: JobTree) -> None:
        """
        Register tree's filesystem outputs as active.

        This should be called after validation and before execution starts.

        Args:
            tree: Job tree to register

        Note:
            If tree has no filesystem outputs, this is a no-op.
        """
        tree_id = tree.root.id
        fs_outputs = self._get_tree_fs_outputs(tree)

        for fs_path in fs_outputs:
            self._active_fs_outputs[fs_path] = tree_id
            logger.info(f"Registered filesystem output: {fs_path} -> tree '{tree_id}'")

    def unregister_tree(self, tree: JobTree) -> None:
        """
        Unregister tree's filesystem outputs (tree completed or failed).

        This should be called when tree execution finishes (success or failure).

        Args:
            tree: Job tree to unregister

        Note:
            If tree has no filesystem outputs, this is a no-op.
            If tree wasn't registered, this is safe (no error).
        """
        tree_id = tree.root.id
        fs_outputs = self._get_tree_fs_outputs(tree)

        for fs_path in fs_outputs:
            if fs_path in self._active_fs_outputs:
                if self._active_fs_outputs[fs_path] == tree_id:
                    del self._active_fs_outputs[fs_path]
                    logger.info(
                        f"Unregistered filesystem output: {fs_path} (tree '{tree_id}')"
                    )
                else:
                    logger.warning(
                        f"Attempted to unregister {fs_path} for tree '{tree_id}', "
                        f"but it's owned by '{self._active_fs_outputs[fs_path]}'"
                    )

    def get_active_outputs(self) -> Dict[str, str]:
        """
        Get currently active filesystem outputs.

        Returns:
            Dict mapping filesystem path -> tree ID

        Note:
            For debugging/monitoring purposes.
        """
        return dict(self._active_fs_outputs)

    def validate_tree_configurations(self, trees: List[JobTree]) -> None:
        """
        Validate that tree configurations don't have conflicting outputs.

        This is STATIC validation at load time - checks if job configurations
        have conflicts that would prevent them from ever running concurrently.

        Different from validate_tree() which checks runtime conflicts with
        currently active trees.

        Args:
            trees: List of all job trees to validate

        Raises:
            OutputConflictError: If trees have overlapping filesystem outputs

        Note:
            This should be called once at startup (and on reload) to validate
            the job configurations.
        """
        # Build map of filesystem outputs: path -> list of trees
        fs_outputs = {}  # path -> [(tree_id, output_name, registry_key), ...]

        for tree in trees:
            tree_id = tree.root.id

            try:
                tree_fs_outputs = self._get_tree_fs_outputs(tree)

                for fs_path in tree_fs_outputs:
                    if fs_path not in fs_outputs:
                        fs_outputs[fs_path] = []

                    # Find which write maps to this path
                    for registry_key in tree.root.writes:
                        entry = self.data_registry[registry_key]
                        if entry.type == "fs" and entry.path:
                            if self._normalize_path(entry.path) == fs_path:
                                fs_outputs[fs_path].append((tree_id, registry_key))
                                break

            except ValueError as e:
                # _get_tree_fs_outputs raises ValueError for unknown registry keys
                # Re-raise as OutputConflictError for consistency
                raise OutputConflictError(str(e))

        # Check for conflicts (multiple trees writing to same path)
        conflicts = []
        for fs_path, tree_infos in fs_outputs.items():
            if len(tree_infos) > 1:
                conflicts.append((fs_path, tree_infos))

        if conflicts:
            error_lines = [
                "CONFIGURATION ERROR: Multiple trees write to the same filesystem locations.",
                "",
                "Trees cannot run concurrently if they write to the same filesystem path.",
                "The following conflicts were found:",
                ""
            ]

            for fs_path, tree_infos in conflicts:
                error_lines.append(f"  Filesystem path: {fs_path}")
                for tree_id, registry_key in tree_infos:
                    error_lines.append(
                        f"    - Tree '{tree_id}' (writes '{registry_key}')"
                    )
                error_lines.append("")

            error_lines.extend([
                "Resolution options:",
                "  1. Change output destinations to use different filesystem paths",
                "  2. Use versioned outputs in data registry (e.g., hello_output_v1, hello_output_v2)",
                "  3. Combine trees into a single pipeline with dependencies",
                "  4. Use MinIO outputs instead of filesystem outputs",
            ])

            raise OutputConflictError("\n".join(error_lines))
