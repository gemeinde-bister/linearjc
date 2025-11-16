"""
Output filesystem lock manager for atomic write operations.

This module provides per-filesystem-target locking to ensure that concurrent
jobs writing to the same filesystem destination don't corrupt each other's data.

Design:
- One lock per unique filesystem path
- Locks are created on-demand and persist for the lifetime of the coordinator
- Thread-safe access via meta-lock pattern
- Read operations don't acquire locks (read-many, write-one pattern)
"""

import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator

logger = logging.getLogger(__name__)


class OutputLockManager:
    """
    Manages per-filesystem-path locks for atomic write operations.

    This ensures that when multiple jobs write to the same filesystem destination,
    their extract operations don't interleave and corrupt data.

    Example:
        lock_manager = OutputLockManager()

        # Job A writing to /var/data/results
        with lock_manager.acquire("/var/data/results"):
            extract_tar_gz(archive, "/var/data/results")

        # Job B writing to /var/data/results (will wait for Job A)
        with lock_manager.acquire("/var/data/results"):
            extract_tar_gz(archive, "/var/data/results")
    """

    def __init__(self):
        """Initialize the lock manager with empty lock registry."""
        self._locks: Dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()
        logger.info("OutputLockManager initialized")

    def _normalize_path(self, fs_path: str) -> str:
        """
        Normalize filesystem path to canonical form for lock key.

        This ensures that /var/data/output and /var/data/output/ map to same lock.

        Args:
            fs_path: Filesystem path (absolute or relative)

        Returns:
            Normalized absolute path as string
        """
        return str(Path(fs_path).resolve())

    def get_lock(self, fs_path: str) -> threading.Lock:
        """
        Get or create a lock for the given filesystem path.

        This method is thread-safe. If multiple threads request a lock for the
        same path simultaneously, only one lock object is created.

        Args:
            fs_path: Filesystem path to get lock for

        Returns:
            threading.Lock instance for this path
        """
        normalized_path = self._normalize_path(fs_path)

        with self._meta_lock:
            if normalized_path not in self._locks:
                logger.debug(f"Creating new lock for path: {normalized_path}")
                self._locks[normalized_path] = threading.Lock()
            return self._locks[normalized_path]

    @contextmanager
    def acquire(self, fs_path: str) -> Generator[None, None, None]:
        """
        Context manager for acquiring and releasing a filesystem path lock.

        This ensures the lock is always released, even if an exception occurs
        during the write operation.

        Args:
            fs_path: Filesystem path to lock

        Yields:
            None (context manager)

        Example:
            with lock_manager.acquire("/var/data/output"):
                # This section has exclusive write access
                extract_tar_gz(archive, "/var/data/output")
        """
        normalized_path = self._normalize_path(fs_path)
        lock = self.get_lock(normalized_path)

        logger.debug(f"Acquiring write lock for: {normalized_path}")
        lock.acquire()
        try:
            logger.debug(f"Write lock acquired for: {normalized_path}")
            yield
        finally:
            lock.release()
            logger.debug(f"Write lock released for: {normalized_path}")

    def get_lock_status(self) -> Dict[str, bool]:
        """
        Get current lock status for all registered paths.

        Returns:
            Dict mapping path -> locked status (True if locked)

        Note:
            This is best-effort and may be stale immediately after returning.
            Intended for debugging/monitoring, not for synchronization.
        """
        with self._meta_lock:
            return {
                path: lock.locked()
                for path, lock in self._locks.items()
            }
