"""
Archive handling for LinearJC coordinator.

All transfers use tar.gz format automatically:
- Archives contain directory contents, not the directory itself
- Preserves permissions, timestamps, symlinks
- Works with both directories and single files
- Simple, predictable, auditable

Security: Uses Python's tarfile module to prevent command injection.
"""
import logging
import shutil
import tarfile
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


class ArchiveError(Exception):
    """Error during archive operations."""
    pass


def create_archive(source_path: str, archive_path: str) -> None:
    """
    Create a tar.gz archive from a file or directory.

    Archives are always tar.gz format - no configuration needed.
    Uses Python's tarfile module for security (prevents command injection).

    Args:
        source_path: Path to file or directory to archive
        archive_path: Destination path for archive (should end in .tar.gz)

    Raises:
        ArchiveError: If archive creation fails
    """
    source = Path(source_path)

    if not source.exists():
        raise ArchiveError(f"Source path does not exist: {source_path}")

    archive_dir = Path(archive_path).parent
    archive_dir.mkdir(parents=True, exist_ok=True)

    logger.debug(f"Creating tar.gz archive: {archive_path}")

    try:
        with tarfile.open(archive_path, 'w:gz') as tar:
            if source.is_file():
                # Archive single file (use arcname to store just filename, not full path)
                tar.add(source, arcname=source.name)
            else:
                # Archive directory contents (not directory itself)
                for item in source.iterdir():
                    tar.add(item, arcname=item.name)

        logger.info(f"Created archive: {archive_path}")

    except tarfile.TarError as e:
        raise ArchiveError(f"Failed to create tar archive: {e}")
    except Exception as e:
        raise ArchiveError(f"Failed to create archive: {e}")


def extract_archive(archive_path: str, dest_path: str, path_type: str = 'directory') -> None:
    """
    Extract a tar.gz archive to a destination file or directory.

    Uses Python's tarfile module for security (prevents command injection).
    Includes safety checks to prevent path traversal attacks during extraction.

    Args:
        archive_path: Path to tar.gz archive file
        dest_path: Destination path (file or directory based on path_type)
        path_type: 'directory' (default) or 'file'

    Raises:
        ArchiveError: If extraction fails, archive contains unsafe paths,
                     or archive content doesn't match path_type
    """
    archive = Path(archive_path)

    if not archive.exists():
        raise ArchiveError(f"Archive does not exist: {archive_path}")

    if path_type == 'directory':
        _extract_to_directory(archive_path, dest_path)
    elif path_type == 'file':
        _extract_to_file(archive_path, dest_path)
    else:
        raise ArchiveError(f"Invalid path_type: {path_type}. Must be 'file' or 'directory'")


def _extract_to_directory(archive_path: str, dest_path: str) -> None:
    """Extract archive contents to a directory."""
    dest_dir = Path(dest_path).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.debug(f"Extracting tar.gz archive to directory: {dest_path}")

    try:
        with tarfile.open(archive_path, 'r:gz') as tar:
            # Security: Check for path traversal in archive members
            for member in tar.getmembers():
                member_path = Path(dest_dir) / member.name
                # Resolve to absolute path and check it's within dest_dir
                try:
                    member_path.resolve().relative_to(dest_dir.resolve())
                except ValueError:
                    raise ArchiveError(
                        f"Archive contains unsafe path: {member.name} "
                        f"(attempts to escape destination directory)"
                    )

            # Extract all members (safe after validation)
            tar.extractall(dest_dir)

        logger.info(f"Extracted archive to directory: {archive_path} -> {dest_path}")

    except tarfile.TarError as e:
        raise ArchiveError(f"Failed to extract tar archive: {e}")
    except Exception as e:
        raise ArchiveError(f"Failed to extract archive: {e}")


def _extract_to_file(archive_path: str, dest_path: str) -> None:
    """
    Extract archive to a single file.

    Validates that archive contains exactly one file (not a directory).
    Extracts to temporary location first, then moves to destination.
    """
    logger.debug(f"Extracting tar.gz archive to file: {dest_path}")

    # Create temporary directory for extraction
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Extract to temporary directory
        with tarfile.open(archive_path, 'r:gz') as tar:
            # Security: Check for path traversal
            for member in tar.getmembers():
                member_path = Path(temp_dir) / member.name
                try:
                    member_path.resolve().relative_to(temp_dir.resolve())
                except ValueError:
                    raise ArchiveError(
                        f"Archive contains unsafe path: {member.name}"
                    )

            tar.extractall(temp_dir)

        # Find extracted files
        extracted = list(temp_dir.iterdir())

        # Validate exactly one file
        if len(extracted) == 0:
            raise ArchiveError(
                "Archive is empty but path_type='file' requires one file"
            )
        elif len(extracted) > 1:
            raise ArchiveError(
                f"Archive contains {len(extracted)} items but path_type='file' "
                f"requires exactly one file. Items: {[f.name for f in extracted]}"
            )

        extracted_item = extracted[0]

        # Validate it's a file, not a directory
        if extracted_item.is_dir():
            raise ArchiveError(
                f"Archive contains directory '{extracted_item.name}' "
                f"but path_type='file' requires a file"
            )

        # Move file to destination
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(extracted_item), str(dest))

        logger.info(f"Extracted archive to file: {archive_path} -> {dest_path}")

    except tarfile.TarError as e:
        raise ArchiveError(f"Failed to extract tar archive: {e}")
    except Exception as e:
        raise ArchiveError(f"Failed to extract archive: {e}")
    finally:
        # Clean up temporary directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
