"""
Job discovery - recursively scan directories for job YAML files.
"""
import logging
from pathlib import Path
from typing import List, Dict
import yaml
from pydantic import ValidationError

from coordinator.models import Job, JobFile

logger = logging.getLogger(__name__)


class JobDiscoveryError(Exception):
    """Error during job discovery."""
    pass


def discover_jobs(jobs_dir: str) -> List[Job]:
    """
    Recursively discover all job YAML files in the given directory.

    Args:
        jobs_dir: Root directory to scan for job files

    Returns:
        List of Job objects

    Raises:
        JobDiscoveryError: If discovery fails
    """
    jobs_path = Path(jobs_dir)

    if not jobs_path.exists():
        raise JobDiscoveryError(f"Jobs directory does not exist: {jobs_dir}")

    if not jobs_path.is_dir():
        raise JobDiscoveryError(f"Jobs path is not a directory: {jobs_dir}")

    # Find all YAML files recursively
    yaml_files = []
    for pattern in ['**/*.yaml', '**/*.yml']:
        yaml_files.extend(jobs_path.glob(pattern))

    logger.info(f"Found {len(yaml_files)} YAML files in {jobs_dir}")

    jobs = []
    errors = []

    for yaml_file in yaml_files:
        try:
            job = load_job_file(yaml_file)
            job.file_path = yaml_file
            jobs.append(job)
            logger.debug(f"Loaded job: {job.id} from {yaml_file}")
        except Exception as e:
            error_msg = f"{yaml_file}: {str(e)}"
            errors.append(error_msg)
            logger.error(f"Failed to load job file: {error_msg}")

    if errors:
        logger.warning(f"Failed to load {len(errors)} job files:")
        for error in errors:
            logger.warning(f"  - {error}")

    logger.info(f"Successfully loaded {len(jobs)} jobs")

    return jobs


def load_job_file(file_path: Path) -> Job:
    """
    Load a single job YAML file.

    Args:
        file_path: Path to YAML file

    Returns:
        Job object

    Raises:
        ValueError: If file is invalid
    """
    try:
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)

        if not data:
            raise ValueError("Empty YAML file")

        if 'job' not in data:
            raise ValueError("Missing 'job' key at root level")

        # Validate using Pydantic
        job_file = JobFile(**data)
        return job_file.job

    except yaml.YAMLError as e:
        raise ValueError(f"YAML parse error: {e}")
    except ValidationError as e:
        raise ValueError(f"Validation error: {e}")


def validate_jobs(jobs: List[Job]) -> Dict[str, List[str]]:
    """
    Validate a list of jobs for consistency.

    Checks:
    - Unique job IDs
    - Valid dependencies (all referenced jobs exist)
    - No self-dependencies

    Args:
        jobs: List of Job objects

    Returns:
        Dictionary of validation errors by job ID (empty if valid)
    """
    errors: Dict[str, List[str]] = {}
    job_ids = {job.id for job in jobs}

    # Check for duplicate IDs
    seen_ids = set()
    for job in jobs:
        if job.id in seen_ids:
            if job.id not in errors:
                errors[job.id] = []
            errors[job.id].append(f"Duplicate job ID: {job.id}")
        seen_ids.add(job.id)

    # Check dependencies
    for job in jobs:
        job_errors = []

        # Check for self-dependency
        if job.id in job.depends:
            job_errors.append(f"Job depends on itself")

        # Check that all dependencies exist
        for dep_id in job.depends:
            if dep_id not in job_ids:
                job_errors.append(f"Dependency '{dep_id}' does not exist")

        if job_errors:
            if job.id not in errors:
                errors[job.id] = []
            errors[job.id].extend(job_errors)

    return errors


def load_data_registry(registry_path: str) -> Dict[str, any]:
    """
    Load the data registry YAML file.

    Args:
        registry_path: Path to data registry YAML file

    Returns:
        Dictionary mapping logical names to registry entries

    Raises:
        ValueError: If registry file is invalid
    """
    try:
        with open(registry_path, 'r') as f:
            data = yaml.safe_load(f)

        if not data or 'registry' not in data:
            raise ValueError("Missing 'registry' key at root level")

        return data['registry']

    except FileNotFoundError:
        raise ValueError(f"Data registry file not found: {registry_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"YAML parse error: {e}")
