#!/usr/bin/env python3
"""
Entry point for coordinator_v2.

This is the new async coordinator implementation using aiomqtt.
For production use, this replaces src/coordinator/main.py.

CLI Commands:
- run: Main scheduler loop
- cleanup: Clean orphaned MinIO/work dir artifacts
- install: Install a job package (.ljc file)
- status: Show coordinator status and job trees
- monitor: Live state monitoring
- test-job: Publish test job for debugging
- timeline: Show execution timeline from logs
"""

import asyncio
import logging
import shutil
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

import click
import yaml

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from coordinator.models import CoordinatorConfig
from coordinator.job_discovery import discover_jobs
from coordinator.tree_builder import build_trees
from coordinator.minio_manager import MinioManager
from coordinator.logging_utils import setup_logging
from coordinator_v2.coordinator import Coordinator, load_data_registry, _compare_versions

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> CoordinatorConfig:
    """Load coordinator config from YAML file."""
    with open(config_path) as f:
        data = yaml.safe_load(f)

    return CoordinatorConfig(**data.get("coordinator", data))


@click.group()
@click.option(
    "-c", "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to coordinator config file",
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    help="Enable debug logging",
)
@click.pass_context
def cli(ctx, config, verbose):
    """LinearJC Coordinator v2 - Async architecture with aiomqtt."""
    ctx.ensure_object(dict)

    # Setup logging
    level = "DEBUG" if verbose else "INFO"
    setup_logging(level=level)

    # Load config
    ctx.obj["config"] = load_config(config)
    ctx.obj["config_path"] = config


# =============================================================================
# Core Commands
# =============================================================================

@cli.command()
@click.pass_context
def run(ctx):
    """Run the coordinator scheduler loop (default mode)."""
    config = ctx.obj["config"]

    logging.info("Starting coordinator_v2...")
    logging.info(f"MQTT: {config.mqtt.broker}:{config.mqtt.port}")
    logging.info(f"MinIO: {config.minio.endpoint}")
    logging.info(f"Jobs: {config.jobs_dir}")

    coordinator = Coordinator(config)

    try:
        asyncio.run(coordinator.run())
    except KeyboardInterrupt:
        logging.info("Received interrupt, shutting down...")


@cli.command()
@click.pass_context
def version(ctx):
    """Show version information."""
    click.echo("LinearJC Coordinator v2.0.0 (async)")
    click.echo("Architecture: aiomqtt single-threaded async")


# =============================================================================
# Maintenance Commands
# =============================================================================

@cli.command()
@click.option(
    "--age-hours",
    default=24,
    type=int,
    help="Clean up executions older than this many hours",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="Only show what would be deleted without actually deleting",
)
@click.option(
    "--work-dir/--no-work-dir",
    default=True,
    help="Also clean up work directory (default: yes)",
)
@click.pass_context
def cleanup(ctx, age_hours: int, dry_run: bool, work_dir: bool):
    """Clean up orphaned execution artifacts from MinIO and work directory."""
    config = ctx.obj["config"]

    click.echo(f"Cleaning up orphaned executions (age > {age_hours}h, dry_run={dry_run})")
    click.echo("=" * 60)

    # Initialize MinIO manager
    minio = MinioManager(config.minio)

    # Clean MinIO
    minio_deleted = cleanup_minio(minio, config, age_hours, dry_run)
    click.echo(f"MinIO: {'Would delete' if dry_run else 'Deleted'} {minio_deleted} objects")

    # Clean work directory
    work_deleted = 0
    if work_dir:
        work_deleted = cleanup_work_dir(config, age_hours, dry_run)
        click.echo(f"Work dir: {'Would delete' if dry_run else 'Deleted'} {work_deleted} directories")

    click.echo("=" * 60)
    click.echo(f"Total: {minio_deleted + work_deleted} items {'would be' if dry_run else ''} cleaned")


def cleanup_minio(minio: MinioManager, config: CoordinatorConfig, age_hours: int, dry_run: bool) -> int:
    """Clean orphaned objects from MinIO temp bucket."""
    cutoff = datetime.now() - timedelta(hours=age_hours)
    deleted = 0

    try:
        objects = minio.client.list_objects(
            config.minio.temp_bucket,
            prefix="jobs/",
            recursive=True,
        )

        for obj in objects:
            if obj.last_modified and obj.last_modified.replace(tzinfo=None) < cutoff:
                if dry_run:
                    logger.info(f"Would delete: {obj.object_name}")
                else:
                    minio.client.remove_object(config.minio.temp_bucket, obj.object_name)
                    logger.info(f"Deleted: {obj.object_name}")
                deleted += 1

    except Exception as e:
        logger.error(f"MinIO cleanup error: {e}")

    return deleted


def cleanup_work_dir(config: CoordinatorConfig, age_hours: int, dry_run: bool) -> int:
    """Clean old execution directories from work directory."""
    cutoff = time.time() - (age_hours * 3600)
    deleted = 0

    work_path = Path(config.work_dir) if hasattr(config, "work_dir") else None
    if not work_path or not work_path.exists():
        return 0

    # Tree execution IDs follow pattern: {job_id}-{timestamp}-{uuid}
    for exec_dir in work_path.glob("*-*-*"):
        if exec_dir.is_dir():
            try:
                if exec_dir.stat().st_mtime < cutoff:
                    if dry_run:
                        logger.info(f"Would delete: {exec_dir}")
                    else:
                        shutil.rmtree(exec_dir)
                        logger.info(f"Deleted: {exec_dir}")
                    deleted += 1
            except Exception as e:
                logger.warning(f"Failed to process {exec_dir}: {e}")

    return deleted


# =============================================================================
# Package Management
# =============================================================================

@cli.command()
@click.argument("package", type=click.Path(exists=True))
@click.pass_context
def install(ctx, package: str):
    """Install a job package (.ljc file)."""
    config = ctx.obj["config"]
    package_path = Path(package)

    click.echo(f"Installing package: {package_path}")
    click.echo("=" * 60)

    try:
        result = install_package(config, package_path)
        click.echo(f"Installed: {result['job_id']} v{result['version']}")
        click.echo("")
        click.echo("Note: Send SIGHUP to running coordinator to reload jobs")

    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Installation failed: {e}", err=True)
        sys.exit(1)


def install_package(config: CoordinatorConfig, package_path: Path) -> dict:
    """
    Install a job package (.ljc file).

    Extracts, validates, checks version, and copies to jobs directory.

    Args:
        config: Coordinator configuration
        package_path: Path to .ljc package file

    Returns:
        dict with 'job_id' and 'version' fields

    Raises:
        ValueError: On validation or version conflicts
    """
    from coordinator.archive_handler import safe_extract_member

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir).resolve()

        # Extract package with security validation
        with tarfile.open(package_path, "r:gz") as tar:
            for member in tar.getmembers():
                safe_extract_member(tar, member, tmppath)

        # Validate structure
        job_yaml = tmppath / "job.yaml"
        script_sh = tmppath / "script.sh"

        if not job_yaml.exists():
            raise ValueError("Package missing job.yaml")

        if not script_sh.exists():
            raise ValueError("Package missing script.sh")

        # Parse job to get job_id and version
        with open(job_yaml) as f:
            job_data = yaml.safe_load(f)

        if "job" not in job_data or "id" not in job_data["job"]:
            raise ValueError("Invalid job.yaml structure")

        job_id = job_data["job"]["id"]
        version = job_data["job"].get("version", "unknown")

        # Check for existing job and handle version upgrade
        jobs_dir = Path(config.jobs_dir)
        dest_job = jobs_dir / f"{job_id}.yaml"

        if dest_job.exists():
            with open(dest_job) as f:
                existing_data = yaml.safe_load(f)
            existing_version = existing_data.get("job", {}).get("version", "0.0.0")

            comparison = _compare_versions(version, existing_version)

            if comparison > 0:
                logger.info(f"Upgrading job {job_id}: {existing_version} -> {version}")
            elif comparison == 0:
                raise ValueError(
                    f"Version {version} of {job_id} is already installed. "
                    f"Bump the version to deploy a new package."
                )
            else:
                raise ValueError(
                    f"Cannot downgrade {job_id}: {version} < {existing_version}. "
                    f"Only upgrades are allowed."
                )
        else:
            logger.info(f"Installing new job: {job_id} v{version}")

        # Cache original .ljc package
        packages_dir = Path(config.jobs_dir).parent / "packages"
        packages_dir.mkdir(exist_ok=True)
        dest_package = packages_dir / f"{job_id}.ljc"
        shutil.copy2(package_path, dest_package)
        logger.info(f"Cached package: {dest_package}")

        # Copy job.yaml to jobs directory
        shutil.copy2(job_yaml, dest_job)
        logger.info(f"Installed job definition: {dest_job}")

        return {
            "job_id": job_id,
            "version": version,
        }


# =============================================================================
# Status & Monitoring Commands
# =============================================================================

@cli.command()
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.pass_context
def status(ctx, output_json: bool):
    """Show coordinator status and current job trees."""
    config = ctx.obj["config"]
    config_path = ctx.obj["config_path"]

    # Load data registry and discover jobs
    data_registry = load_data_registry(config.data_registry)
    jobs = discover_jobs(config.jobs_dir)
    trees = build_trees(jobs)

    if output_json:
        import json
        output = {
            "config": config_path,
            "jobs_dir": config.jobs_dir,
            "registry_entries": len(data_registry),
            "trees": [
                {
                    "job_id": t.root.id,
                    "version": t.root.version,
                    "jobs_in_chain": len(t.jobs),
                    "min_daily": t.min_daily,
                    "max_daily": t.max_daily,
                }
                for t in trees
            ],
        }
        click.echo(json.dumps(output, indent=2))
    else:
        click.echo("=" * 60)
        click.echo("Coordinator Status (v2 - async)")
        click.echo("=" * 60)
        click.echo(f"Config: {config_path}")
        click.echo(f"Jobs directory: {config.jobs_dir}")
        click.echo(f"Data registry: {len(data_registry)} entries")
        click.echo(f"MQTT: {config.mqtt.broker}:{config.mqtt.port}")
        click.echo(f"MinIO: {config.minio.endpoint}")
        click.echo("")
        click.echo("Job Trees:")
        click.echo("-" * 60)

        if not trees:
            click.echo("  No job trees configured")
        else:
            for tree in trees:
                chain_info = f" ({len(tree.jobs)} jobs)" if len(tree.jobs) > 1 else ""
                click.echo(f"  {tree.root.id} v{tree.root.version}{chain_info}")
                click.echo(f"    Schedule: {tree.min_daily}-{tree.max_daily}x/day")
                if len(tree.jobs) > 1:
                    job_ids = " -> ".join(j.id for j in tree.jobs)
                    click.echo(f"    Chain: {job_ids}")


@cli.command()
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.pass_context
def monitor(ctx, output_json: bool):
    """Show live coordinator state and job execution status.

    Note: This reads from disk state. For live monitoring,
    connect to a running coordinator via ljc commands.
    """
    click.echo("Note: Static monitoring from disk. For live state, use 'ljc ps'.")
    click.echo("")
    ctx.invoke(status, output_json=output_json)


@cli.command("test-job")
@click.option("--job-id", default=None, help="Specific job to test (default: first available)")
@click.pass_context
def test_job(ctx, job_id: str | None):
    """Publish a test job to verify the system is working.

    This command requires a running coordinator. It publishes a test job
    request via MQTT and monitors for progress.
    """
    config = ctx.obj["config"]

    # Discover available jobs
    jobs = discover_jobs(config.jobs_dir)
    trees = build_trees(jobs)

    if not trees:
        click.echo("Error: No jobs configured", err=True)
        sys.exit(1)

    # Select job
    if job_id:
        target_tree = None
        for t in trees:
            if t.root.id == job_id:
                target_tree = t
                break
        if not target_tree:
            click.echo(f"Error: Job '{job_id}' not found", err=True)
            sys.exit(1)
    else:
        target_tree = trees[0]

    click.echo(f"Test job: {target_tree.root.id} v{target_tree.root.version}")
    click.echo("")
    click.echo("To execute this job, use:")
    click.echo(f"  ljc exec {target_tree.root.id} --follow")
    click.echo("")
    click.echo("Or directly via MQTT:")
    click.echo(f"  mosquitto_pub -t 'linearjc/dev/exec/request/linearjc-coordinator' \\")
    click.echo(f"    -m '{{\"action\": \"exec_job\", \"job_id\": \"{target_tree.root.id}\"}}'")


@cli.command()
@click.option("--hours", default=24, type=int, help="Show executions from last N hours")
@click.option("--job-id", default=None, help="Filter by job ID")
@click.pass_context
def timeline(ctx, hours: int, job_id: str | None):
    """Show job execution timeline from logs.

    Parses coordinator logs to show execution history.
    For live history, use 'ljc logs <job-id>'.
    """
    config = ctx.obj["config"]

    click.echo(f"Job Execution Timeline (last {hours}h)")
    click.echo("=" * 60)

    # Try to find and parse log file
    log_paths = [
        Path("/var/log/linearjc/coordinator.log"),
        Path("logs/coordinator.log"),
        Path("coordinator.log"),
    ]

    log_file = None
    for p in log_paths:
        if p.exists():
            log_file = p
            break

    if not log_file:
        click.echo("")
        click.echo("No log file found. For live timeline, use:")
        click.echo("  ljc logs <job-id>")
        click.echo("")
        click.echo("Or parse logs directly:")
        click.echo("  grep -E '(Dispatched tree|Completed tree|Chain failed)' /var/log/linearjc/coordinator.log")
        return

    # Parse log file for execution events
    click.echo(f"Reading: {log_file}")
    click.echo("-" * 60)

    cutoff = datetime.now() - timedelta(hours=hours)
    events = []

    try:
        with open(log_file) as f:
            for line in f:
                # Parse timestamp (format: 2026-01-12 10:30:45,123)
                if len(line) < 23:
                    continue

                try:
                    timestamp_str = line[:23].replace(",", ".")
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    continue

                if timestamp < cutoff:
                    continue

                # Look for execution events
                if "Dispatched tree" in line or "Completed tree" in line or "Chain failed" in line:
                    if job_id is None or job_id in line:
                        events.append((timestamp, line.strip()))

    except Exception as e:
        click.echo(f"Error reading log: {e}", err=True)
        return

    if not events:
        click.echo("No execution events found in the specified timeframe")
    else:
        for ts, event in events[-50:]:  # Last 50 events
            # Extract the relevant part of the log message
            parts = event.split("] ", 1)
            msg = parts[-1] if len(parts) > 1 else event
            click.echo(f"{ts.strftime('%Y-%m-%d %H:%M:%S')} | {msg[:80]}")


if __name__ == "__main__":
    cli()
