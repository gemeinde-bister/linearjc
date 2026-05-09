"""
Integration tests for coordinator_v2.

Tests the async coordinator against the real Rust executor.
"""

import getpass
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path

import pytest
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


@pytest.fixture(scope="module")
def coordinator_v2_config_dir(mosquitto_server, minio_server):
    """
    Create config directory for coordinator_v2 testing.

    Session-scoped to share across tests in this module.
    """
    temp_dir = tempfile.mkdtemp(prefix="coordinator_v2_test_")
    temp_path = Path(temp_dir)

    config_dir = temp_path / "config"
    config_dir.mkdir()

    jobs_dir = config_dir / "jobs"
    jobs_dir.mkdir()

    packages_dir = config_dir / "packages"
    packages_dir.mkdir()

    work_dir = temp_path / "work"
    work_dir.mkdir()

    data_dir = temp_path / "data"
    data_dir.mkdir()

    current_user = getpass.getuser()

    # Create coordinator config
    config = {
        "coordinator": {
            "mqtt": {
                "broker": mosquitto_server["host"],
                "port": mosquitto_server["port"],
                "keepalive": 60,
            },
            "minio": {
                "endpoint": minio_server["endpoint"],
                "access_key": minio_server["access_key"],
                "secret_key": minio_server["secret_key"],
                "secure": False,
                "temp_bucket": "linearjc-test",
            },
            "jobs_dir": str(jobs_dir),
            "data_registry": str(config_dir / "data_registry.yaml"),
            "work_dir": str(work_dir),
            "scheduling": {
                "loop_interval": 1,  # Fast for testing
                "message_max_age": 60,
            },
            "signing": {
                "shared_secret": "test-secret-for-coordinator-v2-integration-testing",
            },
            "security": {
                "allowed_data_roots": [str(data_dir), "/tmp"],
                "validate_secrets": False,
            },
            "logging": {
                "level": "DEBUG",
                "json_format": False,
            },
            "archive": {
                "format": "tar.gz",
            },
        }
    }

    with open(config_dir / "config.yaml", "w") as f:
        yaml.dump(config, f)

    # Create data registry
    registry = {
        "registry": {
            "test_input": {
                "type": "fs",
                "path": str(data_dir / "test_input"),
                "kind": "dir",
            },
            "test_output": {
                "type": "fs",
                "path": str(data_dir / "test_output"),
                "kind": "dir",
            },
        }
    }

    with open(config_dir / "data_registry.yaml", "w") as f:
        yaml.dump(registry, f)

    # Create test job
    job_config = {
        "job": {
            "id": "integration.test",
            "version": "1.0.0",
            "reads": ["test_input"],
            "writes": ["test_output"],
            "depends": [],
            "schedule": {
                "min_daily": 1,
                "max_daily": 100,
            },
            "run": {
                "user": current_user,
                "timeout": 30,
                "isolation": "none",
                "network": True,
            },
        }
    }

    # Write job to jobs directory
    with open(jobs_dir / "integration.test.yaml", "w") as f:
        yaml.dump(job_config, f)

    # Create script
    script_content = """#!/bin/sh
set -e
echo "=== Integration Test Job ==="
echo "Working"
exit 0
"""

    script_path = packages_dir / "integration.test"
    script_path.mkdir()
    (script_path / "job.yaml").write_text(yaml.dump(job_config))
    (script_path / "script.sh").write_text(script_content)
    os.chmod(script_path / "script.sh", 0o755)

    # Create .ljc package
    package_path = packages_dir / "integration.test.ljc"
    with tarfile.open(package_path, "w:gz") as tar:
        tar.add(script_path / "job.yaml", arcname="job.yaml")
        tar.add(script_path / "script.sh", arcname="script.sh")

    # Create input data
    input_dir = data_dir / "test_input"
    input_dir.mkdir()
    (input_dir / "input.txt").write_text("Test input data\n")

    yield {
        "temp_dir": temp_path,
        "config_dir": config_dir,
        "config_file": config_dir / "config.yaml",
        "jobs_dir": jobs_dir,
        "packages_dir": packages_dir,
        "work_dir": work_dir,
        "data_dir": data_dir,
        "shared_secret": config["coordinator"]["signing"]["shared_secret"],
    }

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="module")
def executor_for_v2(coordinator_v2_config_dir, mosquitto_server, minio_server):
    """
    Start executor for coordinator_v2 testing.

    The executor sends heartbeats, which coordinator_v2 uses for discovery.
    """
    # Find executor binary
    executor_bin = (
        Path(__file__).parent.parent.parent
        / "src"
        / "executor"
        / "target"
        / "release"
        / "linearjc-executor"
    )

    if not executor_bin.exists():
        executor_bin = (
            Path(__file__).parent.parent.parent
            / "src"
            / "executor"
            / "target"
            / "debug"
            / "linearjc-executor"
        )

    if not executor_bin.exists():
        pytest.skip("Executor binary not found. Build with: cd src/executor && cargo build --release")

    # Create executor jobs directory
    executor_jobs_dir = coordinator_v2_config_dir["work_dir"] / "executor_jobs"
    executor_jobs_dir.mkdir(exist_ok=True)

    # Copy .ljc package for executor
    package_src = coordinator_v2_config_dir["packages_dir"] / "integration.test.ljc"
    package_dst = executor_jobs_dir / "integration.test.ljc"
    shutil.copy(package_src, package_dst)

    # Create executor work directory
    executor_work_dir = coordinator_v2_config_dir["work_dir"] / "executor_work"
    executor_work_dir.mkdir(exist_ok=True)

    # Log file
    log_file = coordinator_v2_config_dir["work_dir"] / "executor.log"

    # Environment
    env = os.environ.copy()
    env["MQTT_BROKER"] = mosquitto_server["host"]
    env["MQTT_PORT"] = str(mosquitto_server["port"])
    env["MQTT_SHARED_SECRET"] = coordinator_v2_config_dir["shared_secret"]
    env["EXECUTOR_ID"] = "coordinator-v2-test-executor"
    env["JOBS_DIR"] = str(executor_jobs_dir)
    env["WORK_DIR"] = str(executor_work_dir)
    env["RUST_LOG"] = "debug"
    env["CAPABILITIES"] = "pool"
    env["MINIO_ENDPOINT"] = minio_server["endpoint"]
    env["MINIO_SECURE"] = "false"

    with open(log_file, "w") as log_f:
        proc = subprocess.Popen(
            [str(executor_bin)],
            stdout=log_f,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
        )

    # Wait for executor to connect
    time.sleep(1)

    if proc.poll() is not None:
        log_content = log_file.read_text() if log_file.exists() else "No log"
        pytest.fail(f"Executor exited early:\n{log_content}")

    yield {
        "process": proc,
        "jobs_dir": executor_jobs_dir,
        "work_dir": executor_work_dir,
        "log_file": log_file,
    }

    # Cleanup
    print(f"\n=== Executor Log ({log_file}) ===")
    if log_file.exists():
        print(log_file.read_text()[-2000:])
    print("=== End Executor Log ===\n")

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="module")
def coordinator_v2_process(coordinator_v2_config_dir, executor_for_v2, mosquitto_server, minio_server):
    """
    Start coordinator_v2 subprocess.

    Depends on executor_for_v2 to ensure executor is sending heartbeats.
    """
    # Wait for executor heartbeats to start flowing
    time.sleep(2)

    # Find coordinator_v2 entry point
    coordinator_main = (
        Path(__file__).parent.parent.parent
        / "src"
        / "coordinator_v2"
        / "main.py"
    )

    if not coordinator_main.exists():
        pytest.fail(f"coordinator_v2 not found: {coordinator_main}")

    config_file = coordinator_v2_config_dir["config_file"]
    log_file = coordinator_v2_config_dir["work_dir"] / "coordinator_v2.log"

    # Environment
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parent.parent.parent / "src")

    with open(log_file, "w") as log_f:
        proc = subprocess.Popen(
            [sys.executable, str(coordinator_main), "-c", str(config_file), "run"],
            stdout=log_f,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
        )

    # Wait for coordinator to initialize
    start = time.time()
    initialized = False

    while time.time() - start < 10:
        if proc.poll() is not None:
            log_content = log_file.read_text() if log_file.exists() else "No log"
            pytest.fail(f"Coordinator exited early:\n{log_content}")

        time.sleep(0.5)
        if time.time() - start > 3:
            initialized = True
            break

    if not initialized:
        proc.kill()
        log_content = log_file.read_text() if log_file.exists() else "No log"
        pytest.fail(f"Coordinator failed to initialize:\n{log_content}")

    yield {
        "process": proc,
        "config": coordinator_v2_config_dir,
        "log_file": log_file,
    }

    # Cleanup
    print(f"\n=== Coordinator v2 Log ({log_file}) ===")
    if log_file.exists():
        print(log_file.read_text()[-2000:])
    print("=== End Coordinator v2 Log ===\n")

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


class TestCoordinatorV2Integration:
    """Integration tests for coordinator_v2."""

    def test_coordinator_starts(self, coordinator_v2_process):
        """Verify coordinator_v2 starts without errors."""
        proc = coordinator_v2_process["process"]
        log_file = coordinator_v2_process["log_file"]

        # Give it a moment to stabilize
        time.sleep(1)

        # Check process is still running
        assert proc.poll() is None, "Coordinator exited unexpectedly"

        # Check log for initialization
        log_content = log_file.read_text()
        assert "Coordinator initialized" in log_content or "Initializing coordinator" in log_content

    def test_executor_heartbeat_received(self, coordinator_v2_process, executor_for_v2):
        """Verify coordinator receives executor heartbeats."""
        log_file = coordinator_v2_process["log_file"]

        # Wait for heartbeat to arrive
        time.sleep(3)

        log_content = log_file.read_text()

        # Look for heartbeat handling in logs
        # The executor sends heartbeats, coordinator should log them
        assert (
            "heartbeat" in log_content.lower()
            or "executor" in log_content.lower()
        ), f"No heartbeat activity in log:\n{log_content[-1000:]}"

    def test_mqtt_connection(self, coordinator_v2_process):
        """Verify coordinator connects to MQTT broker."""
        log_file = coordinator_v2_process["log_file"]

        log_content = log_file.read_text()

        assert "MQTT" in log_content or "Connected" in log_content


class TestCoordinatorV2Components:
    """Test individual coordinator_v2 components."""

    def test_event_router_import(self):
        """Verify EventRouter can be imported."""
        from coordinator_v2.event_router import EventRouter

        router = EventRouter()
        assert len(router) == 0

    def test_executor_registry_import(self):
        """Verify ExecutorRegistry can be imported."""
        from coordinator_v2.executor_registry import ExecutorRegistry

        registry = ExecutorRegistry(ttl=90.0)
        assert len(registry.get_pool_executors()) == 0

    def test_job_state_machine_import(self):
        """Verify JobStateMachine can be imported."""
        from coordinator_v2.job_state_machine import (
            JobExecution,
            JobState,
            JobStateMachine,
        )

        job_exec = JobExecution(
            job_execution_id="test-1",
            tree_execution_id="tree-1",
            job_id="test.job",
            version="1.0.0",
            state=JobState.QUEUED,
        )
        sm = JobStateMachine(job_exec)
        assert not sm.is_terminal

    def test_coordinator_import(self):
        """Verify Coordinator can be imported."""
        from coordinator_v2.coordinator import Coordinator

        # Just verify import works - we can't instantiate without full config
        assert Coordinator is not None
