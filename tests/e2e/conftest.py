"""
E2E test fixtures for LinearJC.

Provides fixtures for running coordinator and executor locally with
spawned MQTT and MinIO servers.

Performance optimizations:
- Session-scoped fixtures share services across ALL test modules
- Readiness polling instead of fixed sleeps
- Use wait_for_coordinator() instead of time.sleep(2) in tests
"""
import os
import sys
import time
import shutil
import signal
import tempfile
import subprocess
from pathlib import Path
from typing import Optional

import pytest
import yaml

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


def is_root() -> bool:
    """Check if running as root."""
    return os.geteuid() == 0


def wait_for_coordinator(ljc_binary, ljc_env, timeout: float = 5.0) -> bool:
    """
    Poll until coordinator responds to a simple query.

    Use this instead of time.sleep(2) in tests for faster execution.
    Returns True if coordinator is ready, False on timeout.
    """
    import subprocess as sp

    env = os.environ.copy()
    env.update(ljc_env)

    start = time.time()
    while time.time() - start < timeout:
        # Try a simple status --all query - fast and tests coordinator readiness
        result = sp.run(
            [str(ljc_binary), "status", "--all"],
            env=env,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0:
            return True
        time.sleep(0.2)  # 200ms polling interval
    return False


# Track if coordinator has been verified ready (session-level flag)
_coordinator_verified_ready = False


@pytest.fixture
def coordinator_ready(ljc_binary, ljc_env, coordinator_process):
    """
    Ensure coordinator is ready before test runs.

    Use this fixture instead of time.sleep(2) at the start of tests.
    Only performs actual check once per session, then returns immediately.
    """
    global _coordinator_verified_ready
    if not _coordinator_verified_ready:
        if wait_for_coordinator(ljc_binary, ljc_env, timeout=10.0):
            _coordinator_verified_ready = True
        else:
            pytest.fail("Coordinator did not become ready within timeout")
    return True


@pytest.fixture(scope="session")
def e2e_temp_dir():
    """Create temporary directory for E2E test artifacts."""
    temp_dir = tempfile.mkdtemp(prefix="linearjc_e2e_")
    yield Path(temp_dir)
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def e2e_config_dir(e2e_temp_dir, mosquitto_server, minio_server):
    """
    Create coordinator config directory with all necessary files.

    Returns path to config directory containing:
    - config.yaml (pointing to local MQTT/MinIO)
    - data_registry.yaml
    - jobs/ directory with test job
    """
    config_dir = e2e_temp_dir / "config"
    config_dir.mkdir()

    jobs_dir = config_dir / "jobs"
    jobs_dir.mkdir()

    packages_dir = config_dir / "packages"
    packages_dir.mkdir()

    work_dir = e2e_temp_dir / "work"
    work_dir.mkdir()

    data_dir = e2e_temp_dir / "data"
    data_dir.mkdir()

    # Create coordinator config
    config = {
        'coordinator': {
            'mqtt': {
                'broker': mosquitto_server['host'],
                'port': mosquitto_server['port'],
                'keepalive': 60,
            },
            'minio': {
                'endpoint': minio_server['endpoint'],
                'access_key': minio_server['access_key'],
                'secret_key': minio_server['secret_key'],
                'secure': False,
                'temp_bucket': 'linearjc-test',
            },
            'jobs_dir': str(jobs_dir),
            'data_registry': str(config_dir / 'data_registry.yaml'),
            'work_dir': str(work_dir),
            'scheduling': {
                'loop_interval': 2,  # Fast for testing
                'message_max_age': 60,
            },
            'signing': {
                'shared_secret': 'test-secret-for-local-e2e-testing-min-32-chars',
            },
            'security': {
                'allowed_data_roots': [str(data_dir), '/tmp'],
                'validate_secrets': False,  # Disable for testing
            },
            'logging': {
                'level': 'DEBUG',
                'json_format': False,
            },
            'archive': {
                'format': 'tar.gz',
            },
        }
    }

    with open(config_dir / 'config.yaml', 'w') as f:
        yaml.dump(config, f)

    # Create data registry with all test entries
    # Note: Per SPEC.md, registry is pushed separately from packages.
    # The coordinator needs registry entries BEFORE job deployment.
    registry = {
        'registry': {
            # Echo test entries
            'echo_input': {
                'type': 'fs',
                'path': str(data_dir / 'echo_input'),
                'kind': 'dir',
            },
            'echo_output': {
                'type': 'fs',
                'path': str(data_dir / 'echo_output'),
                'kind': 'dir',
            },
            # Deploy test entries (pre-registered per SPEC.md design)
            'deploy_input': {
                'type': 'fs',
                'path': str(data_dir / 'deploy_input'),
                'kind': 'dir',
            },
            'deploy_output': {
                'type': 'fs',
                'path': str(data_dir / 'deploy_output'),
                'kind': 'dir',
            },
        }
    }

    with open(config_dir / 'data_registry.yaml', 'w') as f:
        yaml.dump(registry, f)

    # Copy echo test job to jobs directory with current user
    test_job_src = Path(__file__).parent / 'jobs' / 'echo-test'

    # Load job.yaml and update user to current user (for non-root E2E testing)
    import getpass
    current_user = getpass.getuser()

    with open(test_job_src / 'job.yaml') as f:
        job_config = yaml.safe_load(f)
    job_config['job']['run']['user'] = current_user

    # Write updated job.yaml for coordinator
    with open(jobs_dir / 'echo.test.yaml', 'w') as f:
        yaml.dump(job_config, f)

    # Copy script to packages (for executor to find)
    shutil.copytree(test_job_src, packages_dir / 'echo.test')

    # Update job.yaml in packages dir with current user too
    with open(packages_dir / 'echo.test' / 'job.yaml', 'w') as f:
        yaml.dump(job_config, f)

    # Create input data
    input_dir = data_dir / 'echo_input'
    input_dir.mkdir()
    (input_dir / 'input.txt').write_text('Hello from E2E test!\nThis is test input data.\n')

    yield {
        'config_dir': config_dir,
        'config_file': config_dir / 'config.yaml',
        'jobs_dir': jobs_dir,
        'packages_dir': packages_dir,
        'work_dir': work_dir,
        'data_dir': data_dir,
        'shared_secret': config['coordinator']['signing']['shared_secret'],
    }


@pytest.fixture(scope="session")
def coordinator_process(e2e_config_dir, mosquitto_server, minio_server, executor_process):
    """
    Start coordinator subprocess.

    Yields the process handle. Automatically terminates on cleanup.

    Note: Depends on executor_process to ensure executor is running before
    coordinator starts querying for capabilities.
    """
    # Wait for executor to be fully ready before starting coordinator
    # This ensures executor can respond to capability queries
    time.sleep(1)  # Reduced from 2s - executor should be ready quickly

    # Find coordinator entry point
    coordinator_main = Path(__file__).parent.parent.parent / "src" / "coordinator" / "main.py"

    if not coordinator_main.exists():
        pytest.skip(f"Coordinator not found: {coordinator_main}")

    config_file = e2e_config_dir['config_file']

    # Log file for debugging
    log_file = e2e_config_dir['work_dir'] / 'coordinator.log'

    # Start coordinator
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(__file__).parent.parent.parent / "src")

    with open(log_file, 'w') as log_f:
        proc = subprocess.Popen(
            [sys.executable, str(coordinator_main), '-c', str(config_file), 'run'],
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
            # Process exited
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
        'process': proc,
        'config': e2e_config_dir,
        'log_file': log_file,
    }

    # Cleanup - print log on failure for debugging
    print(f"\n=== Coordinator Log ({log_file}) ===")
    if log_file.exists():
        print(log_file.read_text()[-2000:])  # Last 2000 chars
    print("=== End Coordinator Log ===\n")

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="session")
def executor_process(e2e_config_dir, mosquitto_server, minio_server):
    """
    Start executor subprocess.

    Yields the process handle. Automatically terminates on cleanup.
    """
    import tarfile

    # Find executor binary
    executor_bin = Path(__file__).parent.parent.parent / "src" / "executor" / "target" / "release" / "linearjc-executor"

    if not executor_bin.exists():
        # Try debug build
        executor_bin = Path(__file__).parent.parent.parent / "src" / "executor" / "target" / "debug" / "linearjc-executor"

    if not executor_bin.exists():
        pytest.skip(f"Executor binary not found. Build with: cd src/executor && cargo build --release")

    # Create executor jobs directory
    executor_jobs_dir = e2e_config_dir['work_dir'] / 'executor_jobs'
    executor_jobs_dir.mkdir(exist_ok=True)

    # SPEC.md v0.5.0: Create .ljc package (tar.gz containing job.yaml + script.sh)
    package_path = executor_jobs_dir / 'echo.test.ljc'
    job_src_dir = e2e_config_dir['packages_dir'] / 'echo.test'

    with tarfile.open(package_path, 'w:gz') as tar:
        # Add job.yaml
        tar.add(job_src_dir / 'job.yaml', arcname='job.yaml')
        # Add script.sh
        tar.add(job_src_dir / 'script.sh', arcname='script.sh')

    # Log file for debugging
    log_file = e2e_config_dir['work_dir'] / 'executor.log'

    # Environment for executor
    env = os.environ.copy()
    env['MQTT_BROKER'] = mosquitto_server['host']
    env['MQTT_PORT'] = str(mosquitto_server['port'])
    env['MQTT_SHARED_SECRET'] = e2e_config_dir['shared_secret']
    env['EXECUTOR_ID'] = 'e2e-test-executor'
    env['JOBS_DIR'] = str(executor_jobs_dir)
    env['WORK_DIR'] = str(e2e_config_dir['work_dir'] / 'executor_work')
    env['RUST_LOG'] = 'debug'
    # Enable pool capability for on-demand job distribution (Phase 3 Airdrop)
    env['CAPABILITIES'] = 'pool'
    # MinIO endpoint for downloading packages (on-demand distribution)
    env['MINIO_ENDPOINT'] = minio_server['endpoint']
    env['MINIO_SECURE'] = 'false'

    # Create executor work directory
    (e2e_config_dir['work_dir'] / 'executor_work').mkdir(exist_ok=True)

    with open(log_file, 'w') as log_f:
        proc = subprocess.Popen(
            [str(executor_bin)],
            stdout=log_f,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
        )

    # Wait for executor to connect (reduced from 2s - MQTT connect is fast)
    time.sleep(0.5)

    if proc.poll() is not None:
        log_content = log_file.read_text() if log_file.exists() else "No log"
        pytest.fail(f"Executor exited early:\n{log_content}")

    yield {
        'process': proc,
        'jobs_dir': executor_jobs_dir,
        'work_dir': e2e_config_dir['work_dir'] / 'executor_work',
        'log_file': log_file,
    }

    # Cleanup - print log for debugging
    print(f"\n=== Executor Log ({log_file}) ===")
    if log_file.exists():
        print(log_file.read_text()[-2000:])  # Last 2000 chars
    print("=== End Executor Log ===\n")

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture
def mqtt_client(mosquitto_server):
    """Create MQTT client for test assertions."""
    import paho.mqtt.client as mqtt

    client = mqtt.Client()
    client.connect(mosquitto_server['host'], mosquitto_server['port'])
    client.loop_start()

    yield client

    client.loop_stop()
    client.disconnect()


@pytest.fixture(scope="session")
def ljc_binary():
    """
    Find the ljc binary for deployment testing.

    Returns path to ljc binary, or skips test if not found.
    """
    ljc_base = Path(__file__).parent.parent.parent / "tools" / "ljc" / "target"

    # Try release build first
    ljc_path = ljc_base / "release" / "ljc"
    if ljc_path.exists():
        return ljc_path

    # Try debug build
    ljc_path = ljc_base / "debug" / "ljc"
    if ljc_path.exists():
        return ljc_path

    pytest.skip(
        "ljc binary not found. Build with: cd tools/ljc && cargo build --release"
    )


@pytest.fixture(scope="session")
def ljc_env(mosquitto_server, e2e_config_dir):
    """
    Environment variables for ljc deploy command.

    Provides all config via env vars (no .ljcconfig needed).
    """
    return {
        'LINEARJC_SECRET': e2e_config_dir['shared_secret'],
        'MQTT_BROKER': mosquitto_server['host'],
        'MQTT_PORT': str(mosquitto_server['port']),
        'COORDINATOR_ID': 'linearjc-coordinator',
    }


@pytest.fixture(scope="session")
def deploy_test_package(e2e_temp_dir, e2e_config_dir):
    """
    Create a test .ljc package for deployment testing.

    Creates package directly in Python (no ljc build needed).
    Returns path to the .ljc file.
    """
    import tarfile
    import getpass

    current_user = getpass.getuser()
    data_dir = e2e_config_dir['data_dir']

    # Create job directory
    job_dir = e2e_temp_dir / "deploy-test-job"
    job_dir.mkdir(exist_ok=True)

    # job.yaml
    job_yaml = f"""job:
  id: deploy.test
  version: "1.0.0"
  reads: [deploy_input]
  writes: [deploy_output]
  depends: []
  schedule:
    min_daily: 1
    max_daily: 100
  run:
    user: {current_user}
    timeout: 30
    isolation: none
    network: true
"""
    (job_dir / "job.yaml").write_text(job_yaml)

    # script.sh
    script_sh = """#!/bin/sh
set -e
echo "=== Deploy Test Job ==="
echo "LJC_IN: ${LJC_IN:-not set}"
echo "LJC_OUT: ${LJC_OUT:-not set}"

IN_DIR="${LJC_IN:-$LINEARJC_IN_DIR}"
OUT_DIR="${LJC_OUT:-$LINEARJC_OUT_DIR}"

# Read input
INPUT_FILE="${IN_DIR}/deploy_input/input.txt"
cat "$INPUT_FILE"

# Write output
OUTPUT_DIR="${OUT_DIR}/deploy_output"
mkdir -p "$OUTPUT_DIR"
echo "Deployed and executed successfully" > "$OUTPUT_DIR/result.txt"
date -u +%Y-%m-%dT%H:%M:%SZ >> "$OUTPUT_DIR/result.txt"

echo "=== Deploy Test Complete ==="
"""
    script_path = job_dir / "script.sh"
    script_path.write_text(script_sh)
    script_path.chmod(0o755)

    # manifest.yaml
    # manifest.yaml - per SPEC.md, packages do NOT contain registry definitions
    manifest_yaml = """job_id: deploy.test
version: "1.0.0"
created_at: "2025-12-03T00:00:00Z"
files:
  - job.yaml
  - script.sh
"""
    (job_dir / "manifest.yaml").write_text(manifest_yaml)

    # Note: Registry entries are pre-registered on coordinator (see e2e_config_dir fixture)
    # per SPEC.md design. Packages do NOT contain registry definitions.

    # Create input data
    input_dir = data_dir / "deploy_input"
    input_dir.mkdir(exist_ok=True)
    (input_dir / "input.txt").write_text("Hello from deploy test!\n")

    # Build .ljc package (tar.gz) - per SPEC.md: job.yaml, script.sh, manifest.yaml, bin/, data/
    dist_dir = e2e_temp_dir / "dist"
    dist_dir.mkdir(exist_ok=True)
    package_path = dist_dir / "deploy.test.ljc"

    with tarfile.open(package_path, "w:gz") as tar:
        tar.add(job_dir / "job.yaml", arcname="job.yaml")
        tar.add(job_dir / "script.sh", arcname="script.sh")
        tar.add(job_dir / "manifest.yaml", arcname="manifest.yaml")

    return package_path
