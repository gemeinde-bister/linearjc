"""
Pytest fixtures for LinearJC integration tests.

Spawns local Mosquitto and MinIO servers on random ports for isolated testing.
"""
import os
import sys
import time
import socket
import shutil
import signal
import tempfile
import subprocess
from pathlib import Path
from contextlib import closing

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def find_free_port() -> int:
    """Find a free TCP port."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('127.0.0.1', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 10.0) -> bool:
    """Wait for a port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                s.connect(('127.0.0.1', port))
                return True
        except ConnectionRefusedError:
            time.sleep(0.1)
    return False


@pytest.fixture(scope="session")
def mosquitto_server():
    """
    Start a Mosquitto MQTT broker for testing.

    Yields:
        dict with 'host', 'port' keys
    """
    port = find_free_port()

    # Create minimal config
    config_dir = tempfile.mkdtemp(prefix="mosquitto_test_")
    config_file = os.path.join(config_dir, "mosquitto.conf")

    with open(config_file, 'w') as f:
        f.write(f"""
listener {port} 127.0.0.1
allow_anonymous true
""")

    # Start mosquitto
    proc = subprocess.Popen(
        ['mosquitto', '-c', config_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait for it to start
    if not wait_for_port(port, timeout=5.0):
        proc.kill()
        shutil.rmtree(config_dir, ignore_errors=True)
        pytest.skip("Could not start Mosquitto broker")

    yield {
        'host': '127.0.0.1',
        'port': port,
    }

    # Cleanup
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
    shutil.rmtree(config_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def minio_server():
    """
    Start a MinIO server for testing.

    Yields:
        dict with 'endpoint', 'access_key', 'secret_key' keys
    """
    # Find minio binary
    minio_bin = Path(__file__).parent.parent / "bin" / "minio"
    if not minio_bin.exists():
        pytest.skip("MinIO binary not found. Run: curl -sSL https://dl.min.io/server/minio/release/linux-amd64/minio -o bin/minio && chmod +x bin/minio")

    port = find_free_port()
    console_port = find_free_port()

    # Create temp data directory
    data_dir = tempfile.mkdtemp(prefix="minio_test_")

    # Credentials
    access_key = "testadmin"
    secret_key = "testadmin123"

    env = os.environ.copy()
    env['MINIO_ROOT_USER'] = access_key
    env['MINIO_ROOT_PASSWORD'] = secret_key

    # Start minio
    proc = subprocess.Popen(
        [
            str(minio_bin), 'server', data_dir,
            '--address', f'127.0.0.1:{port}',
            '--console-address', f'127.0.0.1:{console_port}',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env
    )

    # Wait for it to start
    if not wait_for_port(port, timeout=10.0):
        proc.kill()
        shutil.rmtree(data_dir, ignore_errors=True)
        pytest.skip("Could not start MinIO server")

    yield {
        'endpoint': f'127.0.0.1:{port}',
        'access_key': access_key,
        'secret_key': secret_key,
        'secure': False,
    }

    # Cleanup
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
    shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture
def temp_work_dir():
    """Create a temporary work directory for tests."""
    work_dir = tempfile.mkdtemp(prefix="linearjc_test_")
    yield work_dir
    shutil.rmtree(work_dir, ignore_errors=True)


@pytest.fixture
def sample_job_yaml() -> str:
    """Sample job YAML in SPEC.md v0.5.0 format."""
    return """
job:
  id: test.job
  version: "1.0.0"

  reads: [test_input]
  writes: [test_output]

  depends: []

  schedule:
    min_daily: 1
    max_daily: 10

  run:
    user: nobody
    timeout: 60
"""


@pytest.fixture
def sample_registry_yaml() -> str:
    """Sample registry YAML in SPEC.md v0.5.0 format."""
    return """
registry:
  test_input:  {type: fs, path: /tmp/test_input.txt, kind: file}
  test_output: {type: fs, path: /tmp/test_output, kind: dir}
"""
