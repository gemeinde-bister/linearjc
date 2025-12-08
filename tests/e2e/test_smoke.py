"""
E2E Smoke Test for LinearJC.

Tests the complete flow: coordinator -> MQTT -> executor -> job execution -> output

Run with:
    # As regular user (skips test)
    pytest tests/e2e/test_smoke.py -v

    # As root (runs full test)
    sudo pytest tests/e2e/test_smoke.py -v -s

    # Force run without root (may fail on permission issues)
    LINEARJC_E2E_FORCE=1 pytest tests/e2e/test_smoke.py -v -s
"""
import os
import sys
import time
import json
import threading
from pathlib import Path

import pytest

# Check if we should run E2E tests
def should_run_e2e():
    """Determine if E2E tests should run."""
    # Force run via environment variable
    if os.environ.get('LINEARJC_E2E_FORCE'):
        return True
    # Run if root
    if os.geteuid() == 0:
        return True
    return False


# Skip marker for non-root users
requires_root = pytest.mark.skipif(
    not should_run_e2e(),
    reason="E2E tests require root. Run with: sudo pytest tests/e2e/ -v"
)


class TestSmoke:
    """Basic smoke tests for LinearJC E2E flow."""

    @requires_root
    def test_coordinator_starts(self, coordinator_process):
        """Coordinator process starts successfully."""
        proc = coordinator_process['process']
        assert proc.poll() is None, "Coordinator should be running"

    @requires_root
    def test_executor_starts(self, executor_process):
        """Executor process starts successfully."""
        proc = executor_process['process']
        assert proc.poll() is None, "Executor should be running"

    @requires_root
    def test_job_execution_flow(self, coordinator_process, executor_process, mqtt_client, e2e_config_dir):
        """
        Test complete job execution flow.

        1. Coordinator schedules echo.test job
        2. Executor receives and runs job
        3. Output is written to filesystem
        """
        # Give coordinator time to schedule the job (loop_interval=2s)
        # The echo.test job has min_daily=1, so it should run on first check
        max_wait = 30  # seconds
        poll_interval = 2

        output_dir = e2e_config_dir['data_dir'] / 'echo_output'
        output_file = output_dir / 'output.txt'

        start = time.time()
        job_completed = False

        print(f"\nWaiting for job to complete (max {max_wait}s)...")
        print(f"  Input: {e2e_config_dir['data_dir'] / 'echo_input' / 'input.txt'}")
        print(f"  Expected output: {output_file}")

        while time.time() - start < max_wait:
            # Check if output exists
            if output_file.exists():
                content = output_file.read_text()
                if 'Processed by echo.test' in content:
                    job_completed = True
                    print(f"\n  Job completed after {time.time() - start:.1f}s")
                    print(f"  Output content:\n{content}")
                    break

            # Check processes are still running
            if coordinator_process['process'].poll() is not None:
                stdout = coordinator_process['process'].stdout.read()
                pytest.fail(f"Coordinator crashed:\n{stdout}")

            if executor_process['process'].poll() is not None:
                stdout = executor_process['process'].stdout.read()
                pytest.fail(f"Executor crashed:\n{stdout}")

            time.sleep(poll_interval)
            print(f"  ... waiting ({time.time() - start:.0f}s)")

        if not job_completed:
            # Dump debug info
            print("\n=== Debug Info ===")
            print(f"Output dir exists: {output_dir.exists()}")
            if output_dir.exists():
                print(f"Output dir contents: {list(output_dir.iterdir())}")

            # Check coordinator work dir
            work_dir = e2e_config_dir['work_dir']
            print(f"Work dir contents: {list(work_dir.iterdir()) if work_dir.exists() else 'N/A'}")

            pytest.fail(f"Job did not complete within {max_wait}s")

        # Verify output content
        content = output_file.read_text()
        assert 'Hello from E2E test!' in content, "Input data should be in output"
        assert 'Processed by echo.test' in content, "Processing marker should be in output"

    @requires_root
    def test_output_integrity(self, coordinator_process, executor_process, e2e_config_dir):
        """Verify output file has expected structure after job runs."""
        # This test runs after test_job_execution_flow due to pytest ordering
        # Wait a bit more in case previous test just barely finished
        time.sleep(2)

        output_file = e2e_config_dir['data_dir'] / 'echo_output' / 'output.txt'

        if not output_file.exists():
            pytest.skip("Output file not created - job may not have run")

        content = output_file.read_text()
        lines = content.strip().split('\n')

        # Should have original input + blank line + processed marker
        assert len(lines) >= 3, f"Expected at least 3 lines, got {len(lines)}"
        assert 'Hello from E2E test!' in lines[0]
        assert 'Processed by echo.test at' in lines[-1]


class TestInfrastructure:
    """Test the test infrastructure itself."""

    def test_mosquitto_fixture(self, mosquitto_server):
        """Mosquitto server fixture works."""
        assert mosquitto_server['host'] == '127.0.0.1'
        assert mosquitto_server['port'] > 0

    def test_minio_fixture(self, minio_server):
        """MinIO server fixture works."""
        assert minio_server['endpoint']
        assert minio_server['access_key']

    def test_config_generation(self, e2e_config_dir):
        """Config directory is generated correctly."""
        assert e2e_config_dir['config_file'].exists()
        assert e2e_config_dir['jobs_dir'].exists()
        assert (e2e_config_dir['jobs_dir'] / 'echo.test.yaml').exists()

    def test_input_data_exists(self, e2e_config_dir):
        """Input data is created for test job."""
        input_file = e2e_config_dir['data_dir'] / 'echo_input' / 'input.txt'
        assert input_file.exists()
        content = input_file.read_text()
        assert 'Hello from E2E test!' in content


if __name__ == "__main__":
    # Allow running directly
    pytest.main([__file__, "-v", "-s"])
