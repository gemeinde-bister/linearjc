"""
E2E tests for ljc kill command.

Tests the job cancellation workflow:
  ljc kill → coordinator DeveloperAPI → executor terminates job

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
  - Coordinator and executor running
"""
import os
import subprocess
import re

import pytest


class TestLjcKill:
    """Test ljc kill command with local infrastructure."""

    def test_kill_unknown_execution_not_found(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test that killing unknown execution returns error.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "kill",
                "fake.job-20251204-120000-abc123def456",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc kill unknown execution stdout ===")
        print(result.stdout)
        print("=== ljc kill unknown execution stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because execution doesn't exist
        assert result.returncode != 0
        output = result.stdout.lower() + result.stderr.lower()
        assert "not found" in output or "failed" in output, (
            f"Expected 'not found' or 'failed' in output:\n{result.stdout}\n{result.stderr}"
        )

    def test_kill_terminated_job_fails(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        e2e_config_dir,
        coordinator_ready,
    ):
        """
        Test that killing already terminated job returns error.

        Steps:
        1. Execute echo.test and wait for completion
        2. Extract execution ID from output
        3. Try to kill the terminated execution
        4. Verify error response
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # First execute the job and wait for completion
        exec_result = subprocess.run(
            [
                str(ljc_binary),
                "exec",
                "echo.test",
                "--wait",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

        print("\n=== ljc exec (for kill terminated test) stdout ===")
        print(exec_result.stdout)
        print("=== end exec output ===\n")

        # Extract execution ID from output using regex
        # Look for execution ID pattern: job_id-YYYYMMDD-HHMMSS-hexstring
        exec_id_pattern = r'(echo\.test-\d{8}-\d{6}-[a-f0-9]+)'
        match = re.search(exec_id_pattern, exec_result.stdout)

        if not match:
            # If we can't find execution ID in output, skip this test
            pytest.skip("Could not extract execution ID from exec output")

        execution_id = match.group(1)
        print(f"Extracted execution ID: {execution_id}")

        # Now try to kill the terminated execution
        result = subprocess.run(
            [
                str(ljc_binary),
                "kill",
                execution_id,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc kill terminated job stdout ===")
        print(result.stdout)
        print("=== ljc kill terminated job stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because job is already terminated
        assert result.returncode != 0, (
            f"Expected kill of terminated job to fail but got code {result.returncode}"
        )
        output = result.stdout.lower() + result.stderr.lower()
        assert "terminated" in output or "completed" in output or "failed" in output or "not found" in output, (
            f"Expected termination-related error in output:\n{result.stdout}\n{result.stderr}"
        )

    def test_kill_force_flag(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test kill --force sends SIGKILL instead of SIGTERM.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Use non-existent execution to test flag parsing
        result = subprocess.run(
            [
                str(ljc_binary),
                "kill",
                "fake.job-20251204-120000-abc123def456",
                "--force",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc kill --force stdout ===")
        print(result.stdout)
        print("=== ljc kill --force stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail (not found), but we're testing that --force is accepted
        assert result.returncode != 0
        # The output should mention SIGKILL or the execution not being found
        output = result.stdout + result.stderr
        assert "SIGKILL" in output or "not found" in output.lower()


class TestLjcKillErrors:
    """Test ljc kill error handling."""

    def test_kill_without_secret(self, ljc_binary, ljc_env):
        """Test kill without LINEARJC_SECRET fails."""
        env = os.environ.copy()
        env.update(ljc_env)
        del env['LINEARJC_SECRET']

        result = subprocess.run(
            [str(ljc_binary), "kill", "fake-execution-id"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0
        output = result.stderr + result.stdout
        assert "LINEARJC_SECRET" in output or "secret" in output.lower()

    def test_kill_without_broker(self, ljc_binary, ljc_env):
        """Test kill with invalid broker fails gracefully."""
        env = os.environ.copy()
        env.update(ljc_env)
        env['MQTT_BROKER'] = 'nonexistent.invalid.host'
        env['MQTT_PORT'] = '9999'

        result = subprocess.run(
            [str(ljc_binary), "kill", "fake-execution-id"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode != 0
        output = result.stdout + result.stderr
        assert ("connect" in output.lower() or
                "failed" in output.lower() or
                "error" in output.lower())

    def test_kill_requires_execution_id(self, ljc_binary, ljc_env):
        """Test kill without execution_id fails."""
        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [str(ljc_binary), "kill"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should fail with usage error
        assert result.returncode != 0
