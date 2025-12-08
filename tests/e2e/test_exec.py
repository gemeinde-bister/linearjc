"""
E2E tests for ljc exec command.

Tests the immediate job execution workflow:
  ljc exec → coordinator DeveloperAPI → job runs → progress updates

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
  - Executor binary built (cargo build --release in src/executor)
"""
import os
import subprocess
import time  # Used for output polling loop
from pathlib import Path

import pytest


class TestLjcExec:
    """Test ljc exec command with local infrastructure."""

    def test_exec_unknown_job_rejected(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test that executing an unknown job is rejected.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "exec",
                "nonexistent.job",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc exec unknown job stdout ===")
        print(result.stdout)
        print("=== ljc exec unknown job stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because job doesn't exist
        assert result.returncode != 0
        assert "not found" in result.stdout.lower() or "rejected" in result.stdout.lower(), (
            f"Expected 'not found' or 'rejected' in output:\n{result.stdout}"
        )

    def test_exec_job_immediate(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        e2e_config_dir,
        coordinator_ready,
    ):
        """
        Test immediate job execution via ljc exec.

        Steps:
        1. Run ljc exec with existing echo.test job
        2. Verify job is accepted
        3. Verify output is created
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Run ljc exec without --follow (just trigger)
        result = subprocess.run(
            [
                str(ljc_binary),
                "exec",
                "echo.test",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        print("\n=== ljc exec stdout ===")
        print(result.stdout)
        print("=== ljc exec stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc exec failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Should show accepted
        assert "accepted" in result.stdout.lower(), (
            f"Expected 'accepted' in output:\n{result.stdout}"
        )

        # Wait for job to complete and check output
        data_dir = e2e_config_dir['data_dir']
        output_dir = data_dir / "echo_output"

        max_wait = 30
        start = time.time()

        while time.time() - start < max_wait:
            # echo.test script creates output.txt not result.txt
            result_file = output_dir / "output.txt"
            if result_file.exists():
                content = result_file.read_text()
                print(f"\nJob output:\n{content}")
                assert "processed" in content.lower() or "hello" in content.lower(), (
                    f"Unexpected output content:\n{content}"
                )
                return

            time.sleep(2)

        # Print logs for debugging if failed
        _print_logs(e2e_config_dir)

        pytest.fail(
            f"Job did not produce output within {max_wait}s\n"
            f"Expected output at: {output_dir}/output.txt"
        )

    def test_exec_with_follow(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        e2e_config_dir,
        coordinator_ready,
    ):
        """
        Test ljc exec --follow streams progress.

        Steps:
        1. Run ljc exec --follow
        2. Verify progress states are shown
        3. Verify final completion
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Run ljc exec with --follow
        result = subprocess.run(
            [
                str(ljc_binary),
                "exec",
                "echo.test",
                "--follow",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,  # Longer timeout for follow mode
        )

        print("\n=== ljc exec --follow stdout ===")
        print(result.stdout)
        print("=== ljc exec --follow stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc exec --follow failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Should show progress states
        output_lower = result.stdout.lower()
        # At minimum should show completion
        assert "completed" in output_lower or "success" in output_lower, (
            f"Expected completion message in output:\n{result.stdout}"
        )

    def test_exec_with_wait_exit_code(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        e2e_config_dir,
        coordinator_ready,
    ):
        """
        Test ljc exec --wait returns job's exit code.

        Steps:
        1. Run ljc exec --wait with successful job
        2. Verify exit code is 0
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Run ljc exec with --wait
        result = subprocess.run(
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

        print("\n=== ljc exec --wait stdout ===")
        print(result.stdout)
        print("=== ljc exec --wait stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed with exit code 0
        assert result.returncode == 0, (
            f"ljc exec --wait returned non-zero: {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )


class TestLjcExecErrors:
    """Test ljc exec error handling."""

    def test_exec_without_secret(self, ljc_binary, ljc_env):
        """Test exec without LINEARJC_SECRET fails."""
        env = os.environ.copy()
        env.update(ljc_env)
        del env['LINEARJC_SECRET']  # Remove the secret

        result = subprocess.run(
            [str(ljc_binary), "exec", "any.job"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0
        assert "LINEARJC_SECRET" in result.stderr or "LINEARJC_SECRET" in result.stdout

    def test_exec_without_broker(self, ljc_binary, ljc_env):
        """Test exec with invalid broker fails gracefully."""
        env = os.environ.copy()
        env.update(ljc_env)
        env['MQTT_BROKER'] = 'nonexistent.invalid.host'
        env['MQTT_PORT'] = '9999'

        result = subprocess.run(
            [str(ljc_binary), "exec", "any.job"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode != 0
        # Should fail to connect
        output = result.stdout + result.stderr
        assert ("connect" in output.lower() or
                "failed" in output.lower() or
                "error" in output.lower())


def _print_logs(e2e_config_dir):
    """Print logs for debugging test failures."""
    coordinator_log = e2e_config_dir['work_dir'] / 'coordinator.log'
    if coordinator_log.exists():
        print(f"\n=== Coordinator log (last 3000 chars) ===")
        print(coordinator_log.read_text()[-3000:])

    executor_log = e2e_config_dir['work_dir'] / 'executor.log'
    if executor_log.exists():
        print(f"\n=== Executor log (last 3000 chars) ===")
        print(executor_log.read_text()[-3000:])
