"""
E2E tests for ljc logs command.

Tests the execution history workflow:
  ljc logs → coordinator DeveloperAPI → returns job execution history

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
  - Coordinator running with registered jobs
"""
import os
import subprocess
import time  # Needed for test_logs_after_execution sleep
import json

import pytest


class TestLjcLogs:
    """Test ljc logs command with local infrastructure."""

    def test_logs_unknown_job_not_found(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test that logs for unknown job returns error.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "logs",
                "nonexistent.job",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc logs unknown job stdout ===")
        print(result.stdout)
        print("=== ljc logs unknown job stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because job doesn't exist
        assert result.returncode != 0
        output = result.stdout.lower() + result.stderr.lower()
        assert "not found" in output or "failed" in output, (
            f"Expected 'not found' or 'failed' in output:\n{result.stdout}\n{result.stderr}"
        )

    def test_logs_known_job_no_history(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test logs for known job with no execution history.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "logs",
                "echo.test",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc logs echo.test stdout ===")
        print(result.stdout)
        print("=== ljc logs echo.test stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed (may have no history yet)
        assert result.returncode == 0, (
            f"ljc logs failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Output should show job ID
        assert "echo.test" in result.stdout, (
            f"Expected 'echo.test' in output:\n{result.stdout}"
        )

    def test_logs_after_execution(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        e2e_config_dir,
        coordinator_ready,
    ):
        """
        Test logs show execution after running a job.

        Steps:
        1. Execute echo.test via ljc exec
        2. Wait for completion
        3. Query logs and verify execution is recorded
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # First execute the job
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

        print("\n=== ljc exec (for logs test) stdout ===")
        print(exec_result.stdout)
        print("=== end exec output ===\n")

        # Wait a moment for execution to be recorded
        time.sleep(2)

        # Now query logs
        result = subprocess.run(
            [
                str(ljc_binary),
                "logs",
                "echo.test",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc logs after exec stdout ===")
        print(result.stdout)
        print("=== ljc logs after exec stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc logs failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Output should show executions count
        output_lower = result.stdout.lower()
        assert "execution" in output_lower or "24h" in output_lower, (
            f"Expected execution info in output:\n{result.stdout}"
        )

    def test_logs_with_last_flag(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test logs --last N limits results.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "logs",
                "echo.test",
                "--last", "5",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc logs --last 5 stdout ===")
        print(result.stdout)
        print("=== ljc logs --last 5 stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc logs --last failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    def test_logs_json_output(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test logs with JSON output format.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "logs",
                "echo.test",
                "--json",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc logs --json stdout ===")
        print(result.stdout)
        print("=== ljc logs --json stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc logs --json failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Output should contain valid JSON
        lines = result.stdout.strip().split('\n')
        json_lines = []
        in_json = False
        for line in lines:
            if line.strip().startswith('{'):
                in_json = True
            if in_json:
                json_lines.append(line)

        if json_lines:
            json_str = '\n'.join(json_lines)
            try:
                data = json.loads(json_str)
                # Should have job_id and executions fields
                assert "job_id" in data, f"Expected 'job_id' in JSON:\n{data}"
                assert "executions" in data, f"Expected 'executions' in JSON:\n{data}"
            except json.JSONDecodeError as e:
                pytest.fail(f"Invalid JSON in output: {e}\n{json_str}")


class TestLjcLogsErrors:
    """Test ljc logs error handling."""

    def test_logs_without_secret(self, ljc_binary, ljc_env):
        """Test logs without LINEARJC_SECRET fails."""
        env = os.environ.copy()
        env.update(ljc_env)
        del env['LINEARJC_SECRET']

        result = subprocess.run(
            [str(ljc_binary), "logs", "any.job"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0
        output = result.stderr + result.stdout
        assert "LINEARJC_SECRET" in output or "secret" in output.lower()

    def test_logs_without_broker(self, ljc_binary, ljc_env):
        """Test logs with invalid broker fails gracefully."""
        env = os.environ.copy()
        env.update(ljc_env)
        env['MQTT_BROKER'] = 'nonexistent.invalid.host'
        env['MQTT_PORT'] = '9999'

        result = subprocess.run(
            [str(ljc_binary), "logs", "any.job"],
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

    def test_logs_requires_job_id(self, ljc_binary, ljc_env):
        """Test logs without job_id fails."""
        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [str(ljc_binary), "logs"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should fail with usage error
        assert result.returncode != 0
