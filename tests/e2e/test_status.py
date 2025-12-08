"""
E2E tests for ljc status command.

Tests the job status query workflow:
  ljc status → coordinator DeveloperAPI → returns job scheduling state

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
  - Coordinator running with registered jobs
"""
import os
import subprocess
import json

import pytest


class TestLjcStatus:
    """Test ljc status command with local infrastructure."""

    def test_status_unknown_job_not_found(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test that querying status of unknown job returns error.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "status",
                "nonexistent.job",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc status unknown job stdout ===")
        print(result.stdout)
        print("=== ljc status unknown job stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because job doesn't exist
        assert result.returncode != 0
        output = result.stdout.lower() + result.stderr.lower()
        assert "not found" in output or "failed" in output, (
            f"Expected 'not found' or 'failed' in output:\n{result.stdout}\n{result.stderr}"
        )

    def test_status_known_job(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test status query for known job (echo.test).
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "status",
                "echo.test",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc status echo.test stdout ===")
        print(result.stdout)
        print("=== ljc status echo.test stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc status failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Output should contain job ID
        assert "echo.test" in result.stdout, (
            f"Expected 'echo.test' in output:\n{result.stdout}"
        )

        # Output should contain schedule info
        assert "schedule" in result.stdout.lower() or "execution" in result.stdout.lower(), (
            f"Expected schedule or execution info in output:\n{result.stdout}"
        )

    def test_status_all_jobs(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test status query for all jobs.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "status",
                "--all",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc status --all stdout ===")
        print(result.stdout)
        print("=== ljc status --all stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc status --all failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Output should contain job count
        assert "job" in result.stdout.lower(), (
            f"Expected 'job' in output:\n{result.stdout}"
        )

    def test_status_json_output(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test status query with JSON output.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "status",
                "echo.test",
                "--json",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc status --json stdout ===")
        print(result.stdout)
        print("=== ljc status --json stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc status --json failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Output should contain valid JSON (extract JSON from output)
        # The output contains progress messages before JSON, so find the JSON part
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
                # Should have job_id field
                assert "job_id" in data, f"Expected 'job_id' in JSON:\n{data}"
            except json.JSONDecodeError as e:
                pytest.fail(f"Invalid JSON in output: {e}\n{json_str}")


class TestLjcStatusErrors:
    """Test ljc status error handling."""

    def test_status_without_secret(self, ljc_binary, ljc_env):
        """Test status without LINEARJC_SECRET fails."""
        env = os.environ.copy()
        env.update(ljc_env)
        del env['LINEARJC_SECRET']  # Remove the secret

        result = subprocess.run(
            [str(ljc_binary), "status", "any.job"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0
        output = result.stderr + result.stdout
        assert "LINEARJC_SECRET" in output or "secret" in output.lower()

    def test_status_without_broker(self, ljc_binary, ljc_env):
        """Test status with invalid broker fails gracefully."""
        env = os.environ.copy()
        env.update(ljc_env)
        env['MQTT_BROKER'] = 'nonexistent.invalid.host'
        env['MQTT_PORT'] = '9999'

        result = subprocess.run(
            [str(ljc_binary), "status", "any.job"],
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

    def test_status_requires_job_id_or_all(self, ljc_binary, ljc_env):
        """Test status without job_id or --all fails."""
        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [str(ljc_binary), "status"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should fail with usage error
        assert result.returncode != 0
