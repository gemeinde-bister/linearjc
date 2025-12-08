"""
E2E tests for ljc ps command.

Tests the process listing workflow:
  ljc ps → coordinator DeveloperAPI → returns active job executions

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
  - Coordinator running
"""
import os
import subprocess
import json

import pytest


class TestLjcPs:
    """Test ljc ps command with local infrastructure."""

    def test_ps_no_active_jobs(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test ps shows no active jobs when nothing is running.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "ps",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc ps stdout ===")
        print(result.stdout)
        print("=== ljc ps stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc ps failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Output should indicate no jobs or show empty table
        output_lower = result.stdout.lower()
        # Either "no active jobs" message or Total: 0 jobs
        assert "no active" in output_lower or "total" in output_lower, (
            f"Expected 'no active' or job count in output:\n{result.stdout}"
        )

    def test_ps_json_output(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test ps with JSON output format.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "ps",
                "--json",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc ps --json stdout ===")
        print(result.stdout)
        print("=== ljc ps --json stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc ps --json failed with code {result.returncode}\n"
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
                # Should have jobs array
                assert "jobs" in data, f"Expected 'jobs' in JSON:\n{data}"
                assert isinstance(data["jobs"], list)
            except json.JSONDecodeError as e:
                pytest.fail(f"Invalid JSON in output: {e}\n{json_str}")

    def test_ps_with_all_flag(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test ps --all includes completed jobs.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "ps",
                "--all",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc ps --all stdout ===")
        print(result.stdout)
        print("=== ljc ps --all stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed
        assert result.returncode == 0, (
            f"ljc ps --all failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    def test_ps_with_executor_filter(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test ps --executor filters by executor ID.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "ps",
                "--executor", "nonexistent-executor",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc ps --executor stdout ===")
        print(result.stdout)
        print("=== ljc ps --executor stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should succeed (just shows no jobs matching filter)
        assert result.returncode == 0, (
            f"ljc ps --executor failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )


class TestLjcPsErrors:
    """Test ljc ps error handling."""

    def test_ps_without_secret(self, ljc_binary, ljc_env):
        """Test ps without LINEARJC_SECRET fails."""
        env = os.environ.copy()
        env.update(ljc_env)
        del env['LINEARJC_SECRET']

        result = subprocess.run(
            [str(ljc_binary), "ps"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0
        output = result.stderr + result.stdout
        assert "LINEARJC_SECRET" in output or "secret" in output.lower()

    def test_ps_without_broker(self, ljc_binary, ljc_env):
        """Test ps with invalid broker fails gracefully."""
        env = os.environ.copy()
        env.update(ljc_env)
        env['MQTT_BROKER'] = 'nonexistent.invalid.host'
        env['MQTT_PORT'] = '9999'

        result = subprocess.run(
            [str(ljc_binary), "ps"],
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
