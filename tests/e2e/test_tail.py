"""
E2E tests for ljc tail command.

Tests the job progress tailing workflow:
  ljc tail → coordinator DeveloperAPI → attach to execution → stream progress

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
  - Coordinator running
"""
import os
import subprocess
from pathlib import Path

import pytest


class TestLjcTail:
    """Test ljc tail command with local infrastructure."""

    def test_tail_unknown_job_not_found(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test that tailing an unknown job returns not_found.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [
                str(ljc_binary),
                "tail",
                "nonexistent.job",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc tail unknown job stdout ===")
        print(result.stdout)
        print("=== ljc tail unknown job stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because no active execution exists
        assert result.returncode != 0
        assert "not found" in result.stdout.lower() or "no execution" in result.stdout.lower(), (
            f"Expected 'not found' or 'no execution' in output:\n{result.stdout}"
        )

    def test_tail_unknown_execution_id_not_found(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        coordinator_ready,
    ):
        """
        Test that tailing an unknown execution ID returns not_found.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Use a fake execution ID format
        result = subprocess.run(
            [
                str(ljc_binary),
                "tail",
                "fake.job-20251203-120000-abc123def",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc tail unknown execution stdout ===")
        print(result.stdout)
        print("=== ljc tail unknown execution stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because execution doesn't exist
        assert result.returncode != 0
        assert "not found" in result.stdout.lower(), (
            f"Expected 'not found' in output:\n{result.stdout}"
        )

    def test_tail_job_with_no_active_execution(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        executor_process,
        coordinator_ready,
    ):
        """
        Test that tailing a known job with no active execution returns not_found.

        Even though echo.test exists as a job, if there's no running execution,
        tail should report no active execution found.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Try to tail echo.test when nothing is running
        result = subprocess.run(
            [
                str(ljc_binary),
                "tail",
                "echo.test",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc tail no active execution stdout ===")
        print(result.stdout)
        print("=== ljc tail no active execution stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail because no active execution exists
        assert result.returncode != 0
        assert "no active" in result.stdout.lower() or "not found" in result.stdout.lower(), (
            f"Expected 'no active' or 'not found' in output:\n{result.stdout}"
        )


class TestLjcTailErrors:
    """Test ljc tail error handling."""

    def test_tail_without_secret(
        self,
        ljc_binary,
        mosquitto_server,
        minio_server,
        e2e_config_dir,
    ):
        """
        Test that tail without LINEARJC_SECRET fails with auth error.
        """
        env = os.environ.copy()
        # Set MQTT broker but NOT the secret
        env["MQTT_BROKER"] = "127.0.0.1"
        env["MQTT_PORT"] = str(mosquitto_server["port"])
        env["LINEARJC_COORDINATOR_ID"] = "linearjc-coordinator"
        env["LINEARJC_CLIENT_ID"] = "test-client"
        # Deliberately NOT setting LINEARJC_SECRET

        result = subprocess.run(
            [
                str(ljc_binary),
                "tail",
                "echo.test",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc tail without secret stdout ===")
        print(result.stdout)
        print("=== ljc tail without secret stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail - either config error or auth error
        assert result.returncode != 0
        # Either "secret" in error or "configuration" in error
        error_text = result.stderr.lower() + result.stdout.lower()
        assert "secret" in error_text or "config" in error_text, (
            f"Expected secret/config error:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_tail_without_broker(
        self,
        ljc_binary,
        e2e_config_dir,
    ):
        """
        Test that tail without MQTT broker fails with connection error.
        """
        env = os.environ.copy()
        # Set secret but wrong broker port
        env["LINEARJC_SECRET"] = "test-secret"
        env["MQTT_BROKER"] = "127.0.0.1"
        env["MQTT_PORT"] = "65534"  # Invalid port
        env["LINEARJC_COORDINATOR_ID"] = "linearjc-coordinator"
        env["LINEARJC_CLIENT_ID"] = "test-client"

        result = subprocess.run(
            [
                str(ljc_binary),
                "tail",
                "echo.test",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\n=== ljc tail without broker stdout ===")
        print(result.stdout)
        print("=== ljc tail without broker stderr ===")
        print(result.stderr)
        print("=== end output ===\n")

        # Should fail with connection error
        assert result.returncode != 0
        error_text = result.stderr.lower() + result.stdout.lower()
        assert "connect" in error_text or "mqtt" in error_text or "broker" in error_text, (
            f"Expected connection error:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
