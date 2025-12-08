"""
E2E tests for ljc sync command.

Tests the registry synchronization workflow:
  ljc sync → coordinator DeveloperAPI → registry response → local update

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
"""
import os
import subprocess
import tempfile
import time  # Used for polling loop
from pathlib import Path

import pytest
import yaml


class TestLjcSync:
    """Test ljc sync command with local infrastructure."""

    def test_ljc_sync_fetches_registry(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        e2e_config_dir,
        e2e_temp_dir,
        coordinator_ready,
    ):
        """
        Test that ljc sync fetches registry from coordinator.

        Steps:
        1. Create an ljc repository with empty registry
        2. Run ljc sync --from <broker>
        3. Verify local registry.yaml was updated with coordinator's entries
        """

        # Create a minimal ljc repository in temp dir
        ljc_repo = e2e_temp_dir / "ljc_sync_test_repo"
        ljc_repo.mkdir()
        (ljc_repo / "jobs").mkdir()
        (ljc_repo / "dist").mkdir()

        # Create empty registry.yaml
        (ljc_repo / "registry.yaml").write_text("registry:\n")

        # Build environment with ljc config
        env = os.environ.copy()
        env.update(ljc_env)

        # Run ljc sync
        result = subprocess.run(
            [
                str(ljc_binary),
                "sync",
                "--from", env['MQTT_BROKER'],
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(ljc_repo),  # Run from the test repo
        )

        # Print output for debugging
        print("\n=== ljc sync stdout ===")
        print(result.stdout)
        print("=== ljc sync stderr ===")
        print(result.stderr)
        print("=== end ljc output ===\n")

        # Verify sync succeeded
        assert result.returncode == 0, (
            f"ljc sync failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Check for success indicators
        assert "Received" in result.stdout or "synced" in result.stdout.lower(), (
            f"Expected success message not found in output:\n{result.stdout}"
        )

        # Verify registry.yaml was updated
        registry_file = ljc_repo / "registry.yaml"
        assert registry_file.exists()

        with open(registry_file) as f:
            registry = yaml.safe_load(f)

        # Coordinator has echo_input, echo_output, deploy_input, deploy_output
        assert registry is not None
        assert 'registry' in registry

        registry_entries = registry['registry']
        assert len(registry_entries) >= 2, (
            f"Expected at least 2 registry entries, got {len(registry_entries)}: {registry_entries}"
        )

        # Verify some known entries exist
        assert 'echo_input' in registry_entries or 'deploy_input' in registry_entries, (
            f"Expected test entries not found: {list(registry_entries.keys())}"
        )

    def test_ljc_sync_shows_diff(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        e2e_config_dir,
        e2e_temp_dir,
    ):
        """
        Test that ljc sync shows diff when registry has different entries.

        Steps:
        1. Create ljc repository with one custom entry
        2. Run ljc sync
        3. Verify diff output shows added entries (from coordinator)
        4. Verify local entry is marked as removed
        """
        # Give coordinator time
        time.sleep(1)

        # Create ljc repository with a local-only entry
        ljc_repo = e2e_temp_dir / "ljc_sync_diff_repo"
        ljc_repo.mkdir()
        (ljc_repo / "jobs").mkdir()
        (ljc_repo / "dist").mkdir()

        # Create registry with local-only entry
        local_registry = """registry:
  local_only_entry: {type: fs, path: /tmp/local, kind: file}
"""
        (ljc_repo / "registry.yaml").write_text(local_registry)

        # Build environment
        env = os.environ.copy()
        env.update(ljc_env)

        # Run ljc sync
        result = subprocess.run(
            [str(ljc_binary), "sync", "--from", env['MQTT_BROKER']],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(ljc_repo),
        )

        print("\n=== ljc sync diff stdout ===")
        print(result.stdout)
        print("=== end output ===\n")

        assert result.returncode == 0

        # Should show local_only_entry as removed
        assert "removed" in result.stdout.lower() or "-" in result.stdout, (
            f"Expected removal indicator in output:\n{result.stdout}"
        )

        # Should show new entries added
        assert "new" in result.stdout.lower() or "+" in result.stdout, (
            f"Expected addition indicator in output:\n{result.stdout}"
        )

    def test_ljc_sync_up_to_date(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        e2e_config_dir,
        e2e_temp_dir,
    ):
        """
        Test ljc sync when local registry matches remote.

        Steps:
        1. Run sync to get initial state
        2. Run sync again
        3. Verify "up to date" message
        """
        # Create repo
        ljc_repo = e2e_temp_dir / "ljc_sync_uptodate_repo"
        ljc_repo.mkdir()
        (ljc_repo / "jobs").mkdir()
        (ljc_repo / "dist").mkdir()
        (ljc_repo / "registry.yaml").write_text("registry:\n")

        env = os.environ.copy()
        env.update(ljc_env)

        # First sync
        result1 = subprocess.run(
            [str(ljc_binary), "sync", "--from", env['MQTT_BROKER']],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(ljc_repo),
        )
        assert result1.returncode == 0

        # Second sync - should be up to date
        result2 = subprocess.run(
            [str(ljc_binary), "sync", "--from", env['MQTT_BROKER']],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(ljc_repo),
        )

        print("\n=== ljc sync (up-to-date) stdout ===")
        print(result2.stdout)
        print("=== end output ===\n")

        assert result2.returncode == 0
        assert "up to date" in result2.stdout.lower(), (
            f"Expected 'up to date' message:\n{result2.stdout}"
        )


class TestLjcSyncErrors:
    """Test ljc sync error handling."""

    def test_sync_without_secret(self, ljc_binary, ljc_env, e2e_temp_dir):
        """Test sync without LINEARJC_SECRET fails."""
        ljc_repo = e2e_temp_dir / "ljc_sync_nosecret_repo"
        ljc_repo.mkdir()
        (ljc_repo / "jobs").mkdir()
        (ljc_repo / "registry.yaml").write_text("registry:\n")

        env = os.environ.copy()
        env.update(ljc_env)
        del env['LINEARJC_SECRET']

        result = subprocess.run(
            [str(ljc_binary), "sync", "--from", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
            cwd=str(ljc_repo),
        )

        assert result.returncode != 0
        assert "LINEARJC_SECRET" in result.stderr or "LINEARJC_SECRET" in result.stdout

    def test_sync_not_in_repo(self, ljc_binary, ljc_env, e2e_temp_dir):
        """Test sync fails when not in an ljc repository."""
        # Create empty directory (no registry.yaml)
        empty_dir = e2e_temp_dir / "ljc_sync_empty_dir"
        empty_dir.mkdir()

        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [str(ljc_binary), "sync", "--from", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
            cwd=str(empty_dir),
        )

        assert result.returncode != 0
        assert "repository" in result.stderr.lower() or "repository" in result.stdout.lower()

    def test_sync_connection_timeout(self, ljc_binary, ljc_env, e2e_temp_dir):
        """Test sync fails gracefully when coordinator is unreachable."""
        ljc_repo = e2e_temp_dir / "ljc_sync_timeout_repo"
        ljc_repo.mkdir()
        (ljc_repo / "jobs").mkdir()
        (ljc_repo / "registry.yaml").write_text("registry:\n")

        env = os.environ.copy()
        env.update(ljc_env)
        # Use invalid broker that won't connect
        env['MQTT_BROKER'] = '192.0.2.1'  # TEST-NET-1, should timeout

        result = subprocess.run(
            [str(ljc_binary), "sync", "--from", "192.0.2.1"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,  # Should timeout before this
            cwd=str(ljc_repo),
        )

        assert result.returncode != 0
        assert "connect" in result.stderr.lower() or "connect" in result.stdout.lower() or \
               "timeout" in result.stderr.lower() or "timeout" in result.stdout.lower() or \
               "failed" in result.stderr.lower() or "failed" in result.stdout.lower()
