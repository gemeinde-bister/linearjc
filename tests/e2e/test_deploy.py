"""
E2E tests for ljc deploy command.

Tests the full deployment pipeline:
  ljc deploy → coordinator DeveloperAPI → MinIO → coordinator install → executor gets job

Requires:
  - ljc binary built (cargo build --release in tools/ljc)
  - Executor binary built (cargo build --release in src/executor)
"""
import os
import subprocess
import time  # Used for polling loops
from pathlib import Path

import pytest


class TestLjcDeploy:
    """Test ljc deploy command with local infrastructure."""

    def test_ljc_binary_exists(self, ljc_binary):
        """Verify ljc binary is available."""
        assert ljc_binary.exists(), f"ljc binary not found at {ljc_binary}"
        assert os.access(ljc_binary, os.X_OK), f"ljc binary not executable"

    def test_deploy_package_created(self, deploy_test_package):
        """Verify test package was created."""
        assert deploy_test_package.exists()
        assert deploy_test_package.stat().st_size > 0

    def test_ljc_deploy_to_coordinator(
        self,
        ljc_binary,
        ljc_env,
        deploy_test_package,
        coordinator_process,
        executor_process,
        e2e_config_dir,
        coordinator_ready,
    ):
        """
        Test full deployment workflow via ljc deploy.

        Steps:
        1. Run ljc deploy with test package
        2. Verify deployment succeeds
        3. Verify job was installed in coordinator's jobs_dir
        """

        # Build environment with ljc config
        env = os.environ.copy()
        env.update(ljc_env)

        # Run ljc deploy
        result = subprocess.run(
            [
                str(ljc_binary),
                "deploy",
                str(deploy_test_package),
                "--to", "localhost",  # --to is required but ignored when using env vars
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,  # 30s upload URL + 60s install + buffer
        )

        # Print output for debugging
        print("\n=== ljc deploy stdout ===")
        print(result.stdout)
        print("=== ljc deploy stderr ===")
        print(result.stderr)
        print("=== end ljc output ===\n")

        # Verify deployment succeeded
        assert result.returncode == 0, (
            f"ljc deploy failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

        # Check for success message
        assert "Deployment successful" in result.stdout or "installed" in result.stdout.lower(), (
            f"Expected success message not found in output:\n{result.stdout}"
        )

        # Verify job was installed in coordinator's jobs_dir
        jobs_dir = e2e_config_dir['jobs_dir']
        installed_job = jobs_dir / "deploy.test.yaml"

        # Give coordinator a moment to write the file
        for _ in range(10):
            if installed_job.exists():
                break
            time.sleep(0.5)

        assert installed_job.exists(), (
            f"Job not installed at {installed_job}\n"
            f"Contents of jobs_dir: {list(jobs_dir.iterdir())}"
        )

    def test_deployed_job_executes(
        self,
        ljc_binary,
        ljc_env,
        deploy_test_package,
        coordinator_process,
        executor_process,
        e2e_config_dir,
    ):
        """
        Test that a deployed job can be executed.

        This test depends on test_ljc_deploy_to_coordinator running first
        (pytest runs tests in order within a class).

        Steps:
        1. Wait for coordinator to discover and schedule the deployed job
        2. Wait for executor to execute the job
        3. Verify output was created
        """
        data_dir = e2e_config_dir['data_dir']
        output_dir = data_dir / "deploy_output"

        # The job should eventually execute
        # Coordinator loop_interval is 2s, so give it time
        max_wait = 30
        start = time.time()

        while time.time() - start < max_wait:
            # Check if output was created
            result_file = output_dir / "result.txt"
            if result_file.exists():
                content = result_file.read_text()
                assert "Deployed and executed successfully" in content
                print(f"\nJob output:\n{content}")
                return

            time.sleep(2)

        # If we get here, job didn't execute
        # Print logs for debugging
        coordinator_log = e2e_config_dir['work_dir'] / 'coordinator.log'
        if coordinator_log.exists():
            print(f"\n=== Coordinator log (last 3000 chars) ===")
            print(coordinator_log.read_text()[-3000:])

        executor_log = e2e_config_dir['work_dir'] / 'executor.log'
        if executor_log.exists():
            print(f"\n=== Executor log (last 3000 chars) ===")
            print(executor_log.read_text()[-3000:])

        pytest.fail(
            f"Deployed job did not execute within {max_wait}s\n"
            f"Expected output at: {output_dir}/result.txt"
        )


class TestLjcDeployErrors:
    """Test ljc deploy error handling."""

    def test_deploy_nonexistent_package(self, ljc_binary, ljc_env):
        """Test deployment of non-existent package fails gracefully."""
        env = os.environ.copy()
        env.update(ljc_env)

        result = subprocess.run(
            [str(ljc_binary), "deploy", "/nonexistent/package.ljc", "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0
        assert "not found" in result.stderr.lower() or "not found" in result.stdout.lower()

    def test_deploy_without_secret(self, ljc_binary, ljc_env):
        """Test deployment without LINEARJC_SECRET fails."""
        env = os.environ.copy()
        env.update(ljc_env)
        del env['LINEARJC_SECRET']  # Remove the secret

        result = subprocess.run(
            [str(ljc_binary), "deploy", "dummy.ljc", "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode != 0
        assert "LINEARJC_SECRET" in result.stderr or "LINEARJC_SECRET" in result.stdout


class TestVersionUpgrade:
    """Test version upgrade behavior during deployment."""

    def _create_test_package(self, e2e_temp_dir, e2e_config_dir, version: str, suffix: str = ""):
        """Helper to create a test package with specified version."""
        import tarfile
        import getpass

        current_user = getpass.getuser()
        data_dir = e2e_config_dir['data_dir']

        # Create job directory
        job_dir = e2e_temp_dir / f"upgrade-test-job-{version.replace('.', '-')}{suffix}"
        job_dir.mkdir(exist_ok=True)

        # job.yaml with specified version
        job_yaml = f"""job:
  id: upgrade.test
  version: "{version}"
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
        script_sh = f"""#!/bin/sh
echo "Upgrade test job version {version}"
"""
        script_path = job_dir / "script.sh"
        script_path.write_text(script_sh)
        script_path.chmod(0o755)

        # manifest.yaml
        manifest_yaml = f"""job_id: upgrade.test
version: "{version}"
created_at: "2025-12-03T00:00:00Z"
files:
  - job.yaml
  - script.sh
"""
        (job_dir / "manifest.yaml").write_text(manifest_yaml)

        # Build .ljc package
        dist_dir = e2e_temp_dir / f"dist-{version.replace('.', '-')}{suffix}"
        dist_dir.mkdir(exist_ok=True)
        package_path = dist_dir / "upgrade.test.ljc"

        with tarfile.open(package_path, "w:gz") as tar:
            tar.add(job_dir / "job.yaml", arcname="job.yaml")
            tar.add(job_dir / "script.sh", arcname="script.sh")
            tar.add(job_dir / "manifest.yaml", arcname="manifest.yaml")

        return package_path

    def test_version_upgrade_succeeds(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        e2e_config_dir,
        e2e_temp_dir,
        coordinator_ready,
    ):
        """
        Test that deploying a higher version succeeds.

        Steps:
        1. Deploy v1.0.0
        2. Deploy v2.0.0 (upgrade)
        3. Verify both succeed
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Deploy v1.0.0
        package_v1 = self._create_test_package(e2e_temp_dir, e2e_config_dir, "1.0.0")
        result1 = subprocess.run(
            [str(ljc_binary), "deploy", str(package_v1), "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )

        print("\n=== Deploy v1.0.0 ===")
        print(result1.stdout)
        if result1.stderr:
            print(result1.stderr)

        assert result1.returncode == 0, f"v1.0.0 deploy failed: {result1.stdout}"

        # Deploy v2.0.0 (upgrade)
        package_v2 = self._create_test_package(e2e_temp_dir, e2e_config_dir, "2.0.0")
        result2 = subprocess.run(
            [str(ljc_binary), "deploy", str(package_v2), "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )

        print("\n=== Deploy v2.0.0 (upgrade) ===")
        print(result2.stdout)
        if result2.stderr:
            print(result2.stderr)

        assert result2.returncode == 0, f"v2.0.0 upgrade failed: {result2.stdout}"

        # Verify job file has v2.0.0
        jobs_dir = e2e_config_dir['jobs_dir']
        job_file = jobs_dir / "upgrade.test.yaml"

        # Give coordinator time to write
        time.sleep(1)

        assert job_file.exists(), f"Job file not found: {job_file}"
        content = job_file.read_text()
        assert "2.0.0" in content, f"Expected version 2.0.0 in job file:\n{content}"

    def test_same_version_fails(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        e2e_config_dir,
        e2e_temp_dir,
        coordinator_ready,
    ):
        """
        Test that deploying the same version fails.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Deploy v3.0.0 first time
        package_v3a = self._create_test_package(e2e_temp_dir, e2e_config_dir, "3.0.0", suffix="a")
        result1 = subprocess.run(
            [str(ljc_binary), "deploy", str(package_v3a), "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result1.returncode == 0, f"First deploy failed: {result1.stdout}"

        # Deploy v3.0.0 again (should fail)
        package_v3b = self._create_test_package(e2e_temp_dir, e2e_config_dir, "3.0.0", suffix="b")
        result2 = subprocess.run(
            [str(ljc_binary), "deploy", str(package_v3b), "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )

        print("\n=== Deploy same version (should fail) ===")
        print(result2.stdout)

        assert result2.returncode != 0, "Same version deploy should have failed"
        assert "already installed" in result2.stdout.lower() or "bump" in result2.stdout.lower(), (
            f"Expected 'already installed' message:\n{result2.stdout}"
        )

    def test_downgrade_fails(
        self,
        ljc_binary,
        ljc_env,
        coordinator_process,
        e2e_config_dir,
        e2e_temp_dir,
        coordinator_ready,
    ):
        """
        Test that downgrading to a lower version fails.
        """

        env = os.environ.copy()
        env.update(ljc_env)

        # Deploy v5.0.0 first
        package_v5 = self._create_test_package(e2e_temp_dir, e2e_config_dir, "5.0.0")
        result1 = subprocess.run(
            [str(ljc_binary), "deploy", str(package_v5), "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result1.returncode == 0, f"v5.0.0 deploy failed: {result1.stdout}"

        # Try to deploy v4.0.0 (downgrade - should fail)
        package_v4 = self._create_test_package(e2e_temp_dir, e2e_config_dir, "4.0.0")
        result2 = subprocess.run(
            [str(ljc_binary), "deploy", str(package_v4), "--to", "localhost"],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )

        print("\n=== Deploy downgrade (should fail) ===")
        print(result2.stdout)

        assert result2.returncode != 0, "Downgrade should have failed"
        assert "downgrade" in result2.stdout.lower() or "cannot" in result2.stdout.lower(), (
            f"Expected downgrade error message:\n{result2.stdout}"
        )
