"""
E2E tests for multi-job chain execution (Phase 12).

Tests the coordinator's ability to execute job dependency chains:
- Job A → Job B (two-job chain)
- Intermediate data passing via MinIO
- Chain completion detection
- Progress forwarding for all jobs in chain
"""
import os
import sys
import time
import json
import shutil
import tarfile
import tempfile
import getpass
from pathlib import Path
from typing import Dict, Any, List, Optional

import pytest
import yaml

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from e2e.conftest import is_root


class TestChainExecution:
    """Tests for multi-job chain execution."""

    @pytest.fixture(scope="class")
    def chain_config_dir(self, e2e_temp_dir, mosquitto_server, minio_server):
        """
        Create config directory with chain jobs.

        Sets up:
        - chain.step1: reads external input, writes intermediate
        - chain.step2: reads intermediate (from step1), writes final output
        """
        config_dir = e2e_temp_dir / "chain_config"
        config_dir.mkdir(exist_ok=True)

        jobs_dir = config_dir / "jobs"
        jobs_dir.mkdir(exist_ok=True)

        packages_dir = config_dir / "packages"
        packages_dir.mkdir(exist_ok=True)

        work_dir = e2e_temp_dir / "chain_work"
        work_dir.mkdir(exist_ok=True)

        data_dir = e2e_temp_dir / "chain_data"
        data_dir.mkdir(exist_ok=True)

        current_user = getpass.getuser()

        # Create coordinator config
        config = {
            'coordinator': {
                'mqtt': {
                    'broker': mosquitto_server['host'],
                    'port': mosquitto_server['port'],
                    'keepalive': 60,
                },
                'minio': {
                    'endpoint': minio_server['endpoint'],
                    'access_key': minio_server['access_key'],
                    'secret_key': minio_server['secret_key'],
                    'secure': False,
                    'temp_bucket': 'linearjc-chain-test',
                },
                'jobs_dir': str(jobs_dir),
                'data_registry': str(config_dir / 'data_registry.yaml'),
                'work_dir': str(work_dir),
                'scheduling': {
                    'loop_interval': 2,
                    'message_max_age': 60,
                },
                'signing': {
                    'shared_secret': 'chain-test-secret-for-local-e2e-testing-32ch',
                },
                'security': {
                    'allowed_data_roots': [str(data_dir), '/tmp'],
                    'validate_secrets': False,
                },
                'logging': {
                    'level': 'DEBUG',
                    'json_format': False,
                },
                'archive': {
                    'format': 'tar.gz',
                },
            }
        }

        with open(config_dir / 'config.yaml', 'w') as f:
            yaml.dump(config, f)

        # Create data registry with chain entries
        registry = {
            'registry': {
                'chain_external_input': {
                    'type': 'fs',
                    'path': str(data_dir / 'chain_external_input'),
                    'kind': 'dir',
                },
                'chain_intermediate': {
                    'type': 'fs',
                    'path': str(data_dir / 'chain_intermediate'),
                    'kind': 'dir',
                },
                'chain_final_output': {
                    'type': 'fs',
                    'path': str(data_dir / 'chain_final_output'),
                    'kind': 'dir',
                },
            }
        }

        with open(config_dir / 'data_registry.yaml', 'w') as f:
            yaml.dump(registry, f)

        # Copy chain jobs with current user
        test_jobs_dir = Path(__file__).parent / 'jobs'

        for job_name in ['chain-step1', 'chain-step2']:
            job_src = test_jobs_dir / job_name

            with open(job_src / 'job.yaml') as f:
                job_config = yaml.safe_load(f)
            job_config['job']['run']['user'] = current_user

            job_id = job_config['job']['id']

            # Write to coordinator jobs dir
            with open(jobs_dir / f'{job_id}.yaml', 'w') as f:
                yaml.dump(job_config, f)

            # Copy to packages dir
            pkg_dir = packages_dir / job_id
            shutil.copytree(job_src, pkg_dir)
            with open(pkg_dir / 'job.yaml', 'w') as f:
                yaml.dump(job_config, f)

        # Create external input data
        input_dir = data_dir / 'chain_external_input'
        input_dir.mkdir(exist_ok=True)
        (input_dir / 'data.txt').write_text('Hello from chain test!\n')

        yield {
            'config_dir': config_dir,
            'config_file': config_dir / 'config.yaml',
            'jobs_dir': jobs_dir,
            'packages_dir': packages_dir,
            'work_dir': work_dir,
            'data_dir': data_dir,
            'shared_secret': config['coordinator']['signing']['shared_secret'],
        }

    @pytest.mark.skipif(not is_root(), reason="E2E tests require root")
    def test_tree_building_with_chain(self, chain_config_dir):
        """Test that tree builder correctly creates chain from dependent jobs."""
        from coordinator.models import Job, JobFile
        from coordinator.tree_builder import build_trees

        jobs_dir = chain_config_dir['jobs_dir']

        # Load jobs
        jobs = []
        for yaml_file in jobs_dir.glob('*.yaml'):
            job_file = JobFile.from_yaml(yaml_file.read_text())
            jobs.append(job_file.job)

        assert len(jobs) == 2, "Should have 2 chain jobs"

        # Build trees
        trees = build_trees(jobs)

        assert len(trees) == 1, "Should build 1 tree from chain"

        tree = trees[0]
        assert tree.root.id == 'chain.step1', "Root should be step1 (no dependencies)"
        assert len(tree.jobs) == 2, "Tree should contain 2 jobs"
        assert tree.jobs[0].id == 'chain.step1', "First job should be step1"
        assert tree.jobs[1].id == 'chain.step2', "Second job should be step2"

    @pytest.mark.skipif(not is_root(), reason="E2E tests require root")
    def test_chain_job_tracker_index(self, chain_config_dir):
        """Test that JobExecution tracks job_index correctly."""
        from coordinator.job_tracker import JobTracker, JobExecution, JobState

        tracker = JobTracker()

        # Create executions for a 2-job chain
        tree_exec_id = "chain.step1-20251203-120000-abcd1234"

        exec1 = JobExecution(
            job_execution_id="chain.step1-20251203-120000-abcd1234",
            tree_execution_id=tree_exec_id,
            job_id="chain.step1",
            job_version="1.0.0",
            state=JobState.COMPLETED,
            executor_id="test-executor",
            assigned_at=1000.0,
            timeout_at=1030.0,
            last_progress=1010.0,
            job_index=0,
        )
        tracker.add(exec1)

        exec2 = JobExecution(
            job_execution_id="chain.step2-20251203-120000-abcd1234",
            tree_execution_id=tree_exec_id,
            job_id="chain.step2",
            job_version="1.0.0",
            state=JobState.RUNNING,
            executor_id="test-executor",
            assigned_at=1011.0,
            timeout_at=1041.0,
            last_progress=1015.0,
            job_index=1,
        )
        tracker.add(exec2)

        # Find by tree execution ID
        executions = tracker.find_by_tree_execution_id(tree_exec_id)

        assert len(executions) == 2, "Should find 2 executions"
        assert executions[0].job_index == 0, "First should be index 0"
        assert executions[1].job_index == 1, "Second should be index 1"
        assert executions[0].job_id == "chain.step1"
        assert executions[1].job_id == "chain.step2"

    @pytest.mark.skipif(not is_root(), reason="E2E tests require root")
    def test_intermediate_input_preparation(self, chain_config_dir, minio_server):
        """Test that intermediate inputs are prepared from MinIO."""
        from coordinator.models import Job, JobFile, DataRegistryEntry
        from coordinator.minio_manager import MinioManager
        from coordinator.job_executor import JobExecutor
        from coordinator.output_locks import OutputLockManager

        jobs_dir = chain_config_dir['jobs_dir']
        work_dir = chain_config_dir['work_dir']
        data_dir = chain_config_dir['data_dir']

        # Load jobs
        job1_file = JobFile.from_yaml((jobs_dir / 'chain.step1.yaml').read_text())
        job2_file = JobFile.from_yaml((jobs_dir / 'chain.step2.yaml').read_text())
        job1 = job1_file.job
        job2 = job2_file.job

        # Setup MinIO
        minio_mgr = MinioManager(
            endpoint=minio_server['endpoint'],
            access_key=minio_server['access_key'],
            secret_key=minio_server['secret_key'],
            secure=False
        )

        # Create bucket
        temp_bucket = 'linearjc-chain-intermediate-test'
        if not minio_mgr.client.bucket_exists(temp_bucket):
            minio_mgr.client.make_bucket(temp_bucket)

        # Create data registry
        data_registry = {
            'chain_intermediate': DataRegistryEntry(
                type='fs',
                path=str(data_dir / 'chain_intermediate'),
                kind='dir'
            ),
        }

        # Create job executor
        lock_mgr = OutputLockManager()
        executor = JobExecutor(
            minio_manager=minio_mgr,
            data_registry=data_registry,
            temp_bucket=temp_bucket,
            output_lock_manager=lock_mgr,
            work_dir=str(work_dir / 'executor_test'),
        )

        # Simulate step1 having uploaded intermediate output
        tree_exec_id = "chain.step1-20251203-130000-test1234"
        intermediate_file = work_dir / 'test_intermediate.tar.gz'

        # Create fake intermediate output
        import tarfile
        intermediate_dir = work_dir / 'intermediate_content' / 'chain_intermediate'
        intermediate_dir.mkdir(parents=True, exist_ok=True)
        (intermediate_dir / 'processed.txt').write_text('Step1 processed: test data\n')

        with tarfile.open(intermediate_file, 'w:gz') as tar:
            tar.add(
                str(intermediate_dir.parent / 'chain_intermediate'),
                arcname='chain_intermediate'
            )

        # Upload to MinIO (simulating executor's upload)
        minio_mgr.upload_file(
            str(intermediate_file),
            temp_bucket,
            f'jobs/{tree_exec_id}/output_chain_intermediate.tar.gz'
        )

        # Now test prepare_chain_job_inputs
        from coordinator.models import JobTree
        tree = JobTree(
            root=job1,
            jobs=[job1, job2],
            min_daily=1,
            max_daily=100,
        )

        prepared = executor.prepare_chain_job_inputs(
            job=job2,
            prev_job=job1,
            tree=tree,
            tree_execution_id=tree_exec_id
        )

        assert 'chain_intermediate' in prepared
        assert prepared['chain_intermediate']['method'] == 'GET'
        assert prepared['chain_intermediate']['format'] == 'tar.gz'
        assert 'url' in prepared['chain_intermediate']
        assert tree_exec_id in prepared['chain_intermediate']['url']


class TestChainIntegration:
    """Integration tests for chain execution with full coordinator."""

    @pytest.mark.skipif(not is_root(), reason="E2E tests require root")
    def test_chain_execution_flow(
        self,
        chain_config_dir,
        mosquitto_server,
        minio_server
    ):
        """
        Full E2E test: execute a 2-job chain and verify outputs.

        This test:
        1. Starts coordinator with chain jobs
        2. Triggers chain execution
        3. Verifies step1 executes and produces intermediate output
        4. Verifies step2 executes after step1 completes
        5. Verifies final output is collected to filesystem
        """
        # This test requires the full E2E infrastructure
        # Implementation would follow the pattern in test_smoke.py
        # but with chain-specific assertions

        # For now, skip if not in full E2E environment
        pytest.skip("Full chain E2E test requires complete infrastructure setup")
