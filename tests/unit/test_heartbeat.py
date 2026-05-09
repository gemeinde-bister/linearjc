"""
Unit tests for heartbeat-based executor discovery (Phase 14).
"""
import time
import pytest

from coordinator.capability_discovery import ExecutorRegistry, ExecutorInfo


class MockMqttClient:
    """Mock MQTT client for testing."""
    def publish_message(self, topic, message, qos=1):
        pass


class TestHeartbeatHandling:
    """Test heartbeat message handling in ExecutorRegistry."""

    def test_heartbeat_updates_registry(self):
        """Heartbeat should add/update executor in registry."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=60.0)

        # Send heartbeat
        heartbeat = {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [{"job_id": "test.job", "version": "1.0.0"}],
            "capability_types": ["general"],
            "supported_formats": ["tar.gz"]
        }
        registry.handle_heartbeat("exec-1", heartbeat)

        # Verify registry updated
        executors = registry.get_all_executors()
        assert "exec-1" in executors
        assert executors["exec-1"].hostname == "node1"
        assert len(executors["exec-1"].capabilities) == 1

    def test_heartbeat_prevents_stale(self):
        """Heartbeat should update last_query_time so registry isn't stale."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=0.1)

        # Registry starts stale
        assert registry.is_stale()

        # Send heartbeat
        heartbeat = {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": []
        }
        registry.handle_heartbeat("exec-1", heartbeat)

        # Registry should not be stale now
        assert not registry.is_stale()

    def test_heartbeat_find_executor(self):
        """Should find executor for job after heartbeat."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=60.0)

        # Send heartbeat with capability
        heartbeat = {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [{"job_id": "my.job", "version": "2.0.0"}],
            "capability_types": [],
            "supported_formats": []
        }
        registry.handle_heartbeat("exec-1", heartbeat)

        # Find executor
        executor = registry.find_executor("my.job", "2.0.0")
        assert executor == "exec-1"

        # Non-existent job returns None
        assert registry.find_executor("other.job", "1.0.0") is None

    def test_heartbeat_updates_existing_executor(self):
        """Heartbeat should update existing executor entry."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=60.0)

        # Initial heartbeat
        heartbeat1 = {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [{"job_id": "job.a", "version": "1.0.0"}],
            "capability_types": [],
            "supported_formats": []
        }
        registry.handle_heartbeat("exec-1", heartbeat1)

        # Updated heartbeat with new capability
        heartbeat2 = {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [
                {"job_id": "job.a", "version": "1.0.0"},
                {"job_id": "job.b", "version": "1.0.0"}
            ],
            "capability_types": [],
            "supported_formats": []
        }
        registry.handle_heartbeat("exec-1", heartbeat2)

        # Verify updated
        executors = registry.get_all_executors()
        assert len(executors["exec-1"].capabilities) == 2

    def test_multiple_executors(self):
        """Should track multiple executors from heartbeats."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=60.0)

        # Heartbeats from two executors
        registry.handle_heartbeat("exec-1", {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [{"job_id": "job.a", "version": "1.0.0"}],
            "capability_types": [],
            "supported_formats": []
        })
        registry.handle_heartbeat("exec-2", {
            "executor_id": "exec-2",
            "hostname": "node2",
            "capabilities": [{"job_id": "job.b", "version": "1.0.0"}],
            "capability_types": [],
            "supported_formats": []
        })

        # Both should be tracked
        assert registry.get_executor_count() == 2
        assert registry.find_executor("job.a", "1.0.0") == "exec-1"
        assert registry.find_executor("job.b", "1.0.0") == "exec-2"


class TestStaleExecutorPruning:
    """Test stale executor pruning."""

    def test_prune_removes_stale_executors(self):
        """Executors that haven't sent heartbeats should be removed."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=60.0)

        # Add executor via heartbeat
        registry.handle_heartbeat("exec-1", {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": []
        })

        # Verify added
        assert registry.get_executor_count() == 1

        # Prune with very short TTL (simulating stale executor)
        removed = registry.prune_stale_executors(max_age=0.0)

        # Should be removed
        assert removed == 1
        assert registry.get_executor_count() == 0

    def test_prune_keeps_fresh_executors(self):
        """Executors with recent heartbeats should not be removed."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=60.0)

        # Add executor via heartbeat
        registry.handle_heartbeat("exec-1", {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": []
        })

        # Prune with long TTL (executor is fresh)
        removed = registry.prune_stale_executors(max_age=90.0)

        # Should not be removed
        assert removed == 0
        assert registry.get_executor_count() == 1

    def test_prune_selective(self):
        """Pruning should only remove stale executors."""
        mqtt = MockMqttClient()
        registry = ExecutorRegistry(mqtt, query_timeout=1.0, registry_ttl=60.0)

        # Add two executors
        registry.handle_heartbeat("exec-1", {
            "executor_id": "exec-1",
            "hostname": "node1",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": []
        })

        # Manually age exec-1 by modifying last_seen
        registry._executors["exec-1"] = ExecutorInfo(
            executor_id="exec-1",
            hostname="node1",
            capabilities=[],
            capability_types=[],
            supported_formats=[],
            last_seen=time.time() - 100,  # 100 seconds ago
            last_query_id="heartbeat"
        )

        # Add fresh executor
        registry.handle_heartbeat("exec-2", {
            "executor_id": "exec-2",
            "hostname": "node2",
            "capabilities": [],
            "capability_types": [],
            "supported_formats": []
        })

        # Prune with 90s TTL
        removed = registry.prune_stale_executors(max_age=90.0)

        # Only stale one should be removed
        assert removed == 1
        assert registry.get_executor_count() == 1
        assert "exec-2" in registry.get_all_executors()
