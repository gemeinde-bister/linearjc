"""Integration tests for MQTT operations."""
import sys
import time
import json
import threading
from pathlib import Path

import pytest
import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


class TestMqttConnection:
    """Tests for MQTT connectivity."""

    def test_mosquitto_server_starts(self, mosquitto_server):
        """Mosquitto server fixture starts correctly."""
        assert mosquitto_server['host'] == '127.0.0.1'
        assert mosquitto_server['port'] > 0

    def test_can_connect(self, mosquitto_server):
        """Can connect to MQTT broker."""
        client = mqtt.Client()
        client.connect(mosquitto_server['host'], mosquitto_server['port'])
        client.disconnect()

    def test_publish_subscribe(self, mosquitto_server):
        """Can publish and receive messages."""
        received = []
        connected = threading.Event()

        def on_connect(client, userdata, flags, rc):
            client.subscribe("test/topic")
            connected.set()

        def on_message(client, userdata, msg):
            received.append(json.loads(msg.payload.decode()))

        # Subscriber
        sub_client = mqtt.Client()
        sub_client.on_connect = on_connect
        sub_client.on_message = on_message
        sub_client.connect(mosquitto_server['host'], mosquitto_server['port'])
        sub_client.loop_start()

        # Wait for subscription
        assert connected.wait(timeout=2.0), "Failed to connect"
        time.sleep(0.2)  # Give subscription time to register

        # Publisher
        pub_client = mqtt.Client()
        pub_client.connect(mosquitto_server['host'], mosquitto_server['port'])

        # Publish message
        test_message = {"job_id": "test.job", "status": "running"}
        pub_client.publish("test/topic", json.dumps(test_message))

        # Wait for message
        time.sleep(0.5)

        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert len(received) == 1
        assert received[0] == test_message

    def test_wildcard_subscription(self, mosquitto_server):
        """Can use wildcard subscriptions."""
        received = []
        connected = threading.Event()

        def on_connect(client, userdata, flags, rc):
            client.subscribe("linearjc/jobs/+/progress")
            connected.set()

        def on_message(client, userdata, msg):
            received.append({
                'topic': msg.topic,
                'payload': json.loads(msg.payload.decode())
            })

        sub_client = mqtt.Client()
        sub_client.on_connect = on_connect
        sub_client.on_message = on_message
        sub_client.connect(mosquitto_server['host'], mosquitto_server['port'])
        sub_client.loop_start()

        assert connected.wait(timeout=2.0)
        time.sleep(0.2)

        pub_client = mqtt.Client()
        pub_client.connect(mosquitto_server['host'], mosquitto_server['port'])

        # Publish to multiple job topics
        pub_client.publish("linearjc/jobs/job1/progress", json.dumps({"state": "running"}))
        pub_client.publish("linearjc/jobs/job2/progress", json.dumps({"state": "completed"}))

        time.sleep(0.5)

        sub_client.loop_stop()
        sub_client.disconnect()
        pub_client.disconnect()

        assert len(received) == 2
        topics = [r['topic'] for r in received]
        assert "linearjc/jobs/job1/progress" in topics
        assert "linearjc/jobs/job2/progress" in topics


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
