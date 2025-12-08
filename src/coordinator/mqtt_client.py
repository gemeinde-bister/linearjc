"""
MQTT client for LinearJC coordinator.

Handles MQTT communication:
- Publishing job requests
- Subscribing to capability announcements
- Subscribing to progress updates
- Message signing/verification
- Auto-reconnect with exponential backoff
- Message queueing during disconnections
"""
import json
import logging
import queue
import threading
import time
from typing import Dict, Any, Callable, Optional, Tuple
import paho.mqtt.client as mqtt

from coordinator.message_signing import sign_message, verify_message, MessageSigningError
from coordinator.models import CoordinatorConfig

logger = logging.getLogger(__name__)


class MqttClientError(Exception):
    """Error during MQTT operations."""
    pass


class MqttClient:
    """
    MQTT client wrapper for coordinator.

    Handles connection, publishing, subscribing with automatic signing.
    """

    def __init__(
        self,
        config: CoordinatorConfig.MqttConfig,
        signing_config: CoordinatorConfig.SigningConfig,
        client_id: str = "linearjc-coordinator"
    ):
        """
        Initialize MQTT client.

        Args:
            config: MQTT configuration
            signing_config: Message signing configuration
            client_id: MQTT client ID
        """
        self.config = config
        self.signing_config = signing_config
        self.client_id = client_id
        self.client: Optional[mqtt.Client] = None
        self.connected = False

        # Callback handlers
        self.capability_handlers: list[Callable[[Dict[str, Any]], None]] = []
        self.capability_response_handlers: list[Callable[[str, Dict[str, Any]], None]] = []
        self.progress_handlers: list[Callable[[str, Dict[str, Any]], None]] = []
        self.developer_deploy_request_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_deploy_complete_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_registry_sync_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_exec_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_tail_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_status_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_ps_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_logs_handlers: list[Callable[[Dict[str, Any], str], None]] = []
        self.developer_kill_handlers: list[Callable[[Dict[str, Any], str], None]] = []

        # Reconnection support
        self.should_reconnect = True
        self.reconnect_delay = 1  # Start with 1 second
        self.max_reconnect_delay = 60  # Max 60 seconds
        self.reconnect_thread: Optional[threading.Thread] = None

        # Message queueing for disconnections
        self.message_queue: queue.Queue[Tuple[str, str, int]] = queue.Queue(maxsize=1000)

    def connect(self) -> None:
        """
        Connect to MQTT broker.

        Raises:
            MqttClientError: If connection fails
        """
        try:
            logger.info(
                f"Connecting to MQTT broker: "
                f"{self.config.broker}:{self.config.port}"
            )

            # Create MQTT client with persistent session
            self.client = mqtt.Client(client_id=self.client_id, clean_session=False)

            # Set callbacks
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message

            # Connect to broker
            self.client.connect(
                self.config.broker,
                self.config.port,
                self.config.keepalive
            )

            # Start network loop in background
            self.client.loop_start()

            # Wait for connection
            timeout = 10  # seconds
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)

            if not self.connected:
                raise MqttClientError("Connection timeout")

            logger.info("Connected to MQTT broker successfully")

        except Exception as e:
            raise MqttClientError(f"Failed to connect to MQTT broker: {e}")

    def disconnect(self) -> None:
        """Disconnect from MQTT broker."""
        if self.client:
            logger.info("Disconnecting from MQTT broker")
            self.should_reconnect = False  # Disable auto-reconnect
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Dict[str, Any],
        rc: int
    ) -> None:
        """Callback when connected to broker."""
        if rc == 0:
            logger.info("MQTT connection established")
            self.connected = True

            # Subscribe to topics
            self._subscribe_to_topics()
        else:
            logger.error(f"MQTT connection failed with code: {rc}")

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        rc: int
    ) -> None:
        """Callback when disconnected from broker."""
        self.connected = False
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnect (code: {rc}), will attempt reconnect")
            self._start_reconnect()
        else:
            logger.info("MQTT disconnected cleanly")

    def _subscribe_to_topics(self) -> None:
        """Subscribe to all relevant topics."""
        # Subscribe to capability announcements (wildcard for all executors)
        self.client.subscribe("linearjc/capabilities/+")
        logger.info("Subscribed to: linearjc/capabilities/+")

        # Subscribe to all progress updates (wildcard for all executions)
        self.client.subscribe("linearjc/jobs/progress/+")
        logger.info("Subscribed to: linearjc/jobs/progress/+")

        # Subscribe to developer API topics (deploy requests)
        self.client.subscribe(f"linearjc/deploy/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/deploy/request/{self.client_id}")

        self.client.subscribe(f"linearjc/deploy/complete/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/deploy/complete/{self.client_id}")

        # Subscribe to registry sync requests
        self.client.subscribe(f"linearjc/registry/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/registry/request/{self.client_id}")

        # Subscribe to dev exec requests
        self.client.subscribe(f"linearjc/dev/exec/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/dev/exec/request/{self.client_id}")

        # Subscribe to dev tail requests
        self.client.subscribe(f"linearjc/dev/tail/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/dev/tail/request/{self.client_id}")

        # Subscribe to dev status requests
        self.client.subscribe(f"linearjc/dev/status/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/dev/status/request/{self.client_id}")

        # Subscribe to dev ps requests
        self.client.subscribe(f"linearjc/dev/ps/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/dev/ps/request/{self.client_id}")

        # Subscribe to dev logs requests
        self.client.subscribe(f"linearjc/dev/logs/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/dev/logs/request/{self.client_id}")

        # Subscribe to dev kill requests
        self.client.subscribe(f"linearjc/dev/kill/request/{self.client_id}")
        logger.info(f"Subscribed to: linearjc/dev/kill/request/{self.client_id}")

    def _start_reconnect(self) -> None:
        """Start reconnection attempts in background thread."""
        if not self.should_reconnect:
            return

        if self.reconnect_thread is None or not self.reconnect_thread.is_alive():
            self.reconnect_thread = threading.Thread(
                target=self._reconnect_loop,
                daemon=True,
                name="mqtt-reconnect"
            )
            self.reconnect_thread.start()
            logger.info("Started MQTT reconnection thread")

    def _reconnect_loop(self) -> None:
        """Attempt to reconnect with exponential backoff."""
        delay = self.reconnect_delay

        while self.should_reconnect and not self.connected:
            logger.info(f"Attempting MQTT reconnect in {delay}s...")
            time.sleep(delay)

            try:
                logger.info("Reconnecting to MQTT broker...")
                self.client.reconnect()
                logger.info("MQTT reconnection successful!")

                # Reset delay on successful reconnect
                self.reconnect_delay = 1

                # Flush queued messages
                self._flush_message_queue()
                break

            except Exception as e:
                logger.warning(f"MQTT reconnect failed: {e}")
                # Exponential backoff with max limit
                delay = min(delay * 2, self.max_reconnect_delay)

    def _flush_message_queue(self) -> None:
        """Publish queued messages after reconnection."""
        queue_size = self.message_queue.qsize()

        if queue_size == 0:
            return

        logger.info(f"Flushing {queue_size} queued messages...")

        flushed = 0
        failed = 0

        while not self.message_queue.empty():
            try:
                topic, payload, qos = self.message_queue.get_nowait()

                result = self.client.publish(topic, payload, qos=qos)

                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    flushed += 1
                else:
                    failed += 1
                    logger.error(f"Failed to flush message to {topic}: {result.rc}")

            except queue.Empty:
                break
            except Exception as e:
                failed += 1
                logger.error(f"Error flushing queued message: {e}")

        logger.info(f"Flushed {flushed} messages, {failed} failed")

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        msg: mqtt.MQTTMessage
    ) -> None:
        """Callback when message received."""
        topic = msg.topic
        payload = msg.payload.decode('utf-8')

        logger.debug(f"Received message on {topic}: {len(payload)} bytes")

        try:
            # Parse JSON envelope
            envelope = json.loads(payload)

            # Verify signature and extract payload
            try:
                message = verify_message(
                    envelope,
                    self.signing_config.shared_secret,
                    max_age_seconds=60
                )
            except MessageSigningError as e:
                logger.warning(f"Message verification failed on {topic}: {e}")
                return

            # Route to appropriate handler
            if topic.startswith("linearjc/capabilities/"):
                executor_id = topic.split("/")[-1]
                self._handle_capability_response(executor_id, message)
            elif topic.startswith("linearjc/jobs/progress/"):
                job_execution_id = topic.split("/")[-1]
                self._handle_progress(job_execution_id, message)
            elif topic.startswith("linearjc/deploy/request/"):
                # Developer API: deploy request
                # Extract client_id from message for response routing
                client_id = message.get('client_id', 'unknown')
                # Note: Handlers will re-verify signature for security
                self._handle_developer_deploy_request(envelope, client_id)
            elif topic.startswith("linearjc/deploy/complete/"):
                # Developer API: deploy complete
                # Extract client_id from message for response routing
                client_id = message.get('client_id', 'unknown')
                # Note: Handlers will re-verify signature for security
                self._handle_developer_deploy_complete(envelope, client_id)
            elif topic.startswith("linearjc/registry/request/"):
                # Developer API: registry sync request
                client_id = message.get('client_id', 'unknown')
                self._handle_developer_registry_sync(envelope, client_id)
            elif topic.startswith("linearjc/dev/exec/request/"):
                # Developer API: exec request
                client_id = message.get('client_id', 'unknown')
                self._handle_developer_exec_request(envelope, client_id)
            elif topic.startswith("linearjc/dev/tail/request/"):
                # Developer API: tail request
                client_id = message.get('client_id', 'unknown')
                self._handle_developer_tail_request(envelope, client_id)
            elif topic.startswith("linearjc/dev/status/request/"):
                # Developer API: status request
                client_id = message.get('client_id', 'unknown')
                self._handle_developer_status_request(envelope, client_id)
            elif topic.startswith("linearjc/dev/ps/request/"):
                # Developer API: ps request
                client_id = message.get('client_id', 'unknown')
                self._handle_developer_ps_request(envelope, client_id)
            elif topic.startswith("linearjc/dev/logs/request/"):
                # Developer API: logs request
                client_id = message.get('client_id', 'unknown')
                self._handle_developer_logs_request(envelope, client_id)
            elif topic.startswith("linearjc/dev/kill/request/"):
                # Developer API: kill request
                client_id = message.get('client_id', 'unknown')
                self._handle_developer_kill_request(envelope, client_id)
            else:
                logger.warning(f"Unknown topic: {topic}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message on {topic}: {e}")
        except Exception as e:
            logger.error(f"Error handling message on {topic}: {e}")

    def _handle_capability_response(self, executor_id: str, message: Dict[str, Any]) -> None:
        """Handle capability response from executor."""
        capabilities = message.get('capabilities', [])

        logger.debug(
            f"Received capability response from {executor_id}: "
            f"{len(capabilities)} jobs"
        )

        # Call new-style capability response handlers (with executor_id)
        for handler in self.capability_response_handlers:
            try:
                handler(executor_id, message)
            except Exception as e:
                logger.error(f"Capability response handler error: {e}")

        # Call old-style capability handlers (backward compatibility)
        for handler in self.capability_handlers:
            try:
                handler(message)
            except Exception as e:
                logger.error(f"Capability handler error: {e}")

    def _handle_progress(
        self,
        job_execution_id: str,
        message: Dict[str, Any]
    ) -> None:
        """Handle progress update."""
        state = message.get('state', 'unknown')
        executor_id = message.get('executor_id', 'unknown')

        logger.info(
            f"Progress update for {job_execution_id}: "
            f"state={state}, executor={executor_id}"
        )

        # Call registered handlers
        for handler in self.progress_handlers:
            try:
                handler(job_execution_id, message)
            except Exception as e:
                logger.error(f"Progress handler error: {e}")

    def _handle_developer_deploy_request(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle deploy request from developer."""
        logger.info(f"Deploy request from developer: {client_id}")

        # Call registered handlers
        for handler in self.developer_deploy_request_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer deploy request handler error: {e}")

    def _handle_developer_deploy_complete(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle deploy complete from developer."""
        logger.info(f"Deploy complete from developer: {client_id}")

        # Call registered handlers
        for handler in self.developer_deploy_complete_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer deploy complete handler error: {e}")

    def _handle_developer_registry_sync(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle registry sync request from developer."""
        logger.info(f"Registry sync request from developer: {client_id}")

        # Call registered handlers
        for handler in self.developer_registry_sync_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer registry sync handler error: {e}")

    def _handle_developer_exec_request(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle exec request from developer."""
        logger.info(f"Exec request from developer: {client_id}")

        # Call registered handlers
        for handler in self.developer_exec_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer exec handler error: {e}")

    def _handle_developer_tail_request(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle tail request from developer."""
        logger.info(f"Tail request from developer: {client_id}")

        # Call registered handlers
        for handler in self.developer_tail_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer tail handler error: {e}")

    def _handle_developer_status_request(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle status request from developer."""
        logger.info(f"Status request from developer: {client_id}")

        for handler in self.developer_status_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer status handler error: {e}")

    def _handle_developer_ps_request(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle ps request from developer."""
        logger.info(f"PS request from developer: {client_id}")

        for handler in self.developer_ps_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer ps handler error: {e}")

    def _handle_developer_logs_request(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle logs request from developer."""
        logger.info(f"Logs request from developer: {client_id}")

        for handler in self.developer_logs_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer logs handler error: {e}")

    def _handle_developer_kill_request(
        self,
        envelope: Dict[str, Any],
        client_id: str
    ) -> None:
        """Handle kill request from developer."""
        logger.info(f"Kill request from developer: {client_id}")

        for handler in self.developer_kill_handlers:
            try:
                handler(envelope, client_id)
            except Exception as e:
                logger.error(f"Developer kill handler error: {e}")

    def publish_job_request(
        self,
        job_execution_id: str,
        job_request: Dict[str, Any]
    ) -> None:
        """
        Publish a job request to MQTT.

        The message will be automatically signed. If disconnected, the message
        will be queued and sent upon reconnection.

        Args:
            job_execution_id: Unique execution ID
            job_request: Job request message (without signature)

        Raises:
            MqttClientError: If queue is full
        """
        # Sign message
        signed_message = sign_message(
            dict(job_request),
            self.signing_config.shared_secret
        )

        # Convert to JSON
        payload = json.dumps(signed_message)

        # Publish to topic
        topic = f"linearjc/jobs/requests/{job_execution_id}"

        if not self.connected:
            # Queue message for later delivery
            try:
                self.message_queue.put_nowait((topic, payload, 1))
                logger.warning(f"Not connected, queued job request: {job_execution_id}")
                return
            except queue.Full:
                raise MqttClientError("Message queue is full, cannot queue job request")

        logger.info(f"Publishing job request to {topic}")
        logger.debug(f"Job request: {job_request.get('job_id')} v{job_request.get('job_version')}")

        result = self.client.publish(topic, payload, qos=1)

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise MqttClientError(f"Failed to publish job request: {result.rc}")

        logger.info(f"Published job request: {job_execution_id}")

    def publish_message(
        self,
        topic: str,
        message: Dict[str, Any],
        qos: int = 1
    ) -> None:
        """
        Publish a generic message to MQTT.

        The message will be automatically signed. If disconnected, the message
        will be queued and sent upon reconnection.

        Args:
            topic: MQTT topic
            message: Message content (without signature)
            qos: Quality of service (0, 1, or 2)

        Raises:
            MqttClientError: If queue is full
        """
        # Sign message
        signed_message = sign_message(
            dict(message),
            self.signing_config.shared_secret
        )

        # Convert to JSON
        payload = json.dumps(signed_message)

        if not self.connected:
            # Queue message for later delivery
            try:
                self.message_queue.put_nowait((topic, payload, qos))
                logger.warning(f"Not connected, queued message to {topic}")
                return
            except queue.Full:
                raise MqttClientError("Message queue is full, cannot queue message")

        logger.debug(f"Publishing to {topic}: {len(payload)} bytes")

        result = self.client.publish(topic, payload, qos=qos)

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise MqttClientError(f"Failed to publish to {topic}: {result.rc}")

    def register_capability_handler(
        self,
        handler: Callable[[Dict[str, Any]], None]
    ) -> None:
        """
        Register a handler for capability announcements (legacy).

        Args:
            handler: Callback function receiving capability message
        """
        self.capability_handlers.append(handler)
        logger.debug("Registered capability handler")

    def register_capability_response_handler(
        self,
        handler: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        """
        Register a handler for capability responses (poll-based).

        Args:
            handler: Callback receiving (executor_id, response_message)
        """
        self.capability_response_handlers.append(handler)
        logger.debug("Registered capability response handler")

    def register_progress_handler(
        self,
        handler: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        """
        Register a handler for progress updates.

        Args:
            handler: Callback receiving (job_execution_id, progress_message)
        """
        self.progress_handlers.append(handler)
        logger.debug("Registered progress handler")

    def register_developer_deploy_request_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer deploy requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_deploy_request_handlers.append(handler)
        logger.debug("Registered developer deploy request handler")

    def register_developer_deploy_complete_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer deploy completion notifications.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_deploy_complete_handlers.append(handler)
        logger.debug("Registered developer deploy complete handler")

    def register_developer_registry_sync_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer registry sync requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_registry_sync_handlers.append(handler)
        logger.debug("Registered developer registry sync handler")

    def register_developer_exec_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer exec requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_exec_handlers.append(handler)
        logger.debug("Registered developer exec handler")

    def register_developer_tail_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer tail requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_tail_handlers.append(handler)
        logger.debug("Registered developer tail handler")

    def register_developer_status_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer status requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_status_handlers.append(handler)
        logger.debug("Registered developer status handler")

    def register_developer_ps_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer ps requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_ps_handlers.append(handler)
        logger.debug("Registered developer ps handler")

    def register_developer_logs_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer logs requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_logs_handlers.append(handler)
        logger.debug("Registered developer logs handler")

    def register_developer_kill_handler(
        self,
        handler: Callable[[Dict[str, Any], str], None]
    ) -> None:
        """
        Register a handler for developer kill requests.

        Args:
            handler: Callback receiving (envelope, client_id)
        """
        self.developer_kill_handlers.append(handler)
        logger.debug("Registered developer kill handler")

    def wait_for_messages(self, duration_seconds: float) -> None:
        """
        Wait for incoming messages for specified duration.

        Args:
            duration_seconds: How long to wait
        """
        logger.debug(f"Waiting for messages for {duration_seconds}s")
        time.sleep(duration_seconds)

    def is_connected(self) -> bool:
        """Check if connected to broker."""
        return self.connected
