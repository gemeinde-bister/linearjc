"""
Developer API for LinearJC coordinator.

Handles MQTT requests from developer tools (ljc):
- Deploy requests (presigned MinIO URLs)
- Deploy completion (download and install)
- Registry sync requests
- Exec requests (immediate job execution)
- Status queries (job scheduling state)
- PS queries (active job list)
- Logs queries (execution history)
- Kill requests (job cancellation)

All messages are signed with HMAC-SHA256 and verified before processing.
"""
import logging
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, Optional, Callable, List, TYPE_CHECKING

from coordinator.message_signing import verify_and_extract, MessageSigningError
from coordinator.minio_manager import MinioManager
from coordinator.job_tracker import JobState

if TYPE_CHECKING:
    from coordinator.job_tracker import JobTracker

logger = logging.getLogger(__name__)


class DeveloperAPIError(Exception):
    """Error in developer API operations."""
    pass


class DeveloperAPI:
    """
    Handles developer tool requests via MQTT.

    Workflow:
    1. Developer sends deploy request (signed)
    2. Coordinator generates presigned MinIO URL
    3. Developer uploads package to MinIO
    4. Developer sends deploy-complete notification (signed)
    5. Coordinator downloads, verifies, and installs package
    6. Coordinator sends result (signed)
    """

    def __init__(
        self,
        minio_manager: MinioManager,
        mqtt_client,
        shared_secret: str,
        coordinator_id: str,
        install_callback,
        exec_callback: Optional[Callable[[Any, Optional[str]], Dict[str, Any]]] = None,
    ):
        """
        Initialize developer API.

        Args:
            minio_manager: MinIO manager for presigned URLs
            mqtt_client: MQTT client for publishing responses
            shared_secret: Shared secret for message signing
            coordinator_id: This coordinator's ID
            install_callback: Function to call to install packages (coordinator.install_package)
            exec_callback: Function to execute a tree (tree, dev_client_id) -> {tree_execution_id, job_execution_id}
        """
        self.minio = minio_manager
        self.mqtt = mqtt_client
        self.secret = shared_secret
        self.coordinator_id = coordinator_id
        self.install_callback = install_callback
        self.exec_callback = exec_callback

        # Track pending deployments (request_id -> metadata)
        self.pending_deployments: Dict[str, Dict[str, Any]] = {}

    def handle_deploy_request(self, envelope: Dict[str, Any], client_id: str) -> None:
        """
        Handle deploy request from developer.

        Request format:
        {
            "request_id": "uuid",
            "action": "request_upload_url",
            "job_id": "backup.pool",
            "package_size": 2048,
            "checksum_sha256": "abc123..."
        }

        Response:
        {
            "request_id": "uuid",
            "upload_url": "http://minio:9000/...",
            "expires_in": 600
        }
        """
        try:
            # Verify signature
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            # Extract fields
            request_id = payload.get('request_id')
            job_id = payload.get('job_id')
            package_size = payload.get('package_size')
            checksum = payload.get('checksum_sha256')

            if not all([request_id, job_id, package_size, checksum]):
                raise DeveloperAPIError("Missing required fields in deploy request")

            logger.info(
                f"Deploy request from {client_id}: job_id={job_id}, "
                f"size={package_size}, request_id={request_id}"
            )

            # Generate presigned upload URL (10 minute expiry)
            object_key = f"deployments/{job_id}-{request_id}.ljc"
            upload_url = self.minio.generate_presigned_put_url(
                bucket='linearjc',
                object_name=object_key,
                expires_seconds=600
            )

            # Store deployment metadata
            self.pending_deployments[request_id] = {
                'job_id': job_id,
                'object_key': object_key,
                'checksum': checksum,
                'client_id': client_id
            }

            # Send response
            response = {
                'request_id': request_id,
                'upload_url': upload_url,
                'expires_in': 600
            }

            self._send_response(client_id, response)

            logger.info(f"Sent upload URL to {client_id} (request_id={request_id})")

        except MessageSigningError as e:
            logger.error(f"Message verification failed from {client_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling deploy request from {client_id}: {e}")

    def handle_deploy_complete(self, envelope: Dict[str, Any], client_id: str) -> None:
        """
        Handle deploy completion notification from developer.

        Request format:
        {
            "request_id": "uuid",
            "action": "deploy_complete",
            "job_id": "backup.pool",
            "checksum_sha256": "abc123..."
        }

        Response:
        {
            "request_id": "uuid",
            "status": "installed" | "failed",
            "job_id": "backup.pool",
            "version": "1.0.0",
            "error": "..." (if failed)
        }
        """
        try:
            # Verify signature
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            # Extract fields
            request_id = payload.get('request_id')
            job_id = payload.get('job_id')
            checksum = payload.get('checksum_sha256')

            if not all([request_id, job_id, checksum]):
                raise DeveloperAPIError("Missing required fields in deploy complete")

            logger.info(
                f"Deploy complete from {client_id}: job_id={job_id}, "
                f"request_id={request_id}"
            )

            # Check if we have this deployment
            if request_id not in self.pending_deployments:
                logger.error(f"Unknown deployment request_id: {request_id}")
                self._send_error_response(client_id, request_id, "Unknown deployment")
                return

            deployment = self.pending_deployments[request_id]

            # Verify job_id and checksum match
            if deployment['job_id'] != job_id:
                logger.error(f"Job ID mismatch: expected {deployment['job_id']}, got {job_id}")
                self._send_error_response(client_id, request_id, "Job ID mismatch")
                return

            if deployment['checksum'] != checksum:
                logger.error(f"Checksum mismatch for {job_id}")
                self._send_error_response(client_id, request_id, "Checksum mismatch")
                return

            # Download package from MinIO
            logger.info(f"Downloading package from MinIO: {deployment['object_key']}")

            try:
                package_data = self.minio.download_object_to_bytes(
                    bucket='linearjc',
                    object_name=deployment['object_key']
                )
            except Exception as e:
                logger.error(f"Failed to download package from MinIO: {e}")
                self._send_error_response(client_id, request_id, f"MinIO download failed: {e}")
                return

            # Verify checksum
            import hashlib
            computed_checksum = hashlib.sha256(package_data).hexdigest()
            if computed_checksum != checksum:
                logger.error(f"Checksum verification failed after download")
                self._send_error_response(client_id, request_id, "Checksum verification failed")
                return

            logger.info(f"Checksum verified: {checksum[:16]}...")

            # Save to temp file and install
            try:
                with tempfile.NamedTemporaryFile(suffix='.ljc', delete=False) as tmp:
                    tmp.write(package_data)
                    tmp_path = Path(tmp.name)

                logger.info(f"Installing package from {tmp_path}")

                # Call install callback (coordinator's install_package function)
                result = self.install_callback(tmp_path)

                # Clean up temp file
                tmp_path.unlink()

                # Send success response
                response = {
                    'request_id': request_id,
                    'status': 'installed',
                    'job_id': result.get('job_id', job_id),
                    'version': result.get('version', 'unknown')
                }

                self._send_response(client_id, response)

                logger.info(
                    f"Successfully installed {job_id} v{result.get('version')} "
                    f"for {client_id}"
                )

                # Clean up pending deployment
                del self.pending_deployments[request_id]

            except Exception as e:
                logger.error(f"Installation failed: {e}")
                self._send_error_response(client_id, request_id, str(e))

        except MessageSigningError as e:
            logger.error(f"Message verification failed from {client_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling deploy complete from {client_id}: {e}")

    def _send_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send signed response to developer."""
        topic = f"linearjc/deploy/response/{client_id}"

        try:
            # publish_message will sign the message, so don't pre-sign it
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send response to {client_id}: {e}")

    def _send_error_response(
        self,
        client_id: str,
        request_id: str,
        error: str
    ) -> None:
        """Send error response to developer."""
        response = {
            'request_id': request_id,
            'status': 'failed',
            'error': error
        }
        self._send_response(client_id, response)

    def handle_registry_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        data_registry: Dict[str, Any],
        save_registry_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> None:
        """
        Handle registry request from developer (sync or push).

        Routes to appropriate handler based on 'action' field.
        """
        try:
            # Verify signature
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)
            action = payload.get('action', 'registry_sync')

            if action == 'registry_push':
                self._handle_registry_push(payload, client_id, data_registry, save_registry_callback)
            else:
                self._handle_registry_sync(payload, client_id, data_registry)

        except MessageSigningError as e:
            logger.error(f"Message verification failed from {client_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling registry request from {client_id}: {e}")
            # Try to send error response
            try:
                request_id = envelope.get('payload', {})
                if isinstance(request_id, str):
                    import json
                    request_id = json.loads(request_id).get('request_id', 'unknown')
                else:
                    request_id = request_id.get('request_id', 'unknown')
                self._send_registry_error_response(client_id, request_id, str(e))
            except Exception:
                pass  # Best effort

    def _handle_registry_sync(
        self,
        payload: Dict[str, Any],
        client_id: str,
        data_registry: Dict[str, Any]
    ) -> None:
        """
        Handle registry sync (pull) request from developer.

        Request format:
        {
            "request_id": "uuid",
            "action": "registry_sync",
            "client_id": "developer-alice"
        }

        Response:
        {
            "request_id": "uuid",
            "status": "success",
            "registry": {...},
            "entry_count": N
        }
        """
        request_id = payload.get('request_id')

        if not request_id:
            raise DeveloperAPIError("Missing request_id in registry sync request")

        logger.info(
            f"Registry sync request from {client_id}: request_id={request_id}"
        )

        # Serialize registry entries to dict format
        registry_dict = {}
        for key, entry in data_registry.items():
            # Convert pydantic model to dict, excluding None values
            entry_dict = entry.model_dump(exclude_none=True)
            registry_dict[key] = entry_dict

        # Send response
        response = {
            'request_id': request_id,
            'status': 'success',
            'registry': registry_dict,
            'entry_count': len(registry_dict)
        }

        self._send_registry_response(client_id, response)

        logger.info(
            f"Sent registry ({len(registry_dict)} entries) to {client_id}"
        )

    def _handle_registry_push(
        self,
        payload: Dict[str, Any],
        client_id: str,
        data_registry: Dict[str, Any],
        save_registry_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> None:
        """
        Handle registry push request from developer.

        Request format:
        {
            "request_id": "uuid",
            "action": "registry_push",
            "client_id": "developer-alice",
            "registry": {
                "my_input": {"type": "fs", "path": "/var/...", "kind": "file"},
                ...
            },
            "entry_count": N
        }

        Response:
        {
            "request_id": "uuid",
            "status": "success",
            "added": N,
            "updated": M,
            "total": T
        }
        """
        from coordinator.models import DataRegistryEntry

        request_id = payload.get('request_id')
        incoming_registry = payload.get('registry', {})

        if not request_id:
            raise DeveloperAPIError("Missing request_id in registry push request")

        logger.info(
            f"Registry push request from {client_id}: request_id={request_id}, "
            f"entries={len(incoming_registry)}"
        )

        added = 0
        updated = 0

        # Merge incoming entries into data_registry
        for key, entry_data in incoming_registry.items():
            try:
                # Parse and validate entry
                new_entry = DataRegistryEntry(**entry_data)

                if key in data_registry:
                    # Check if entry changed
                    existing = data_registry[key]
                    if existing.model_dump(exclude_none=True) != new_entry.model_dump(exclude_none=True):
                        data_registry[key] = new_entry
                        updated += 1
                        logger.info(f"Updated registry entry: {key}")
                else:
                    data_registry[key] = new_entry
                    added += 1
                    logger.info(f"Added registry entry: {key}")

            except Exception as e:
                logger.error(f"Invalid registry entry '{key}': {e}")
                # Continue processing other entries

        # Save registry if callback provided and changes were made
        if (added > 0 or updated > 0) and save_registry_callback:
            try:
                save_registry_callback(data_registry)
                logger.info(f"Registry saved: {added} added, {updated} updated")
            except Exception as e:
                logger.error(f"Failed to save registry: {e}")
                self._send_registry_error_response(client_id, request_id, f"Failed to save registry: {e}")
                return

        # Send response
        response = {
            'request_id': request_id,
            'status': 'success',
            'added': added,
            'updated': updated,
            'total': len(data_registry)
        }

        self._send_registry_response(client_id, response)

        logger.info(
            f"Registry push complete from {client_id}: {added} added, {updated} updated, "
            f"total {len(data_registry)} entries"
        )

    # Keep old method name for backwards compatibility
    def handle_registry_sync_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        data_registry: Dict[str, Any],
        save_registry_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> None:
        """Backwards compatible wrapper for handle_registry_request."""
        self.handle_registry_request(envelope, client_id, data_registry, save_registry_callback)

    def _send_registry_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send signed registry sync response to developer."""
        topic = f"linearjc/registry/response/{client_id}"

        try:
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send registry response to {client_id}: {e}")

    def _send_registry_error_response(
        self,
        client_id: str,
        request_id: str,
        error: str
    ) -> None:
        """Send error response for registry sync."""
        response = {
            'request_id': request_id,
            'status': 'failed',
            'error': error
        }
        self._send_registry_response(client_id, response)

    def handle_exec_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        trees: List[Any]
    ) -> None:
        """
        Handle exec request from developer (immediate job execution).

        Request format:
        {
            "request_id": "uuid",
            "action": "exec_job",
            "client_id": "developer-alice",
            "job_id": "backup.pool",
            "follow": true
        }

        Response:
        {
            "request_id": "uuid",
            "status": "accepted" | "rejected" | "error",
            "job_execution_id": "...",
            "tree_execution_id": "...",
            "error": "..." (if rejected/error)
        }
        """
        try:
            # Verify signature
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            # Extract fields
            request_id = payload.get('request_id')
            job_id = payload.get('job_id')
            follow = payload.get('follow', False)

            if not all([request_id, job_id]):
                raise DeveloperAPIError("Missing required fields in exec request")

            logger.info(
                f"Exec request from {client_id}: job_id={job_id}, "
                f"follow={follow}, request_id={request_id}"
            )

            # Find the tree for this job
            tree = None
            for t in trees:
                if t.root.id == job_id:
                    tree = t
                    break

            if not tree:
                logger.error(f"Job not found: {job_id}")
                self._send_exec_response(client_id, {
                    'request_id': request_id,
                    'status': 'rejected',
                    'error': f"Job not found: {job_id}"
                })
                return

            # Check if exec callback is configured
            if not self.exec_callback:
                logger.error("Exec callback not configured")
                self._send_exec_response(client_id, {
                    'request_id': request_id,
                    'status': 'error',
                    'error': "Exec not supported by this coordinator"
                })
                return

            # Execute the tree
            # Pass client_id to exec_callback so it can be stored in JobExecution
            # BEFORE the MQTT publish - this prevents the race condition where
            # progress updates arrive before tracking is established
            try:
                dev_client_id = client_id if follow else None
                result = self.exec_callback(tree, dev_client_id)
                tree_execution_id = result.get('tree_execution_id')
                job_execution_id = result.get('job_execution_id')

                # Send accepted response
                response = {
                    'request_id': request_id,
                    'status': 'accepted',
                    'job_execution_id': job_execution_id,
                    'tree_execution_id': tree_execution_id,
                }

                self._send_exec_response(client_id, response)

                logger.info(
                    f"Exec accepted for {job_id}: "
                    f"job_execution_id={job_execution_id}"
                )

            except Exception as e:
                logger.error(f"Exec failed for {job_id}: {e}")
                self._send_exec_response(client_id, {
                    'request_id': request_id,
                    'status': 'error',
                    'error': str(e)
                })

        except MessageSigningError as e:
            logger.error(f"Message verification failed from {client_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling exec request from {client_id}: {e}")

    def _send_exec_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send signed exec response to developer."""
        topic = f"linearjc/dev/exec/response/{client_id}"

        try:
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send exec response to {client_id}: {e}")

    def handle_tail_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        job_tracker
    ) -> None:
        """
        Handle tail request from developer (attach to existing execution).

        Request format:
        {
            "request_id": "uuid",
            "action": "tail",
            "client_id": "developer-alice",
            "job_id": "backup.pool",        # Optional: find active execution
            "execution_id": "backup.pool-..." # Optional: attach directly
        }

        Response:
        {
            "request_id": "uuid",
            "status": "attached" | "not_found" | "error",
            "job_execution_id": "...",
            "job_id": "...",
            "current_state": "running",
            "executor_id": "...",
            "error": "..." (if not_found/error)
        }
        """
        try:
            # Verify signature
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            # Extract fields
            request_id = payload.get('request_id')
            job_id = payload.get('job_id')
            execution_id = payload.get('execution_id')

            if not request_id:
                raise DeveloperAPIError("Missing request_id in tail request")

            if not job_id and not execution_id:
                raise DeveloperAPIError("Must provide either job_id or execution_id")

            logger.info(
                f"Tail request from {client_id}: "
                f"job_id={job_id}, execution_id={execution_id}, "
                f"request_id={request_id}"
            )

            # Find the execution
            job_exec = None

            if execution_id:
                # Direct attachment by execution ID
                job_exec = job_tracker.get_job(execution_id)
                if not job_exec:
                    logger.warning(f"Execution not found: {execution_id}")
                    self._send_tail_response(client_id, {
                        'request_id': request_id,
                        'status': 'not_found',
                        'error': f"Execution not found: {execution_id}"
                    })
                    return
            else:
                # Find active execution by job ID
                job_exec = job_tracker.find_active_by_job_id(job_id)
                if not job_exec:
                    logger.warning(f"No active execution for job: {job_id}")
                    self._send_tail_response(client_id, {
                        'request_id': request_id,
                        'status': 'not_found',
                        'error': f"No active execution found for job: {job_id}"
                    })
                    return

            # Attach dev client for progress forwarding
            job_tracker.attach_dev_client(job_exec.job_execution_id, client_id)

            # Send attached response
            response = {
                'request_id': request_id,
                'status': 'attached',
                'job_execution_id': job_exec.job_execution_id,
                'job_id': job_exec.job_id,
                'current_state': job_exec.state.value,
                'executor_id': job_exec.executor_id,
            }

            self._send_tail_response(client_id, response)

            logger.info(
                f"Tail attached for {job_exec.job_id}: "
                f"execution={job_exec.job_execution_id}, "
                f"state={job_exec.state.value}"
            )

        except MessageSigningError as e:
            logger.error(f"Message verification failed from {client_id}: {e}")
        except DeveloperAPIError as e:
            logger.error(f"Tail request error from {client_id}: {e}")
            # Try to send error response
            try:
                request_id = payload.get('request_id', 'unknown') if 'payload' in dir() else 'unknown'
                self._send_tail_response(client_id, {
                    'request_id': request_id,
                    'status': 'error',
                    'error': str(e)
                })
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error handling tail request from {client_id}: {e}")

    def _send_tail_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send signed tail response to developer."""
        topic = f"linearjc/dev/tail/response/{client_id}"

        try:
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send tail response to {client_id}: {e}")

    # =========================================================================
    # Status Query Handler
    # =========================================================================

    def handle_status_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        trees: List[Any],
        job_tracker: 'JobTracker'
    ) -> None:
        """
        Handle status request from developer.

        Request format:
        {
            "request_id": "uuid",
            "action": "status",
            "client_id": "developer-alice",
            "job_id": "backup.pool",  # Optional if --all
            "all": false
        }

        Response:
        {
            "request_id": "uuid",
            "success": true,
            "data": { ... }
        }
        """
        try:
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            request_id = payload.get('request_id')
            job_id = payload.get('job_id')
            show_all = payload.get('all', False)

            if not request_id:
                raise DeveloperAPIError("Missing request_id in status request")

            logger.info(
                f"Status request from {client_id}: job_id={job_id}, all={show_all}"
            )

            if job_id:
                data = self._get_job_status(job_id, trees, job_tracker)
            elif show_all:
                data = self._get_all_jobs_status(trees, job_tracker)
            else:
                raise DeveloperAPIError("Specify job_id or use --all")

            response = {
                'request_id': request_id,
                'success': True,
                'data': data,
            }
            self._send_status_response(client_id, response)

        except MessageSigningError as e:
            logger.error(f"Status verification failed from {client_id}: {e}")
        except DeveloperAPIError as e:
            logger.error(f"Status request error from {client_id}: {e}")
            try:
                request_id = payload.get('request_id', 'unknown') if 'payload' in dir() else 'unknown'
                self._send_status_response(client_id, {
                    'request_id': request_id,
                    'success': False,
                    'error': str(e),
                })
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error handling status request from {client_id}: {e}")

    def _get_job_status(
        self,
        job_id: str,
        trees: List[Any],
        job_tracker: 'JobTracker'
    ) -> Dict[str, Any]:
        """Get status for a single job."""
        tree = None
        for t in trees:
            if t.root.id == job_id:
                tree = t
                break

        if not tree:
            raise DeveloperAPIError(f"Job not found: {job_id}")

        now = time.time()
        active_exec = None

        # Check for active execution
        terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
        for job_exec in job_tracker.get_all_jobs().values():
            if job_exec.job_id == job_id and job_exec.state not in terminal_states:
                active_exec = {
                    'execution_id': job_exec.job_execution_id,
                    'state': job_exec.state.value,
                    'executor': job_exec.executor_id,
                    'duration': now - job_exec.assigned_at if job_exec.assigned_at else 0,
                    'timeout_remaining': job_exec.timeout_at - now if job_exec.timeout_at else None,
                }
                break

        return {
            'job_id': tree.root.id,
            'version': tree.root.version,
            'jobs_in_chain': len(tree.jobs),
            'schedule': {
                'min_daily': tree.min_daily,
                'max_daily': tree.max_daily,
            },
            'last_execution': tree.last_execution,
            'next_execution': tree.next_execution,
            'executions_24h': tree.get_executions_last_24h(now),
            'active_execution': active_exec,
        }

    def _get_all_jobs_status(
        self,
        trees: List[Any],
        job_tracker: 'JobTracker'
    ) -> Dict[str, Any]:
        """Get status for all jobs."""
        jobs = []
        for tree in trees:
            try:
                status = self._get_job_status(tree.root.id, trees, job_tracker)
                jobs.append(status)
            except Exception as e:
                logger.warning(f"Failed to get status for {tree.root.id}: {e}")

        return {'jobs': jobs}

    def _send_status_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send status response."""
        topic = f"linearjc/dev/status/response/{client_id}"
        try:
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send status response to {client_id}: {e}")

    # =========================================================================
    # PS Query Handler
    # =========================================================================

    def handle_ps_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        job_tracker: 'JobTracker'
    ) -> None:
        """
        Handle ps request from developer (list active jobs).

        Request format:
        {
            "request_id": "uuid",
            "action": "ps",
            "client_id": "developer-alice",
            "executor": "pool-executor-1",  # Optional filter
            "all": false  # Include completed jobs
        }
        """
        try:
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            request_id = payload.get('request_id')
            executor_filter = payload.get('executor')
            show_all = payload.get('all', False)

            if not request_id:
                raise DeveloperAPIError("Missing request_id in ps request")

            logger.info(
                f"PS request from {client_id}: executor={executor_filter}, all={show_all}"
            )

            terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
            jobs = []
            now = time.time()

            for job_exec in job_tracker.get_all_jobs().values():
                # Filter by state
                if not show_all and job_exec.state in terminal_states:
                    continue

                # Filter by executor
                if executor_filter and job_exec.executor_id != executor_filter:
                    continue

                jobs.append({
                    'execution_id': job_exec.job_execution_id,
                    'tree_execution_id': job_exec.tree_execution_id,
                    'job_id': job_exec.job_id,
                    'state': job_exec.state.value,
                    'executor': job_exec.executor_id,
                    'duration': now - job_exec.assigned_at if job_exec.assigned_at else 0,
                    'job_index': job_exec.job_index,
                })

            response = {
                'request_id': request_id,
                'success': True,
                'data': {'jobs': jobs},
            }
            self._send_ps_response(client_id, response)

        except MessageSigningError as e:
            logger.error(f"PS verification failed from {client_id}: {e}")
        except DeveloperAPIError as e:
            logger.error(f"PS request error from {client_id}: {e}")
            try:
                request_id = payload.get('request_id', 'unknown') if 'payload' in dir() else 'unknown'
                self._send_ps_response(client_id, {
                    'request_id': request_id,
                    'success': False,
                    'error': str(e),
                })
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error handling ps request from {client_id}: {e}")

    def _send_ps_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send ps response."""
        topic = f"linearjc/dev/ps/response/{client_id}"
        try:
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send ps response to {client_id}: {e}")

    # =========================================================================
    # Logs Query Handler
    # =========================================================================

    def handle_logs_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        trees: List[Any]
    ) -> None:
        """
        Handle logs request from developer (execution history).

        Request format:
        {
            "request_id": "uuid",
            "action": "logs",
            "client_id": "developer-alice",
            "job_id": "backup.pool",
            "last": 10,  # Number of executions
            "failed": false  # Only failed
        }
        """
        try:
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            request_id = payload.get('request_id')
            job_id = payload.get('job_id')
            last_n = payload.get('last', 10)
            failed_only = payload.get('failed', False)

            if not request_id:
                raise DeveloperAPIError("Missing request_id in logs request")
            if not job_id:
                raise DeveloperAPIError("Missing job_id in logs request")

            logger.info(
                f"Logs request from {client_id}: job_id={job_id}, last={last_n}"
            )

            # Find tree
            tree = None
            for t in trees:
                if t.root.id == job_id:
                    tree = t
                    break

            if not tree:
                raise DeveloperAPIError(f"Job not found: {job_id}")

            now = time.time()
            executions = []

            # Get execution history from tree (timestamps only)
            for timestamp in sorted(tree.execution_history, reverse=True)[:last_n]:
                age_hours = (now - timestamp) / 3600
                executions.append({
                    'timestamp': timestamp,
                    'age_hours': round(age_hours, 1),
                })

            response = {
                'request_id': request_id,
                'success': True,
                'data': {
                    'job_id': job_id,
                    'total_24h': tree.get_executions_last_24h(now),
                    'executions': executions,
                },
            }
            self._send_logs_response(client_id, response)

        except MessageSigningError as e:
            logger.error(f"Logs verification failed from {client_id}: {e}")
        except DeveloperAPIError as e:
            logger.error(f"Logs request error from {client_id}: {e}")
            try:
                request_id = payload.get('request_id', 'unknown') if 'payload' in dir() else 'unknown'
                self._send_logs_response(client_id, {
                    'request_id': request_id,
                    'success': False,
                    'error': str(e),
                })
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error handling logs request from {client_id}: {e}")

    def _send_logs_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send logs response."""
        topic = f"linearjc/dev/logs/response/{client_id}"
        try:
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send logs response to {client_id}: {e}")

    # =========================================================================
    # Kill Request Handler
    # =========================================================================

    def handle_kill_request(
        self,
        envelope: Dict[str, Any],
        client_id: str,
        job_tracker: 'JobTracker'
    ) -> None:
        """
        Handle kill request from developer (cancel running job).

        Request format:
        {
            "request_id": "uuid",
            "action": "kill",
            "client_id": "developer-alice",
            "execution_id": "backup.pool-20251204-...",
            "force": false  # SIGKILL instead of SIGTERM
        }
        """
        try:
            payload = verify_and_extract(envelope, self.secret, max_age_seconds=60)

            request_id = payload.get('request_id')
            execution_id = payload.get('execution_id')
            force = payload.get('force', False)

            if not request_id:
                raise DeveloperAPIError("Missing request_id in kill request")
            if not execution_id:
                raise DeveloperAPIError("Missing execution_id in kill request")

            logger.info(
                f"Kill request from {client_id}: execution_id={execution_id}, force={force}"
            )

            job_exec = job_tracker.get_job(execution_id)
            if not job_exec:
                raise DeveloperAPIError(f"Execution not found: {execution_id}")

            terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.TIMEOUT}
            if job_exec.state in terminal_states:
                raise DeveloperAPIError(
                    f"Job already terminated: {job_exec.state.value}"
                )

            if not job_exec.executor_id:
                raise DeveloperAPIError("Job not yet assigned to executor")

            # Publish cancel message to executor
            cancel_msg = {
                'execution_id': execution_id,
                'signal': 'SIGKILL' if force else 'SIGTERM',
            }

            cancel_topic = f"linearjc/executors/{job_exec.executor_id}/cancel"
            self.mqtt.publish_message(cancel_topic, cancel_msg, qos=1)

            logger.info(
                f"Kill signal sent to executor {job_exec.executor_id} "
                f"for {execution_id}"
            )

            response = {
                'request_id': request_id,
                'success': True,
                'data': {
                    'message': f'Kill signal sent to executor {job_exec.executor_id}',
                    'execution_id': execution_id,
                    'signal': 'SIGKILL' if force else 'SIGTERM',
                },
            }
            self._send_kill_response(client_id, response)

        except MessageSigningError as e:
            logger.error(f"Kill verification failed from {client_id}: {e}")
        except DeveloperAPIError as e:
            logger.error(f"Kill request error from {client_id}: {e}")
            try:
                request_id = payload.get('request_id', 'unknown') if 'payload' in dir() else 'unknown'
                self._send_kill_response(client_id, {
                    'request_id': request_id,
                    'success': False,
                    'error': str(e),
                })
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error handling kill request from {client_id}: {e}")

    def _send_kill_response(self, client_id: str, payload: Dict[str, Any]) -> None:
        """Send kill response."""
        topic = f"linearjc/dev/kill/response/{client_id}"
        try:
            self.mqtt.publish_message(topic, payload, qos=1)
        except Exception as e:
            logger.error(f"Failed to send kill response to {client_id}: {e}")
