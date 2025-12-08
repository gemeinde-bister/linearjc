"""
Minio manager for LinearJC coordinator.

Handles all Minio operations:
- Upload/download files
- Generate pre-signed URLs
- Bucket management
- Cleanup of orphaned objects
"""
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple
from minio import Minio
from minio.error import S3Error
from urllib3.exceptions import MaxRetryError

from coordinator.models import CoordinatorConfig

logger = logging.getLogger(__name__)


class MinioError(Exception):
    """Error during Minio operations."""
    pass


class MinioManager:
    """
    Manager for Minio object storage operations.

    Provides high-level interface for file operations and cleanup.
    """

    def __init__(self, config: CoordinatorConfig.MinioConfig):
        """
        Initialize Minio manager.

        Args:
            config: Minio configuration from coordinator config
        """
        self.config = config
        self.client: Optional[Minio] = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to Minio server."""
        try:
            self.client = Minio(
                self.config.endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure
            )
            logger.info(f"Connected to Minio: {self.config.endpoint}")

        except Exception as e:
            raise MinioError(f"Failed to connect to Minio: {e}")

    def ensure_bucket(self, bucket: str) -> None:
        """
        Ensure bucket exists, create if needed.

        Args:
            bucket: Bucket name

        Raises:
            MinioError: If bucket creation fails
        """
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
            else:
                logger.debug(f"Bucket exists: {bucket}")

        except S3Error as e:
            raise MinioError(f"Failed to ensure bucket '{bucket}': {e}")

    def upload_file(
        self,
        local_path: str,
        bucket: str,
        object_name: str,
        content_type: str = "application/octet-stream"
    ) -> None:
        """
        Upload a file to Minio.

        Args:
            local_path: Path to local file
            bucket: Destination bucket
            object_name: Object name in bucket
            content_type: MIME type (default: application/octet-stream)

        Raises:
            MinioError: If upload fails
        """
        local_file = Path(local_path)

        if not local_file.exists():
            raise MinioError(f"Local file does not exist: {local_path}")

        try:
            self.ensure_bucket(bucket)

            file_size = local_file.stat().st_size
            logger.debug(
                f"Uploading {local_path} -> s3://{bucket}/{object_name} "
                f"({file_size} bytes)"
            )

            self.client.fput_object(
                bucket,
                object_name,
                str(local_file),
                content_type=content_type
            )

            logger.info(f"Uploaded: s3://{bucket}/{object_name}")

        except S3Error as e:
            raise MinioError(f"Failed to upload {local_path}: {e}")

    def download_file(
        self,
        bucket: str,
        object_name: str,
        local_path: str
    ) -> None:
        """
        Download a file from Minio.

        Args:
            bucket: Source bucket
            object_name: Object name in bucket
            local_path: Destination local path

        Raises:
            MinioError: If download fails
        """
        local_file = Path(local_path)
        local_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            logger.debug(
                f"Downloading s3://{bucket}/{object_name} -> {local_path}"
            )

            self.client.fget_object(
                bucket,
                object_name,
                str(local_file)
            )

            logger.info(f"Downloaded: {local_path}")

        except S3Error as e:
            raise MinioError(
                f"Failed to download s3://{bucket}/{object_name}: {e}"
            )

    def download_object_to_bytes(
        self,
        bucket: str,
        object_name: str
    ) -> bytes:
        """
        Download an object from Minio and return as bytes.

        Args:
            bucket: Source bucket
            object_name: Object name in bucket

        Returns:
            Object data as bytes

        Raises:
            MinioError: If download fails
        """
        try:
            logger.debug(f"Downloading s3://{bucket}/{object_name} to memory")

            response = self.client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()

            logger.info(f"Downloaded {len(data)} bytes from s3://{bucket}/{object_name}")

            return data

        except S3Error as e:
            raise MinioError(
                f"Failed to download s3://{bucket}/{object_name}: {e}"
            )

    def generate_presigned_get_url(
        self,
        bucket: str,
        object_name: str,
        expires_seconds: int = 3600
    ) -> str:
        """
        Generate pre-signed GET URL for downloading.

        Args:
            bucket: Bucket name
            object_name: Object name
            expires_seconds: URL expiry time in seconds (default: 1 hour)

        Returns:
            Pre-signed URL string

        Raises:
            MinioError: If URL generation fails
        """
        try:
            url = self.client.presigned_get_object(
                bucket,
                object_name,
                expires=timedelta(seconds=expires_seconds)
            )

            logger.debug(
                f"Generated GET URL for s3://{bucket}/{object_name} "
                f"(expires in {expires_seconds}s)"
            )

            return url

        except S3Error as e:
            raise MinioError(
                f"Failed to generate GET URL for s3://{bucket}/{object_name}: {e}"
            )

    def generate_presigned_put_url(
        self,
        bucket: str,
        object_name: str,
        expires_seconds: int = 3600
    ) -> str:
        """
        Generate pre-signed PUT URL for uploading.

        Args:
            bucket: Bucket name
            object_name: Object name
            expires_seconds: URL expiry time in seconds (default: 1 hour)

        Returns:
            Pre-signed URL string

        Raises:
            MinioError: If URL generation fails
        """
        try:
            self.ensure_bucket(bucket)

            url = self.client.presigned_put_object(
                bucket,
                object_name,
                expires=timedelta(seconds=expires_seconds)
            )

            logger.debug(
                f"Generated PUT URL for s3://{bucket}/{object_name} "
                f"(expires in {expires_seconds}s)"
            )

            return url

        except S3Error as e:
            raise MinioError(
                f"Failed to generate PUT URL for s3://{bucket}/{object_name}: {e}"
            )

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        older_than_days: Optional[int] = None
    ) -> List[Tuple[str, datetime, int]]:
        """
        List objects in bucket with optional filters.

        Args:
            bucket: Bucket name
            prefix: Object name prefix (default: all objects)
            older_than_days: Only return objects older than N days

        Returns:
            List of tuples: (object_name, last_modified, size_bytes)

        Raises:
            MinioError: If listing fails
        """
        try:
            objects = []
            cutoff_date = None

            if older_than_days is not None:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=older_than_days)

            for obj in self.client.list_objects(bucket, prefix=prefix, recursive=True):
                # Filter by age if requested
                if cutoff_date and obj.last_modified > cutoff_date:
                    continue

                objects.append((
                    obj.object_name,
                    obj.last_modified,
                    obj.size
                ))

            logger.debug(
                f"Listed {len(objects)} objects in s3://{bucket}/{prefix}"
                + (f" (older than {older_than_days} days)" if older_than_days else "")
            )

            return objects

        except S3Error as e:
            raise MinioError(f"Failed to list objects in s3://{bucket}/{prefix}: {e}")

    def delete_object(self, bucket: str, object_name: str) -> None:
        """
        Delete a single object.

        Args:
            bucket: Bucket name
            object_name: Object name

        Raises:
            MinioError: If deletion fails
        """
        try:
            self.client.remove_object(bucket, object_name)
            logger.debug(f"Deleted: s3://{bucket}/{object_name}")

        except S3Error as e:
            raise MinioError(
                f"Failed to delete s3://{bucket}/{object_name}: {e}"
            )

    def cleanup_old_objects(
        self,
        bucket: str,
        prefix: str,
        older_than_days: int,
        dry_run: bool = False
    ) -> int:
        """
        Delete objects older than specified age.

        Args:
            bucket: Bucket name
            prefix: Object name prefix
            older_than_days: Delete objects older than N days
            dry_run: If True, only log what would be deleted

        Returns:
            Number of objects deleted (or would be deleted if dry_run)

        Raises:
            MinioError: If cleanup fails
        """
        try:
            objects = self.list_objects(bucket, prefix, older_than_days)

            if not objects:
                logger.info(
                    f"No objects to clean up in s3://{bucket}/{prefix} "
                    f"(older than {older_than_days} days)"
                )
                return 0

            logger.info(
                f"Found {len(objects)} objects to clean up in s3://{bucket}/{prefix}"
            )

            deleted_count = 0
            for obj_name, last_modified, size in objects:
                age_days = (datetime.now(timezone.utc) - last_modified).days

                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would delete: {obj_name} "
                        f"(age: {age_days} days, size: {size} bytes)"
                    )
                else:
                    self.delete_object(bucket, obj_name)
                    logger.info(
                        f"Deleted: {obj_name} "
                        f"(age: {age_days} days, size: {size} bytes)"
                    )

                deleted_count += 1

            return deleted_count

        except MinioError:
            raise
        except Exception as e:
            raise MinioError(f"Cleanup failed: {e}")

    def cleanup_orphaned_executions(
        self,
        bucket: str,
        age_hours: int = 24,
        dry_run: bool = False
    ) -> int:
        """
        Clean up orphaned job execution artifacts.

        Deletes all objects under jobs/* prefix that are older than
        specified hours. This handles cases where coordinator crashed
        and didn't clean up temporary files.

        Args:
            bucket: Bucket name (typically temp_bucket)
            age_hours: Delete executions older than N hours
            dry_run: If True, only log what would be deleted

        Returns:
            Number of objects deleted

        Raises:
            MinioError: If cleanup fails
        """
        older_than_days = age_hours / 24.0

        logger.info(
            f"Cleaning up orphaned executions in s3://{bucket}/jobs/ "
            f"(older than {age_hours} hours)"
        )

        return self.cleanup_old_objects(
            bucket,
            prefix="jobs/",
            older_than_days=older_than_days,
            dry_run=dry_run
        )

    def test_connection(self) -> bool:
        """
        Test connection to Minio server.

        Returns:
            True if connection works, False otherwise
        """
        try:
            # Try to list buckets as a connection test
            list(self.client.list_buckets())
            return True

        except (S3Error, MaxRetryError) as e:
            logger.error(f"Minio connection test failed: {e}")
            return False
