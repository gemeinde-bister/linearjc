"""Integration tests for MinIO operations."""
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from minio import Minio


class TestMinioConnection:
    """Tests for MinIO connectivity."""

    def test_minio_server_starts(self, minio_server):
        """MinIO server fixture starts correctly."""
        assert minio_server['endpoint']
        assert minio_server['access_key']
        assert minio_server['secret_key']

    def test_can_connect(self, minio_server):
        """Can connect to MinIO with credentials."""
        client = Minio(
            minio_server['endpoint'],
            access_key=minio_server['access_key'],
            secret_key=minio_server['secret_key'],
            secure=minio_server['secure']
        )
        # List buckets (should work even if empty)
        buckets = list(client.list_buckets())
        assert isinstance(buckets, list)

    def test_create_bucket_and_upload(self, minio_server, temp_work_dir):
        """Can create bucket, upload, and download files."""
        client = Minio(
            minio_server['endpoint'],
            access_key=minio_server['access_key'],
            secret_key=minio_server['secret_key'],
            secure=minio_server['secure']
        )

        bucket_name = "test-bucket"

        # Create bucket
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        # Create test file
        test_file = os.path.join(temp_work_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("Hello, MinIO!")

        # Upload
        client.fput_object(bucket_name, "test.txt", test_file)

        # Download
        download_path = os.path.join(temp_work_dir, "downloaded.txt")
        client.fget_object(bucket_name, "test.txt", download_path)

        # Verify
        with open(download_path) as f:
            content = f.read()
        assert content == "Hello, MinIO!"

    def test_presigned_urls(self, minio_server, temp_work_dir):
        """Can generate and use presigned URLs."""
        from datetime import timedelta

        client = Minio(
            minio_server['endpoint'],
            access_key=minio_server['access_key'],
            secret_key=minio_server['secret_key'],
            secure=minio_server['secure']
        )

        bucket_name = "presigned-test"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        # Upload a file first
        test_file = os.path.join(temp_work_dir, "presigned.txt")
        with open(test_file, 'w') as f:
            f.write("Presigned content")
        client.fput_object(bucket_name, "presigned.txt", test_file)

        # Generate presigned GET URL
        url = client.presigned_get_object(
            bucket_name,
            "presigned.txt",
            expires=timedelta(hours=1)
        )
        assert url.startswith("http://")
        assert "presigned.txt" in url


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
