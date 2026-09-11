"""
MinIO (S3-compatible) uploader for Parquet landing zone files.

Partition layout:
  {table_name}/year={Y}/month={MM}/day={DD}/hour={HH}/{uuid}.parquet

The timestamp used for partitioning comes from the last CDC event in the
batch (source.ts_ms), falling back to the current wall clock if absent.
This keeps partitions aligned with database time rather than consumer time.
"""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


class MinIOUploader:
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
    ) -> None:
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            # MinIO does not use AWS regions but boto3 requires a value
            region_name="us-east-1",
        )

    def _object_key(self, table_name: str, ts: datetime) -> str:
        return (
            f"{table_name}/"
            f"year={ts.year:04d}/"
            f"month={ts.month:02d}/"
            f"day={ts.day:02d}/"
            f"hour={ts.hour:02d}/"
            f"{uuid.uuid4()}.parquet"
        )

    def upload(
        self,
        local_path: Path,
        table_name: str,
        event_ts: datetime | None = None,
    ) -> str:
        """Upload *local_path* to MinIO under a time-partitioned key.

        Returns the object key on success; raises on failure.
        """
        ts = event_ts or datetime.now(tz=timezone.utc)
        key = self._object_key(table_name, ts)
        try:
            self._client.upload_file(str(local_path), self._bucket, key)
            logger.info("Uploaded s3://%s/%s", self._bucket, key)
            return key
        except (BotoCoreError, ClientError) as exc:
            logger.error("MinIO upload failed for %s: %s", key, exc)
            raise
