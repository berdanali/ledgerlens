#!/usr/bin/env python3
"""
Download landing zone Parquet files from MinIO to a local cache directory.

Use this when DuckDB's httpfs extension cannot be loaded (e.g. Windows
Smart App Control).  After syncing, run dbt with the local path override:

    python fetch_from_minio.py
    dbt run --profiles-dir . --vars '{"landing_base_url": "landing_cache"}'
    dbt test --profiles-dir . --vars '{"landing_base_url": "landing_cache"}'

Files that already exist locally and match the remote size are skipped,
so subsequent runs only download new or changed files.

Required env vars (same as the consumer):
    MINIO_SECRET_KEY
Optional:
    MINIO_ENDPOINT_URL   (default: http://localhost:9000)
    MINIO_ACCESS_KEY     (default: minioadmin)
    MINIO_BUCKET         (default: ledgerlens-landing)
"""

import argparse
import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TABLES = ["customers", "accounts", "transactions"]


def sync(output_dir: str) -> None:
    endpoint  = os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ["MINIO_SECRET_KEY"]
    bucket    = os.environ.get("MINIO_BUCKET", "ledgerlens-landing")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    out = Path(output_dir)
    downloaded = skipped = 0

    for table in TABLES:
        paginator = client.get_paginator("list_objects_v2")
        try:
            pages = paginator.paginate(Bucket=bucket, Prefix=f"{table}/")
            for page in pages:
                for obj in page.get("Contents", []):
                    key  = obj["Key"]
                    dest = out / key
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.exists() and dest.stat().st_size == obj["Size"]:
                        skipped += 1
                        continue
                    client.download_file(bucket, key, str(dest))
                    logger.info("Downloaded  %s", key)
                    downloaded += 1
        except (BotoCoreError, ClientError) as exc:
            logger.error("Failed to sync table %s: %s", table, exc)
            raise

    logger.info(
        "Sync complete — %d downloaded, %d skipped (unchanged). Cache: %s/",
        downloaded, skipped, output_dir,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", default="landing_cache",
                        help="Local directory to sync files into (default: landing_cache)")
    args = parser.parse_args()
    sync(args.output_dir)


if __name__ == "__main__":
    main()
