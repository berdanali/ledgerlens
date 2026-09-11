#!/usr/bin/env python3
"""
GDPR / BaFin retention configuration for LedgerLens.

Applies retention rules to two layers:

  Kafka  — sets retention.ms per topic so brokers expire old messages
           automatically without manual intervention.

  MinIO  — sets S3 lifecycle expiration rules per landing-zone prefix
           so Parquet objects are deleted after the retention window.

Retention periods chosen:
  transactions / accounts : 7 years  — BaFin §257 HGB (Handelsgesetzbuch)
                                       financial records minimum
  customers               : 90 days  — GDPR Art. 5(1)(e) storage limitation;
                                       shorter because customer events carry
                                       pseudonymized PII tokens.  With
                                       crypto-shredding (see CLAUDE.md §8)
                                       this could be extended to 7 years
                                       because destroyed keys make tokens
                                       permanently unlinkable.

Safe to re-run — Kafka alter_configs and S3 put_bucket_lifecycle_configuration
are both idempotent.

Usage (from project root, with .env sourced):
    python gdpr/configure_retention.py
"""

from __future__ import annotations

import logging
import os

import base64
import hashlib

import boto3
from botocore.exceptions import ClientError
from confluent_kafka.admin import AdminClient, ConfigResource

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retention constants
# ---------------------------------------------------------------------------

_SEVEN_YEARS_MS: int = 7 * 365 * 24 * 60 * 60 * 1_000   # 220,752,000,000 ms
_NINETY_DAYS_MS: int = 90 * 24 * 60 * 60 * 1_000          #   7,776,000,000 ms

# Kafka topics → retention.ms
TOPIC_RETENTION_MS: dict[str, int] = {
    "ledgerlens.public.transactions": _SEVEN_YEARS_MS,
    "ledgerlens.public.accounts":     _SEVEN_YEARS_MS,
    "ledgerlens.public.customers":    _NINETY_DAYS_MS,
}

# MinIO landing-zone prefixes → expiration days
# Must mirror Kafka retention so both layers age out together.
MINIO_EXPIRY_DAYS: dict[str, int] = {
    "transactions": 7 * 365,   # 2555 days
    "accounts":     7 * 365,
    "customers":    90,
}


# ---------------------------------------------------------------------------
# Kafka retention
# ---------------------------------------------------------------------------

def _apply_kafka_retention(bootstrap_servers: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics.keys())
    resources: list[ConfigResource] = []

    for topic, retention_ms in TOPIC_RETENTION_MS.items():
        if topic not in existing:
            log.warning("Topic %s not found — run after Debezium connector is active", topic)
            continue
        log.info("  %-50s  retention.ms = %d", topic, retention_ms)
        resources.append(
            ConfigResource(
                restype=ConfigResource.Type.TOPIC,
                name=topic,
                set_config={"retention.ms": str(retention_ms)},
            )
        )

    if not resources:
        log.warning("No topics updated — check that the Debezium connector has run at least once")
        return

    for resource, future in admin.alter_configs(resources).items():  # noqa: deprecated — incremental_alter_configs requires more complex ConfigEntry API
        try:
            future.result()
            log.info("OK  %s", resource.name)
        except Exception as exc:
            log.error("FAIL %s: %s", resource.name, exc)
            raise


# ---------------------------------------------------------------------------
# MinIO S3 lifecycle
# ---------------------------------------------------------------------------

def _add_content_md5(request, **kwargs) -> None:
    """MinIO requires Content-MD5 on PutBucketLifecycleConfiguration; boto3 omits it."""
    if request.body:
        body = request.body if isinstance(request.body, bytes) else request.body.encode("utf-8")
        request.headers["Content-MD5"] = base64.b64encode(hashlib.md5(body).digest()).decode()


def _apply_minio_lifecycle(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket: str,
) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",   # MinIO ignores region; boto3 requires a non-empty value
    )
    s3.meta.events.register(
        "before-send.s3.PutBucketLifecycleConfiguration",
        _add_content_md5,
    )

    rules = [
        {
            "ID": f"ledgerlens-expire-{prefix}",
            "Filter": {"Prefix": f"{prefix}/"},
            "Status": "Enabled",
            "Expiration": {"Days": days},
        }
        for prefix, days in MINIO_EXPIRY_DAYS.items()
    ]

    log.info("Applying %d lifecycle rules to bucket '%s'", len(rules), bucket)
    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": rules},
        )
    except ClientError as exc:
        log.error("Failed to set lifecycle rules: %s", exc)
        raise

    # Read back to confirm
    response = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
    for rule in response.get("Rules", []):
        log.info(
            "  %-45s  prefix=%-16s  expire=%d days",
            rule["ID"],
            rule["Filter"].get("Prefix", ""),
            rule["Expiration"]["Days"],
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _load_dotenv() -> None:
    """Load .env from the project root (parent of this file's directory)."""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    env_path = os.path.normpath(env_path)
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    _load_dotenv()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    endpoint  = os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")
    access    = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret    = os.environ["MINIO_ROOT_PASSWORD"]
    bucket    = os.environ.get("MINIO_BUCKET", "ledgerlens-landing")

    log.info("=== Kafka topic retention ===")
    _apply_kafka_retention(bootstrap)

    log.info("=== MinIO S3 lifecycle ===")
    _apply_minio_lifecycle(endpoint, access, secret, bucket)

    log.info("Retention configuration complete.")


if __name__ == "__main__":
    main()
