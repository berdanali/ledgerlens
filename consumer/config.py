"""
Runtime configuration for the landing zone consumer.

All values come from environment variables — no defaults for secrets.
"""

import os

from pydantic import BaseModel, Field


class LandingConfig(BaseModel):
    kafka_bootstrap_servers: str
    schema_registry_url: str
    group_id: str = "ledgerlens-landing-consumer"
    topics: list[str] = Field(
        default_factory=lambda: [
            "ledgerlens.public.customers",
            "ledgerlens.public.accounts",
            "ledgerlens.public.transactions",
        ]
    )
    flush_interval_seconds: float = 60.0
    flush_max_messages: int = 5000
    minio_endpoint_url: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str = "ledgerlens-landing"
    pseudonym_salt: str


def load_config() -> LandingConfig:
    return LandingConfig(
        kafka_bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"],
        group_id=os.environ.get("CONSUMER_GROUP_ID", "ledgerlens-landing-consumer"),
        flush_interval_seconds=float(os.environ.get("FLUSH_INTERVAL_SECONDS", "60")),
        flush_max_messages=int(os.environ.get("FLUSH_MAX_MESSAGES", "5000")),
        minio_endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        minio_access_key=os.environ["MINIO_ACCESS_KEY"],
        minio_secret_key=os.environ["MINIO_SECRET_KEY"],
        minio_bucket=os.environ.get("MINIO_BUCKET", "ledgerlens-landing"),
        pseudonym_salt=os.environ["PSEUDONYM_SALT"],
    )
