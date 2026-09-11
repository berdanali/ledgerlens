"""
Landing zone consumer: Kafka → (pseudonymize) → Parquet → MinIO.

Batching strategy: time-windowed with a message-count ceiling.
A topic's buffer is flushed when either:
  - flush_interval_seconds have elapsed since the last flush, OR
  - the buffer reaches flush_max_messages.

Offsets are committed only after a successful MinIO upload, so a crash
before upload leaves the batch unconsumed and it will be reprocessed.
This gives at-least-once delivery to the landing zone.
"""

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from consumer.config import LandingConfig
from consumer.pseudonymizer import pseudonymize
from consumer.uploader import MinIOUploader
from consumer.writer import write_parquet

logger = logging.getLogger(__name__)


def _table_name(topic: str) -> str:
    """'ledgerlens.public.transactions' → 'transactions'"""
    return topic.rsplit(".", 1)[-1]


def _event_ts(envelope: dict[str, Any]) -> datetime | None:
    ts_ms = (envelope.get("source") or {}).get("ts_ms") or envelope.get("ts_ms")
    if ts_ms:
        return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    return None


class LandingConsumer:
    def __init__(self, config: LandingConfig) -> None:
        self._config = config
        registry = SchemaRegistryClient({"url": config.schema_registry_url})
        self._deserializer = AvroDeserializer(registry)
        self._kafka = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": config.group_id,
                "auto.offset.reset": "earliest",
                # Manual commit — only after successful MinIO upload
                "enable.auto.commit": False,
            }
        )
        self._uploader = MinIOUploader(
            endpoint_url=config.minio_endpoint_url,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            bucket=config.minio_bucket,
        )
        self._envelopes: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._metas: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._last_flush: dict[str, float] = defaultdict(time.monotonic)

    def _flush(self, topic: str) -> None:
        envelopes = self._envelopes.pop(topic, [])
        metas = self._metas.pop(topic, [])
        if not envelopes:
            return

        table = _table_name(topic)
        pseudonymized = [
            pseudonymize(env, table, self._config.pseudonym_salt)
            for env in envelopes
        ]
        ts = _event_ts(envelopes[-1])

        tmp: Path = write_parquet(pseudonymized, metas)
        try:
            self._uploader.upload(tmp, table, ts)
            self._kafka.commit(asynchronous=False)
            logger.info(
                "Flushed %d events for topic %s", len(envelopes), topic
            )
        except Exception:
            logger.exception("Flush failed for topic %s — will retry", topic)
            # Put messages back so they are retried on the next flush cycle
            self._envelopes[topic] = envelopes + self._envelopes[topic]
            self._metas[topic] = metas + self._metas[topic]
        finally:
            tmp.unlink(missing_ok=True)

        self._last_flush[topic] = time.monotonic()

    def _should_flush(self, topic: str) -> bool:
        elapsed = time.monotonic() - self._last_flush[topic]
        return (
            elapsed >= self._config.flush_interval_seconds
            or len(self._envelopes[topic]) >= self._config.flush_max_messages
        )

    def _handle_message(self, msg: Message) -> None:
        if msg.value() is None:
            return  # Kafka tombstone — delete propagated via op="d" envelope
        try:
            envelope: dict[str, Any] = self._deserializer(msg.value(), None)
        except Exception as exc:
            logger.error(
                "Avro decode error topic=%s offset=%d: %s",
                msg.topic(),
                msg.offset(),
                exc,
            )
            return
        topic = msg.topic()
        self._envelopes[topic].append(envelope)
        self._metas[topic].append(
            {
                "topic": topic,
                "partition": msg.partition(),
                "offset": msg.offset(),
            }
        )

    def run(self) -> None:
        self._kafka.subscribe(self._config.topics)
        logger.info("Subscribed to %s", self._config.topics)
        try:
            while True:
                msg = self._kafka.poll(timeout=1.0)
                if msg is not None:
                    if msg.error():
                        code = msg.error().code()
                        if code != KafkaError._PARTITION_EOF:
                            logger.error("Kafka error: %s", msg.error())
                    else:
                        self._handle_message(msg)

                for topic in list(self._envelopes):
                    if self._should_flush(topic):
                        self._flush(topic)

        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
        finally:
            logger.info("Flushing remaining buffers...")
            for topic in list(self._envelopes):
                self._flush(topic)
            self._kafka.close()
