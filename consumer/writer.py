"""
Buffer-to-Parquet writer for CDC envelopes.

Each Debezium envelope is flattened into a single row:
  - Columns from the *after* image (or *before* for deletes)
  - _cdc_op, _cdc_ts_ms, _cdc_before (JSON string), _kafka_* metadata

PyArrow infers the schema from the data at flush time.  All datetime /
Decimal types that fastavro produces are handled natively by PyArrow.
"""

import decimal
import json
import logging
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    return str(obj)


def _flatten(envelope: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    after: dict[str, Any] = envelope.get("after") or {}
    before: dict[str, Any] | None = envelope.get("before")

    # For DELETE events after is null — use before as the row payload
    row = dict(after) if after else dict(before or {})

    row["_cdc_op"] = envelope.get("op", "")
    source: dict[str, Any] = envelope.get("source") or {}
    row["_cdc_ts_ms"] = source.get("ts_ms") or envelope.get("ts_ms")
    row["_cdc_before"] = (
        json.dumps(before, default=_json_default) if before else None
    )
    row["_kafka_topic"] = meta["topic"]
    row["_kafka_partition"] = meta["partition"]
    row["_kafka_offset"] = meta["offset"]
    return row


def write_parquet(
    envelopes: list[dict[str, Any]],
    kafka_metas: list[dict[str, Any]],
) -> Path:
    """Flatten *envelopes* and write them to a temporary Parquet file.

    Returns the path to the temp file; caller is responsible for deleting it.
    """
    rows = [_flatten(env, meta) for env, meta in zip(envelopes, kafka_metas)]
    table = pa.Table.from_pylist(rows)
    tmp = Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.parquet"
    pq.write_table(table, tmp, compression="snappy")
    logger.info(
        "Parquet written: %d rows, %d bytes → %s",
        len(rows),
        tmp.stat().st_size,
        tmp,
    )
    return tmp
