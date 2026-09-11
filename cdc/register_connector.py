#!/usr/bin/env python3
"""
Register the Debezium source connector with Kafka Connect.

Reads the connector template from cdc/connectors/ledgerlens-source.json,
injects DB credentials from environment variables, and calls the Kafka
Connect REST API.  Safe to run multiple times — PUT is idempotent.

Usage:
    python cdc/register_connector.py
"""

import json
import logging
import os
import time
from pathlib import Path

import requests

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
_CONNECTOR_CONFIG = Path(__file__).parent / "connectors" / "ledgerlens-source.json"


def _wait_for_connect(url: str, retries: int = 24, delay: float = 5.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(f"{url}/connectors", timeout=5)
            if resp.status_code == 200:
                logger.info("Kafka Connect is ready at %s", url)
                return
        except requests.exceptions.ConnectionError:
            pass
        logger.info("Waiting for Kafka Connect (%d/%d)...", attempt, retries)
        time.sleep(delay)
    raise RuntimeError(f"Kafka Connect at {url} did not become ready after {retries} attempts")


def _inject_secrets(config: dict) -> dict:
    config["database.user"] = os.environ["POSTGRES_USER"]
    config["database.password"] = os.environ["POSTGRES_PASSWORD"]
    config["database.dbname"] = os.environ.get("POSTGRES_DB", "ledgerlens")
    return config


def register_connector(url: str = CONNECT_URL) -> None:
    with _CONNECTOR_CONFIG.open() as f:
        payload = json.load(f)

    connector_name = payload["name"]
    connector_config = _inject_secrets(payload["config"])

    resp = requests.put(
        f"{url}/connectors/{connector_name}/config",
        json=connector_config,
        headers={"Content-Type": "application/json"},
        timeout=15,
    )

    if resp.status_code in (200, 201):
        logger.info("Connector '%s' registered (HTTP %d)", connector_name, resp.status_code)
    else:
        logger.error("Registration failed: HTTP %d — %s", resp.status_code, resp.text)
        resp.raise_for_status()


def main() -> None:
    _wait_for_connect(CONNECT_URL)
    register_connector(CONNECT_URL)


if __name__ == "__main__":
    main()
