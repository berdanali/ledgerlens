#!/usr/bin/env python3
"""
Synthetic OLTP seed generator for LedgerLens.

Generates realistic banking data (customers, accounts, transactions) and
inserts it into a PostgreSQL database.  Data distributions are intentionally
non-uniform so the anomaly detection layer has meaningful signal to find.

Usage:
    python oltp/seed_generator.py

Required env vars (or DATABASE_URL):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
from faker import Faker
from pydantic import BaseModel, Field

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class SeedConfig(BaseModel):
    n_customers: int = Field(default=500, ge=1)
    accounts_per_customer_min: int = 1
    accounts_per_customer_max: int = 3
    transactions_per_account_min: int = 10
    transactions_per_account_max: int = 200
    suspicious_ratio: float = Field(default=0.03, ge=0.0, le=1.0)
    faker_locale: str = "de_DE"
    days_of_history: int = 730


# ---------------------------------------------------------------------------
# Distribution tables
# ---------------------------------------------------------------------------

# Transaction volume by hour (index = hour 0-23); reflects typical retail banking.
_HOUR_WEIGHTS: list[float] = [
    0.5, 0.3, 0.2, 0.2, 0.2, 0.3,   # 00-05 overnight low
    0.5, 1.0, 1.5, 2.5, 2.5, 2.0,   # 06-11 morning ramp
    2.0, 2.0, 1.5, 1.5, 1.5, 1.5,   # 12-17 midday
    2.5, 2.5, 2.0, 1.5, 1.0, 0.7,   # 18-23 evening peak then drop
]

_TRANSACTION_TYPES = ["TRANSFER", "WITHDRAWAL", "DEPOSIT", "PAYMENT"]
_TYPE_WEIGHTS = [0.30, 0.25, 0.25, 0.20]

_TRANSACTION_STATUSES = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
_STATUS_WEIGHTS = [0.88, 0.06, 0.04, 0.02]

_CURRENCIES = ["EUR", "USD", "GBP"]
_CURRENCY_WEIGHTS = [0.80, 0.12, 0.08]

_ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "BUSINESS"]
_ACCOUNT_TYPE_WEIGHTS = [0.55, 0.35, 0.10]

_ACCOUNT_STATUSES = ["ACTIVE", "FROZEN", "CLOSED"]
_ACCOUNT_STATUS_WEIGHTS = [0.90, 0.05, 0.05]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config() -> SeedConfig:
    return SeedConfig(
        n_customers=int(os.environ.get("SEED_N_CUSTOMERS", 500)),
        suspicious_ratio=float(os.environ.get("SEED_SUSPICIOUS_RATIO", 0.03)),
    )


def _get_connection() -> psycopg2.extensions.connection:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        host = os.environ["POSTGRES_HOST"]
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ["POSTGRES_DB"]
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return psycopg2.connect(dsn)


def _random_datetime(config: SeedConfig) -> datetime:
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=config.days_of_history)
    total_seconds = config.days_of_history * 86400
    random_offset = timedelta(seconds=random.randint(0, total_seconds))
    day = (start + random_offset).replace(second=0, microsecond=0)
    hour = random.choices(range(24), weights=_HOUR_WEIGHTS)[0]
    return day.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_customers(config: SeedConfig, faker: Faker) -> list[dict[str, Any]]:
    customers = []
    for _ in range(config.n_customers):
        customers.append({
            "customer_id": str(uuid.uuid4()),
            "first_name": faker.first_name(),
            "last_name": faker.last_name(),
            "email": faker.unique.email(),
            "national_id": faker.numerify("##########"),
            "date_of_birth": faker.date_of_birth(minimum_age=18, maximum_age=80),
        })
    logger.info("Generated %d customers", len(customers))
    return customers


def generate_accounts(
    config: SeedConfig,
    customers: list[dict[str, Any]],
    faker: Faker,
) -> list[dict[str, Any]]:
    accounts = []
    for customer in customers:
        n = random.randint(config.accounts_per_customer_min, config.accounts_per_customer_max)
        for _ in range(n):
            currency = random.choices(_CURRENCIES, weights=_CURRENCY_WEIGHTS)[0]
            # Account balance: lognormal centred around ~1100 EUR
            balance = round(max(0.0, float(np.random.lognormal(mean=7.0, sigma=1.5))), 2)
            accounts.append({
                "account_id": str(uuid.uuid4()),
                "customer_id": customer["customer_id"],
                "account_number": faker.iban(),
                "account_type": random.choices(_ACCOUNT_TYPES, weights=_ACCOUNT_TYPE_WEIGHTS)[0],
                "currency": currency,
                "balance": balance,
                "status": random.choices(_ACCOUNT_STATUSES, weights=_ACCOUNT_STATUS_WEIGHTS)[0],
            })
    logger.info("Generated %d accounts", len(accounts))
    return accounts


def _normal_transactions(
    account: dict[str, Any],
    n: int,
    config: SeedConfig,
    all_account_ids: list[str],
) -> list[dict[str, Any]]:
    txns: list[dict[str, Any]] = []
    for _ in range(n):
        # Transaction amount: lognormal — median ~90 EUR, realistic long tail
        amount = round(max(0.01, float(np.random.lognormal(mean=4.5, sigma=1.2))), 2)
        txn_type = random.choices(_TRANSACTION_TYPES, weights=_TYPE_WEIGHTS)[0]
        to_id = None
        if txn_type == "TRANSFER" and len(all_account_ids) > 1:
            to_id = random.choice([a for a in all_account_ids if a != account["account_id"]])
        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "from_account_id": account["account_id"],
            "to_account_id": to_id,
            "amount": amount,
            "currency": account["currency"],
            "transaction_type": txn_type,
            "status": random.choices(_TRANSACTION_STATUSES, weights=_STATUS_WEIGHTS)[0],
            "description": None,
            "created_at": _random_datetime(config),
        })
    return txns


def _suspicious_burst(
    account: dict[str, Any],
    config: SeedConfig,
    all_account_ids: list[str],
) -> list[dict[str, Any]]:
    """Rapid high-value transfers clustered in a 2-hour window.

    Pattern detectable by: high transaction frequency, amounts 5-10× normal,
    and tight temporal clustering — all three anomaly signals at once.
    """
    n = random.randint(5, 15)
    burst_start = _random_datetime(config)
    txns: list[dict[str, Any]] = []
    for i in range(n):
        amount = round(float(np.random.lognormal(mean=4.5, sigma=1.2)) * random.uniform(5, 10), 2)
        to_id = None
        if len(all_account_ids) > 1:
            to_id = random.choice([a for a in all_account_ids if a != account["account_id"]])
        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "from_account_id": account["account_id"],
            "to_account_id": to_id,
            "amount": amount,
            "currency": account["currency"],
            "transaction_type": "TRANSFER",
            "status": "COMPLETED",
            "description": None,
            "created_at": burst_start + timedelta(seconds=random.randint(i * 10, i * 120)),
        })
    return txns


def generate_transactions(
    config: SeedConfig,
    accounts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    all_account_ids = [a["account_id"] for a in accounts]
    n_suspicious = max(1, int(len(accounts) * config.suspicious_ratio))
    suspicious_ids = set(random.sample(all_account_ids, n_suspicious))

    transactions: list[dict[str, Any]] = []
    for account in accounts:
        n = random.randint(
            config.transactions_per_account_min,
            config.transactions_per_account_max,
        )
        transactions.extend(_normal_transactions(account, n, config, all_account_ids))
        if account["account_id"] in suspicious_ids:
            transactions.extend(_suspicious_burst(account, config, all_account_ids))

    logger.info(
        "Generated %d transactions (%d accounts with suspicious burst patterns)",
        len(transactions),
        n_suspicious,
    )
    return transactions


# ---------------------------------------------------------------------------
# DB insert helpers
# ---------------------------------------------------------------------------

def _insert_customers(
    conn: psycopg2.extensions.connection,
    customers: list[dict[str, Any]],
) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO customers
              (customer_id, first_name, last_name, email, national_id, date_of_birth)
            VALUES %s
            """,
            [
                (c["customer_id"], c["first_name"], c["last_name"],
                 c["email"], c["national_id"], c["date_of_birth"])
                for c in customers
            ],
        )
    conn.commit()
    logger.info("Inserted %d customers", len(customers))


def _insert_accounts(
    conn: psycopg2.extensions.connection,
    accounts: list[dict[str, Any]],
) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO accounts
              (account_id, customer_id, account_number, account_type, currency, balance, status)
            VALUES %s
            """,
            [
                (a["account_id"], a["customer_id"], a["account_number"],
                 a["account_type"], a["currency"], a["balance"], a["status"])
                for a in accounts
            ],
        )
    conn.commit()
    logger.info("Inserted %d accounts", len(accounts))


def _insert_transactions(
    conn: psycopg2.extensions.connection,
    transactions: list[dict[str, Any]],
) -> None:
    batch_size = 5000
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(transactions), batch_size):
            batch = transactions[i : i + batch_size]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO transactions
                  (transaction_id, from_account_id, to_account_id, amount, currency,
                   transaction_type, status, description, created_at)
                VALUES %s
                """,
                [
                    (t["transaction_id"], t["from_account_id"], t["to_account_id"],
                     t["amount"], t["currency"], t["transaction_type"],
                     t["status"], t["description"], t["created_at"])
                    for t in batch
                ],
            )
            conn.commit()
            total += len(batch)
            logger.info("Inserted %d / %d transactions", total, len(transactions))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    config = _load_config()

    Faker.seed(42)
    np.random.seed(42)
    random.seed(42)
    faker = Faker(config.faker_locale)

    customers = generate_customers(config, faker)
    accounts = generate_accounts(config, customers, faker)
    transactions = generate_transactions(config, accounts)

    try:
        conn = _get_connection()
    except psycopg2.OperationalError as exc:
        logger.error("Cannot connect to database: %s", exc)
        raise

    try:
        _insert_customers(conn, customers)
        _insert_accounts(conn, accounts)
        _insert_transactions(conn, transactions)
        logger.info("Seed complete.")
    except psycopg2.Error as exc:
        logger.error("Database error during seed: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
