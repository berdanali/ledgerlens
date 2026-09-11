-- =============================================================================
-- LedgerLens OLTP Schema
-- =============================================================================
-- CDC Setup Note:
--   PostgreSQL must start with wal_level=logical (set in docker-compose.yml).
--
--   After the container is healthy, create the Debezium replication slot once:
--     SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
--   Do NOT create the slot here — running this script twice would fail.
--   Slot creation belongs in the Debezium connector startup or a one-time
--   admin script (see cdc/README.md when that phase is built).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- updated_at trigger (reused by customers and accounts)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- customers
-- PII fields: first_name, last_name, email, national_id, date_of_birth
-- None of these fields reach the landing zone in raw form (pseudonymized
-- in the Python consumer layer via SHA-256 + project salt).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id   UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name    VARCHAR(100)  NOT NULL,
    last_name     VARCHAR(100)  NOT NULL,
    email         VARCHAR(255)  NOT NULL UNIQUE,
    national_id   VARCHAR(50)   NOT NULL,
    date_of_birth DATE          NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- FULL ensures UPDATE/DELETE events carry the complete old row in WAL,
-- which Debezium needs to emit before-image in change events.
ALTER TABLE customers REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- accounts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS accounts (
    account_id     UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id    UUID           NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    account_number VARCHAR(34)    NOT NULL UNIQUE,
    account_type   VARCHAR(20)    NOT NULL CHECK (account_type IN ('CHECKING', 'SAVINGS', 'BUSINESS')),
    currency       CHAR(3)        NOT NULL CHECK (currency IN ('EUR', 'USD', 'GBP')),
    balance        NUMERIC(18, 2) NOT NULL DEFAULT 0.00,
    status         VARCHAR(20)    NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE accounts REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts(customer_id);

-- ---------------------------------------------------------------------------
-- transactions
-- Immutable by design — no updated_at, no update trigger.
-- Corrections are handled by inserting a REVERSED transaction,
-- matching how real core banking systems work.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    from_account_id  UUID           NOT NULL REFERENCES accounts(account_id),
    to_account_id    UUID           REFERENCES accounts(account_id),
    amount           NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency         CHAR(3)        NOT NULL,
    transaction_type VARCHAR(20)    NOT NULL CHECK (transaction_type IN ('TRANSFER', 'WITHDRAWAL', 'DEPOSIT', 'PAYMENT')),
    status           VARCHAR(20)    NOT NULL DEFAULT 'COMPLETED' CHECK (status IN ('COMPLETED', 'PENDING', 'FAILED', 'REVERSED')),
    description      TEXT,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
);

ALTER TABLE transactions REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS idx_transactions_from_account ON transactions(from_account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at   ON transactions(created_at);
