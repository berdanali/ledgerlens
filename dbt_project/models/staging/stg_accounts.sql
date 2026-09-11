-- Grain: one row per CDC event per account — NOT deduplicated by account_id.
-- The full change history is needed by int_account_changes to build SCD Type 2.
-- account_number is a SHA-256 token (IBAN pseudonymized in consumer layer).

with raw as (
    select *
    from read_parquet(
        '{{ var("landing_base_url") }}/accounts/**/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
),

-- Remove Kafka at-least-once duplicates only — keep all business events.
deduped_messages as (
    select *,
        row_number() over (
            partition by _kafka_partition, _kafka_offset
            order by     _kafka_offset
        ) as _msg_rn
    from raw
)

select
    account_id,
    customer_id,
    account_number  as account_number_token,
    account_type,
    currency,
    balance::DECIMAL(18,2) as balance,
    status,
    created_at,
    updated_at,
    _cdc_op,
    _cdc_ts_ms
from deduped_messages
where _msg_rn  = 1
  and _cdc_op != 'd'
