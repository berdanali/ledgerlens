-- Grain: one row per customer — latest CDC state, SCD Type 1.
-- PII columns were pseudonymized in the consumer layer; field names are
-- preserved but values are SHA-256 tokens.

with raw as (
    select *
    from read_parquet(
        's3://{{ var("landing_bucket") }}/customers/**/*.parquet',
        hive_partitioning = true
    )
),

-- Remove at-least-once duplicates at the Kafka message level before any
-- business logic.  Two messages with the same partition+offset are identical.
deduped_messages as (
    select *,
        row_number() over (
            partition by _kafka_partition, _kafka_offset
            order by     _kafka_offset
        ) as _msg_rn
    from raw
),

-- SCD Type 1: keep only the most recent event per customer.
latest as (
    select *,
        row_number() over (
            partition by customer_id
            order by     _cdc_ts_ms desc
        ) as _row_rn
    from deduped_messages
    where _msg_rn  = 1
      and _cdc_op != 'd'
)

select
    customer_id,
    first_name      as first_name_token,
    last_name       as last_name_token,
    email           as email_token,
    national_id     as national_id_token,
    date_of_birth   as date_of_birth_token,
    created_at,
    updated_at,
    _cdc_op,
    _cdc_ts_ms
from latest
where _row_rn = 1
