-- Grain: one row per transaction — earliest CDC event only.
-- Transactions are immutable by design (corrections use REVERSED status),
-- so the first CDC event is the authoritative state.

with raw as (
    select *
    from read_parquet(
        '{{ var("landing_base_url") }}/transactions/**/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
),

deduped_messages as (
    select *,
        row_number() over (
            partition by _kafka_partition, _kafka_offset
            order by     _kafka_offset
        ) as _msg_rn
    from raw
),

-- Pick the earliest event per transaction (op='r' for snapshot, 'c' for live inserts)
first_seen as (
    select *,
        row_number() over (
            partition by transaction_id
            order by     _cdc_ts_ms asc
        ) as _row_rn
    from deduped_messages
    where _msg_rn = 1
      and _cdc_op in ('r', 'c')
)

select
    transaction_id,
    from_account_id,
    to_account_id,
    amount::DECIMAL(18,2) as amount,
    currency,
    transaction_type,
    status,
    description,
    created_at,
    _cdc_ts_ms
from first_seen
where _row_rn = 1
