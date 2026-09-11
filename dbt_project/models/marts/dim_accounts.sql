-- Grain: one row per account version (SCD Type 2).
-- Each row represents the state of an account between valid_from and valid_to.
-- is_current = true marks the active version.
--
-- This is the concrete portfolio argument for CDC over batch:
--   batch export → one row per account, current balance only.
--   CDC → full balance and status history, reconstructible for any point in time.

with changes as (
    select * from {{ ref('int_account_changes') }}
),

scd2 as (
    select
        -- Surrogate key: unique per (account, version)
        md5(account_id || '|' || change_ts_ms::varchar)  as account_sk,
        account_id,
        customer_id,
        account_number_token,
        account_type,
        currency,
        balance,
        status,
        created_at,

        -- valid_from: when this version became active
        epoch_ms(change_ts_ms)                                                              as valid_from,

        -- valid_to: when the next version superseded this one (NULL = still current)
        epoch_ms(
            lead(change_ts_ms) over (partition by account_id order by change_ts_ms)
        )                                                                                   as valid_to,

        lead(change_ts_ms) over (partition by account_id order by change_ts_ms) is null    as is_current,

        row_number() over (partition by account_id order by change_ts_ms)                  as version_number
    from changes
)

select * from scd2
