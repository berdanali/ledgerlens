-- Grain: one row per meaningful state change per account.
-- "Meaningful" = balance or status changed (or the very first CDC event).
-- This is the input to the SCD Type 2 dim_accounts mart.
--
-- Why deduplicate here rather than in staging?
-- stg_accounts keeps all events so other marts can reuse it without
-- assumptions.  This intermediate model owns the SCD change-detection logic.

with staged as (
    select * from {{ ref('stg_accounts') }}
),

-- For each event, look at the previous event for the same account
with_prev as (
    select
        *,
        lag(balance) over (partition by account_id order by _cdc_ts_ms) as prev_balance,
        lag(status)  over (partition by account_id order by _cdc_ts_ms) as prev_status
    from staged
),

-- A new SCD row is warranted when this is the first event, or when a
-- tracked slowly-changing attribute actually changed.
changepoints as (
    select
        *,
        case
            when prev_balance is null                    then true  -- first ever event
            when balance != prev_balance                 then true  -- balance moved
            when status  != coalesce(prev_status, '')   then true  -- status flipped
            else false
        end as is_change
    from with_prev
)

select
    account_id,
    customer_id,
    account_number_token,
    account_type,
    currency,
    balance,
    status,
    created_at,
    _cdc_ts_ms  as change_ts_ms
from changepoints
where is_change = true
