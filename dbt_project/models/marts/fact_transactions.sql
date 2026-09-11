-- Grain: one row per financial transaction.
-- Joins to dim_accounts (current version) for the from_customer_id FK.
-- date_id links to dim_date for time-series analysis.

with txns as (
    select * from {{ ref('stg_transactions') }}
),

-- Use current account version for the customer FK lookup.
-- Historical account versions intentionally not used here — the customer
-- who owned the account at transaction time can be reconstructed via
-- dim_accounts by filtering on valid_from <= created_at < valid_to.
current_accounts as (
    select account_id, customer_id
    from {{ ref('dim_accounts') }}
    where is_current = true
)

select
    t.transaction_id,
    t.from_account_id,
    t.to_account_id,
    a.customer_id                                                   as from_customer_id,
    t.amount,
    t.currency,
    t.transaction_type,
    t.status,
    t.description,
    t.created_at,
    cast(strftime(t.created_at::date, '%Y%m%d') as integer)        as date_id,
    t._cdc_ts_ms                                                    as cdc_ingested_at_ms
from txns t
left join current_accounts a on t.from_account_id = a.account_id
