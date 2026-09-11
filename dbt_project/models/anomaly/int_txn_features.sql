{{ config(materialized='view') }}

-- Grain: one row per COMPLETED transaction.
-- Pre-computes window features consumed by all anomaly rules.
-- Only COMPLETED status is included — failed/reversed events carry no
-- meaningful pattern signal for fraud detection.

with completed as (
    select *
    from {{ ref('fact_transactions') }}
    where status = 'COMPLETED'
),

-- Rolling count of completed TRANSFERs from the same source account
-- in the 2 hours preceding (and including) each event.
-- created_at is stored as VARCHAR in the materialized fact table;
-- explicit ::TIMESTAMP cast is required for RANGE framing with INTERVAL bounds.
burst_window as (
    select
        transaction_id,
        count(*) over (
            partition by from_account_id
            order by     created_at::timestamp
            range between interval '2 hours' preceding and current row
        ) as transfers_in_2h_window
    from completed
    where transaction_type = 'TRANSFER'
),

-- All-time per-account amount statistics and same-day activity count.
amount_stats as (
    select
        transaction_id,
        from_account_id,
        amount,
        avg(amount)        over (partition by from_account_id)          as acct_mean_amount,
        stddev_pop(amount) over (partition by from_account_id)          as acct_stddev_amount,
        count(*) over (
            partition by from_account_id,
                         created_at::date
        )                                                               as txns_on_same_day
    from completed
),

-- Average daily transaction count across the full history window per account.
daily_avg as (
    select
        from_account_id,
        count(*)::decimal
            / nullif(count(distinct created_at::date), 0)               as avg_daily_txns
    from completed
    group by from_account_id
)

select
    a.transaction_id,
    a.from_account_id,
    a.amount,
    a.acct_mean_amount,
    coalesce(a.acct_stddev_amount, 0)                                   as acct_stddev_amount,
    a.txns_on_same_day,
    d.avg_daily_txns,
    coalesce(b.transfers_in_2h_window, 0)                               as transfers_in_2h_window
from amount_stats a
left join daily_avg   d using (from_account_id)
left join burst_window b using (transaction_id)
