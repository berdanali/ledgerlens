-- Grain: one row per (transaction_id, rule_code).
-- A transaction that triggers multiple rules generates one row per rule,
-- allowing downstream queries to ask "which rules fired?" and count by rule.
--
-- rule_score semantics differ by rule:
--   BURST_TRANSFER    → count of transfers in the 2-hour window
--   HIGH_VALUE_OUTLIER → ratio of amount to account mean (e.g., 7.2 = 7.2× mean)
--   DORMANT_SPIKE     → ratio of same-day count to average daily count

with features as (
    select * from {{ ref('int_txn_features') }}
),

fact as (
    select * from {{ ref('fact_transactions') }}
),

-- Rule 1 ─────────────────────────────────────────────────────────────────────
-- BURST_TRANSFER: >= 5 completed TRANSFERs from the same account in 2 hours.
-- Directly targets the synthetic burst pattern seeded into the OLTP data.
-- Regulatory context: high-frequency transfers are a classic smurfing indicator
-- (FATF Recommendation 16 / EU AMLD).
burst as (
    select
        f.transaction_id,
        f.from_account_id,
        f.from_customer_id,
        f.amount,
        f.transaction_type,
        f.status,
        f.created_at,
        'BURST_TRANSFER'                                              as rule_code,
        feat.transfers_in_2h_window::varchar
            || ' transfers from same account in 2-hour window'       as rule_description,
        feat.transfers_in_2h_window::decimal(10, 2)                  as rule_score
    from fact f
    join features feat using (transaction_id)
    where f.transaction_type = 'TRANSFER'
      and f.status           = 'COMPLETED'
      and feat.transfers_in_2h_window >= 5
),

-- Rule 2 ─────────────────────────────────────────────────────────────────────
-- HIGH_VALUE_OUTLIER: amount > 4× mean + 2× stddev for the source account.
-- Threshold calibrated to fire on synthetic burst amounts (5-10× normal) while
-- tolerating occasional large-but-legitimate transactions at the 3-4× level.
high_value as (
    select
        f.transaction_id,
        f.from_account_id,
        f.from_customer_id,
        f.amount,
        f.transaction_type,
        f.status,
        f.created_at,
        'HIGH_VALUE_OUTLIER'                                          as rule_code,
        'Amount ' || round(f.amount, 2)::varchar
            || ' is ' || round(f.amount / nullif(feat.acct_mean_amount, 0), 1)::varchar
            || 'x account mean ('
            || round(feat.acct_mean_amount, 2)::varchar || ')'       as rule_description,
        round(f.amount / nullif(feat.acct_mean_amount, 0), 2)        as rule_score
    from fact f
    join features feat using (transaction_id)
    where f.status = 'COMPLETED'
      and feat.acct_mean_amount > 0
      and f.amount > (feat.acct_mean_amount * 4 + feat.acct_stddev_amount * 2)
),

-- Rule 3 ─────────────────────────────────────────────────────────────────────
-- DORMANT_SPIKE: same-day count >= 5 AND >= 3× the account's average daily rate.
-- Absolute floor of 5 prevents flagging accounts whose average is < 1/day
-- for a completely routine two-transaction day.
-- Catches account-takeover and structuring patterns where a dormant account
-- suddenly becomes highly active.
dormant_spike as (
    select
        f.transaction_id,
        f.from_account_id,
        f.from_customer_id,
        f.amount,
        f.transaction_type,
        f.status,
        f.created_at,
        'DORMANT_SPIKE'                                               as rule_code,
        feat.txns_on_same_day::varchar
            || ' transactions on this day vs avg '
            || round(feat.avg_daily_txns, 1)::varchar
            || '/day'                                                 as rule_description,
        round(feat.txns_on_same_day / nullif(feat.avg_daily_txns, 0), 2) as rule_score
    from fact f
    join features feat using (transaction_id)
    where f.status = 'COMPLETED'
      and feat.avg_daily_txns  > 0
      and feat.txns_on_same_day >= 5
      and feat.txns_on_same_day > feat.avg_daily_txns * 3
),

all_flags as (
    select * from burst
    union all
    select * from high_value
    union all
    select * from dormant_spike
)

select
    md5(transaction_id || '|' || rule_code)  as flag_sk,
    transaction_id,
    from_account_id,
    from_customer_id,
    amount,
    transaction_type,
    status,
    created_at,
    rule_code,
    rule_description,
    rule_score,
    current_timestamp                        as flagged_at
from all_flags
