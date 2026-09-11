import duckdb

con = duckdb.connect("ledgerlens.duckdb", read_only=True)

print("=== Flags by rule ===")
con.sql("""
    SELECT
        rule_code,
        COUNT(*)                                        AS flag_count,
        COUNT(DISTINCT transaction_id)                  AS unique_txns,
        COUNT(DISTINCT from_account_id)                 AS unique_accounts,
        ROUND(MIN(rule_score), 2)                       AS min_score,
        ROUND(AVG(rule_score), 2)                       AS avg_score,
        ROUND(MAX(rule_score), 2)                       AS max_score
    FROM mart_suspicious_transactions
    GROUP BY rule_code
    ORDER BY flag_count DESC
""").show()

print("\n=== Accounts hit by multiple rules (top 10) ===")
con.sql("""
    SELECT
        from_account_id,
        COUNT(DISTINCT rule_code)   AS rules_fired,
        COUNT(*)                    AS total_flags,
        STRING_AGG(DISTINCT rule_code, ', ' ORDER BY rule_code) AS rules
    FROM mart_suspicious_transactions
    GROUP BY from_account_id
    HAVING COUNT(DISTINCT rule_code) > 1
    ORDER BY rules_fired DESC, total_flags DESC
    LIMIT 10
""").show()

print("\n=== Sample BURST_TRANSFER flags ===")
con.sql("""
    SELECT
        transaction_id,
        from_account_id,
        ROUND(amount, 2) AS amount,
        created_at,
        rule_score AS transfers_in_2h,
        rule_description
    FROM mart_suspicious_transactions
    WHERE rule_code = 'BURST_TRANSFER'
    ORDER BY rule_score DESC
    LIMIT 5
""").show()

print("\n=== Sample HIGH_VALUE_OUTLIER flags ===")
con.sql("""
    SELECT
        transaction_id,
        ROUND(amount, 2)        AS amount,
        ROUND(rule_score, 2)    AS amount_vs_mean,
        rule_description
    FROM mart_suspicious_transactions
    WHERE rule_code = 'HIGH_VALUE_OUTLIER'
    ORDER BY rule_score DESC
    LIMIT 5
""").show()
