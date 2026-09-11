import duckdb

con = duckdb.connect("ledgerlens.duckdb", read_only=True)

print("=== dim_customers ===")
con.sql("SELECT COUNT(*) as customers FROM dim_customers").show()

print("\n=== dim_accounts (SCD Type 2) ===")
con.sql("""
    SELECT
        COUNT(*)                              AS total_versions,
        COUNT(DISTINCT account_id)            AS unique_accounts,
        SUM(CASE WHEN is_current THEN 1 END)  AS current_versions,
        SUM(CASE WHEN NOT is_current THEN 1 END) AS historical_versions,
        MAX(version_number)                   AS max_versions_per_account
    FROM dim_accounts
""").show()

print("\n=== fact_transactions ===")
con.sql("""
    SELECT transaction_type, status, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS total_amount
    FROM fact_transactions
    GROUP BY transaction_type, status
    ORDER BY cnt DESC
    LIMIT 8
""").show()

print("\n=== SCD Type 2 sample: accounts with multiple versions ===")
con.sql("""
    SELECT account_id, version_number, balance, status, valid_from, is_current
    FROM dim_accounts
    WHERE account_id IN (
        SELECT account_id FROM dim_accounts GROUP BY account_id HAVING COUNT(*) > 1 LIMIT 3
    )
    ORDER BY account_id, version_number
""").show()
