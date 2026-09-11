FROM apache/airflow:2.9.0

# dbt-duckdb includes dbt-core; version pinned to match the host dev environment.
# boto3 is NOT needed here — MinIO is reached via DuckDB's built-in httpfs S3 client.
USER airflow
RUN pip install --no-cache-dir "dbt-duckdb>=1.11.0,<2"
