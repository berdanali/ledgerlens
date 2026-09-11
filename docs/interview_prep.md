# LedgerLens — Interview Preparation

Technical Q&A organized by component. Each answer is written to be delivered in 2–3 minutes in a German data engineering interview.

---

## 1. Architecture & CDC

**Q: Why CDC instead of a nightly batch export?**

Batch exports give you a point-in-time snapshot — you see the current state but you miss every intermediate change that happened during the day. If a customer's account balance goes from 1,000 to 50,000 and back to 1,000 between two batch runs, the export sees nothing unusual. CDC reads PostgreSQL's Write-Ahead Log (WAL) and captures every INSERT, UPDATE, and DELETE with a `before`/`after` payload. In a financial context this matters for three reasons: (1) near-real-time freshness without polling the production DB, (2) a complete audit trail of every state transition, and (3) the ability to detect patterns like rapid sequential transfers that only appear in the event stream, not in snapshots.

---

**Q: What does Debezium actually do, and why Kafka Connect specifically?**

Debezium is a Kafka Connect source connector. It creates a PostgreSQL logical replication slot, subscribes to the WAL via the `pgoutput` plugin, and translates each WAL entry into a structured Kafka message with a Debezium envelope (`before`, `after`, `op`, `ts_ms`). Kafka Connect handles the operational concerns — restart on failure, offset tracking, distributed workers — so Debezium only has to deal with the CDC logic. The `pgoutput` plugin is the PostgreSQL-native choice (no third-party extension needed, available since PG 10).

---

**Q: What happens if the consumer goes down for an hour?**

Kafka retains messages up to the configured retention period (7 years for transaction topics in this project). When the consumer restarts, it resumes from the last committed offset. Offsets are committed only *after* a successful MinIO upload, not when the message is received — this gives at-least-once delivery semantics. The consumer may process some messages twice if it crashes between upload and commit; duplicates are handled downstream in dbt by deduplicating on `(_kafka_partition, _kafka_offset)` before any business logic.

---

## 2. Landing Zone & Storage

**Q: Why Parquet and MinIO? Why not write directly to a database?**

The landing zone is intentionally raw and append-only. If a transformation bug is discovered later, you can rebuild the entire mart layer from the Parquet files without re-extracting from the OLTP system. Writing directly to a database would couple the ingestion pipeline to the transformation layer — a schema migration in the mart would require coordinating with the consumer. Parquet + Hive partitioning (`year=/month=/day=/hour=`) also makes it cheap to read only the time range you need; DuckDB's partition pruning skips irrelevant folders automatically.

---

**Q: What is the DuckDB lakehouse pattern, and what are its limits?**

dbt-duckdb with the `httpfs` extension reads Parquet files directly from MinIO over the S3 protocol — no ETL load step, no separate warehouse service. DuckDB is an in-process OLAP engine that runs as a library. For this project (~100k transactions) it's instantaneous. The practical ceiling is roughly tens of billions of rows on a single node with enough RAM. Beyond that, the natural upgrade paths are MotherDuck (DuckDB-as-a-service with horizontal scale) or replacing DuckDB with Trino/Spark while keeping the same Parquet/S3 storage layer — the storage and compute are already decoupled by design.

---

**Q: DuckDB has a single-writer lock. How does this interact with Airflow?**

DuckDB allows multiple concurrent readers but only one writer at a time. Airflow runs `dbt run` (write) on a schedule; the `docker` profile target writes to `/opt/airflow/ledgerlens.duckdb` inside the Airflow container's named volume. The `dev` profile (Windows host) writes to `dbt_project/ledgerlens.duckdb` on the local filesystem — a separate file. The two targets never contend. For ad-hoc queries during a running dbt job, the safe approach is `duckdb.connect(read_only=True)`.

---

## 3. dbt Transformations

**Q: Why SCD Type 2 for dim_accounts and Type 1 for dim_customers?**

These are two different answers to the question "how long should we keep history?"

`dim_accounts` uses Type 2 (new row per change, `valid_from`/`valid_to`/`is_current`/`version_number`) because the balance and status history is operationally meaningful. An AML investigation needs to know what the balance was *at the time of a suspicious transaction*, not just what it is today. Type 2 is also the direct demonstration of CDC's value over batch: a batch export gives you one row with today's balance; CDC gives you the full timeline.

`dim_customers` uses Type 1 (overwrite in place) deliberately for GDPR. Keeping a history of past names or email addresses extends the period during which personal data is retained. With Type 1 there is exactly one row per customer to update or purge on an erasure request.

---

**Q: How does int_account_changes work?**

It uses `LAG()` window functions to compare each CDC event's `balance` and `status` against the previous event for the same `account_id`. A row is flagged as `is_change = true` when: (a) it's the first event for that account, (b) the balance changed, or (c) the status changed. Only `is_change = true` rows flow into `dim_accounts` as new SCD Type 2 versions. This filters out Debezium snapshot re-reads and retries that would otherwise create false "new versions" for accounts that didn't actually change.

---

**Q: What does union_by_name = true do in the read_parquet() calls?**

PyArrow infers the schema of each Parquet batch independently from the data in that batch. A batch of small transactions infers `amount` as `DECIMAL(6,2)`; a batch with larger values infers `DECIMAL(7,2)`. Without `union_by_name`, DuckDB tries to stack files with mismatched schemas and raises a type error. `union_by_name = true` tells DuckDB to unify columns by name across files rather than by position, and use the widest compatible type. The explicit `::DECIMAL(18,2)` cast in each staging model then pins the final type regardless of what was inferred.

---

## 4. Anomaly Detection

**Q: Why SQL rules instead of a machine learning model?**

Two reasons, one technical and one regulatory.

Technical: AutoPulse (my other portfolio project) already demonstrates unsupervised ML with Isolation Forest and W&B experiment tracking. Using the same approach here would be redundant. Rule-based detection is also the industry-standard *first layer* in AML pipelines — ML is typically a second-pass enrichment on top of pre-filtered candidates, not the primary gate.

Regulatory: BaFin audit requirements mean every suspicious transaction flag must answer the question "why is this flagged?" An Isolation Forest anomaly score cannot be explained to a compliance officer or submitted as evidence in an investigation. A SQL rule (`15 transfers from the same account in 2 hours`) can. This is a deliberate architecture choice, not a limitation.

---

**Q: Walk me through the BURST_TRANSFER rule.**

```sql
COUNT(*) OVER (
    PARTITION BY from_account_id
    ORDER BY created_at::TIMESTAMP
    RANGE BETWEEN INTERVAL '2 hours' PRECEDING AND CURRENT ROW
) >= 5
```

For each completed TRANSFER, DuckDB counts how many other completed TRANSFERs from the same source account occurred in the 2-hour window ending at (and including) that transaction's timestamp. A rolling window rather than a fixed hourly bucket is used because a fixed bucket would split a burst that straddles a clock hour — 10 transactions from 23:55 to 00:05 would look like 5 per hour, both below the threshold. The rolling window catches it regardless of when it starts.

The threshold of 5 was chosen to match the seed generator's minimum burst size (5–15 transactions per burst). In a production system this would be tuned against a labelled dataset or adjusted per account tier.

---

**Q: The HIGH_VALUE_OUTLIER rule flags 708 accounts. Isn't that too many false positives?**

Yes, and that's an intentional design decision made visible. The threshold (`amount > 4×mean + 2×stddev`) is deliberately permissive as a first gate. In a real AML pipeline this would feed an analyst queue, not an automatic block. The tuning levers are: raise the multiplier (6× instead of 4×), add an absolute floor (`amount > 1,000 EUR`), or replace the mean/stddev with a rolling percentile. The point the project demonstrates is the detection *pattern*, not the production-ready threshold — which would require a labelled dataset of confirmed fraud cases to calibrate against.

---

## 5. Airflow Orchestration

**Q: Why SequentialExecutor and SQLite instead of LocalExecutor + Postgres?**

LocalExecutor + Postgres would add two more services to docker-compose (separate metadata DB + init container). This DAG is inherently sequential — `dbt_run` must complete before `dbt_test`. SequentialExecutor processes one task at a time, which is exactly the right model. The portfolio value is in the DAG design (dependency declaration, failure callbacks, schedule), not in the executor backend. A real production deployment would use CeleryExecutor or KubernetesExecutor with a proper metadata DB — the DAG code is identical regardless.

---

**Q: What does the on_failure_callback actually do? Why not use email alerts?**

```python
def _on_failure(context: dict) -> None:
    log.error(
        "LedgerLens | TASK FAILED | dag=%s task=%s run_id=%s ...",
        ti.dag_id, ti.task_id, context["dag_run"].run_id, ...
    )
```

It writes a structured ERROR log entry with the DAG name, task ID, run ID, execution date, and exception. In production this log would be shipped to a centralised log aggregator (Datadog, ELK, CloudWatch Logs) where an alert rule can trigger a PagerDuty or Slack notification. Email alerting in Airflow requires `AIRFLOW__SMTP__*` configuration and an SMTP relay — additional infrastructure that doesn't add demonstrable value in a local portfolio stack. The callback pattern is the same; only the delivery channel changes.

---

**Q: Why does the docker Airflow profile use httpfs directly while the dev profile does not?**

Windows Smart App Control blocks DuckDB from downloading and executing the `httpfs` extension DLL because it is an unsigned binary fetched from the internet at runtime. Inside the Linux Docker container this restriction does not apply. This is a real-world portability constraint — not a workaround but a documented limitation of the local dev environment. In CI/CD or any Linux deployment the `docker` profile with `httpfs` is the correct default.

---

## 6. GDPR / BaFin Compliance

**Q: How does your pipeline handle a GDPR right-to-erasure request?**

Currently (global salt): delete the customer from PostgreSQL (Debezium emits `op='d'`, `stg_customers` filters it, customer disappears from `dim_customers`). Kafka and MinIO retain pseudonymized tokens for the 90-day TTL, after which they expire automatically via the lifecycle rules applied by `gdpr/configure_retention.py`.

The gap: tokens are technically still reversible while the global `PSEUDONYM_SALT` exists. The production fix is crypto-shredding: move from one global salt to a per-customer salt stored in a key store. On an erasure request, delete the customer's key. All tokens derived from that key become permanently unlinkable — the Parquet files can remain because they no longer constitute personal data. The files age out via the same 90-day lifecycle rule.

The evolution path for `pseudonymizer.py` is documented in CLAUDE.md §8.

---

**Q: Why are transaction records retained for 7 years but customer records for only 90 days?**

BaFin (Bundesanstalt für Finanzdienstleistungsaufsicht) enforces §257 HGB (Handelsgesetzbuch), which requires financial records — including transaction records — to be retained for a minimum of 7 years. Deleting them early is a regulatory violation.

Customer PII is governed by GDPR Article 5(1)(e), the storage limitation principle: personal data must not be kept longer than necessary for the purpose for which it was collected. The bank's relationship with the customer defines "necessary." After the relationship ends, the appropriate retention period is the shorter of the legal minimum and the time needed for dispute resolution — 90 days is a conservative default for the demo.

The tension between "must keep for 7 years" (BaFin) and "must delete on request" (GDPR) is resolved by pseudonymization: the transaction records are kept with tokens, not real identities. With crypto-shredding, destroying the key makes the tokens anonymous — retaining them no longer conflicts with GDPR.

---

**Q: What is the difference between pseudonymization and anonymization under GDPR?**

Under GDPR Recital 26, anonymized data is data from which re-identification is "reasonably impossible." Truly anonymous data falls outside GDPR entirely.

Pseudonymization (Art. 4(5)) replaces identifiers with tokens but retains the ability to re-identify using "additional information" — in our case the `PSEUDONYM_SALT`. As long as the salt exists, the tokens are pseudonymous, not anonymous. GDPR still applies to pseudonymized data.

This distinction matters: our landing zone contains pseudonymized data (still subject to GDPR), not anonymous data. Crypto-shredding crosses the line — once the key is deleted, re-identification is no longer possible by anyone, which makes the retained tokens effectively anonymous under Recital 26.

---

## 7. General Data Engineering

**Q: If you had to scale this to 10× the transaction volume, what would you change first?**

The bottleneck at 10× would likely be the Python consumer's single-threaded flush loop and DuckDB's single-node compute. I'd address them separately:

Consumer: partition the Kafka topics into more partitions and run multiple consumer instances — Kafka's consumer group protocol handles the assignment automatically. Batching parameters (`FLUSH_MAX_MESSAGES`, `FLUSH_INTERVAL_SECONDS`) would be tuned upward.

dbt/DuckDB: at 1 billion+ rows, DuckDB's single-node ceiling starts to matter. The natural upgrade is MotherDuck (managed DuckDB with horizontal read scaling) or replacing DuckDB with Trino pointed at the same MinIO/Parquet storage — the storage layer stays unchanged.

The Kafka and MinIO layers are already horizontally scalable by design; they wouldn't need changes until much higher volumes.

---

**Q: What would you add to this project in a real production environment that you deliberately omitted here?**

Four things:

1. **Auth and network security** — Kafka SASL/SSL, MinIO IAM policies, Airflow connection encryption. Omitted because they add infrastructure complexity without demonstrating new patterns.

2. **Schema evolution strategy** — what happens when a column is added to the `transactions` table? Avro + Schema Registry handles backward-compatible changes automatically, but breaking changes (column rename, type change) require a migration plan. A registry `FULL_TRANSITIVE` compatibility policy would catch breaking changes before they reach production.

3. **Data quality monitoring** — dbt tests catch structural issues (not null, unique, accepted values) but not business logic drift (e.g., average transaction amount drops 90% because of a currency conversion bug). Elementary or re_data would add anomaly detection on the dbt metrics themselves.

4. **Dead letter queue** — the consumer currently retries failed MinIO uploads in-memory. A production consumer would route persistently failing messages to a DLQ topic for manual inspection rather than blocking the main processing loop indefinitely.
