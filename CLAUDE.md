# CLAUDE.md — LedgerLens

This file is the persistent context for Claude Code on this project. Read it at the start of every session before doing anything else. Keep it updated as the project evolves — when a phase completes or an architectural decision is made, update the relevant section in the same session.

## 1. Project Identity

**Name:** LedgerLens
**One-liner:** A CDC-driven, GDPR/BaFin-aware financial transaction analytics platform — captures changes from an OLTP banking system in real time, models them into a dimensional warehouse via dbt, and flags suspicious transactions.
**Purpose:** Portfolio project for a Data Engineering / AI Engineering job search in Germany. Every architectural choice should be defensible in a technical interview, not just "good enough to run."
**Author:** Berdan — computer engineering graduate, 1 year as junior backend developer, based in Turkey, targeting the German data engineering market.

## 2. Problem Statement

Financial institutions run high-throughput OLTP systems (accounts, transactions) where analytics teams need near-real-time visibility without hammering the production database with queries, and without violating data protection law (GDPR for personal data, BaFin-style retention/audit expectations for financial records in Germany). LedgerLens demonstrates the standard modern answer to this: Change Data Capture (CDC) instead of batch exports or direct querying, a data lake landing zone, and dbt-modeled marts for analytics — with privacy and retention built into the pipeline, not bolted on afterward.

## 3. Architecture

```
PostgreSQL (OLTP)
   │  logical replication (WAL)
   ▼
Debezium (Kafka Connect)
   │  change events (Avro, Schema Registry)
   ▼
Kafka topics (per source table)
   │
   ▼
Landing zone (raw, append-only — MinIO/S3-compatible, Parquet)
   │
   ▼
dbt: staging → intermediate → marts (star schema)
   │
   ├─► fact_transactions, dim_customer, dim_account, dim_date
   ▼
Anomaly / suspicious-transaction flagging layer (on top of marts)
   │
   ▼
Airflow: orchestrates dbt run + dbt test on a schedule
```

**Why CDC instead of batch export:** near-real-time freshness without adding load to the OLTP system; captures deletes and updates, not just inserts; is the pattern production fintech data platforms actually use. This is a deliberate portfolio choice to demonstrate a skill batch-only pipelines don't.

**Why a landing zone before dbt:** raw events are kept immutable and replayable — if a transformation bug is found later, marts can be rebuilt from the landing zone without re-extracting from the source system.

## 4. Tech Stack

- **Source system:** PostgreSQL (seeded with synthetic banking data — customers, accounts, transactions)
- **CDC:** Debezium via Kafka Connect
- **Streaming backbone:** Apache Kafka + Schema Registry (Avro)
- **Landing zone:** MinIO (S3-compatible), Parquet format
- **Transformation:** dbt + DuckDB (reads Parquet files directly from MinIO via the `httpfs` extension — lakehouse pattern; no separate warehouse service needed)
  - **Known limitation:** DuckDB uses a single-writer file lock per database file. If Airflow triggers a `dbt run` while a manual query session is open against the same `.duckdb` file, the second writer will block or error. Mitigation: use read-only connections for ad-hoc queries (`duckdb.connect(read_only=True)`) and ensure Airflow is the sole writer during scheduled runs.
- **Orchestration:** Apache Airflow
- **Language:** Python (type-hinted, OOP where it adds clarity, not for its own sake)
- **Containerization:** Docker Compose (single source of truth for the local stack)

## 5. Repository Structure

```
ledgerlens/
├── docker-compose.yml
├── .env                        # secrets, never committed — see .env.example
├── CLAUDE.md
├── README.md
├── oltp/                       # synthetic source system
│   ├── schema.sql
│   └── seed_generator.py
├── cdc/                        # Debezium connector configs
│   └── connectors/*.json
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   └── dbt_project.yml
├── airflow/
│   └── dags/
├── anomaly/                    # suspicious-transaction detection logic
└── docs/
    └── architecture-diagram.png
```

Adjust this structure as the project develops, but keep it in sync with what actually exists — a stale tree here is worse than no tree.

## 6. Coding Standards

- Python: full type hints on function signatures, `dataclasses` or Pydantic models for structured data, no bare `except:` clauses, structured logging (not `print`).
- SQL / dbt: every model has a corresponding `.yml` with column descriptions and at least one test; no model without a documented grain.
- Every module that touches data must have explicit error handling for the failure modes that matter (connection loss, schema mismatch, malformed record) — not defensive code for its own sake.
- Secrets only ever come from `.env` / environment variables, never hardcoded, never logged.

## 7. Working Process (how Claude Code should operate on this project)

1. Work module by module, in the phase order defined in the kickoff prompt (architecture → OLTP seed → CDC → landing zone → dbt → anomaly detection → Airflow → GDPR/retention → docs). Do not jump ahead without explicit go-ahead.
2. Before writing code for a new module, present 2–3 viable approaches with trade-offs and wait for a decision. Do not pick silently on anything with a real trade-off (e.g. CDC tool choice, file format, retry strategy).
3. After finishing a module, list 2–3 questions a senior data engineer or hiring manager might ask about it — this project doubles as interview prep.
3a. After the interview questions, always include a "Senin Yapman Gerekenler" (Your Action Items) section: a numbered checklist of every command the user must run manually (docker compose, pip install, migrations, etc.) and anything they need to verify or edit by hand. One concrete command or check per item — no vague instructions.
4. When modifying existing files, show only the changed functions/classes/lines (`# ... existing code ...` for the rest) — never repeat a whole file unless explicitly asked.
5. Commit after each completed module with a clear, conventional message. Never batch multiple unrelated changes into one commit.
6. Update this file's "Status" section (below) at the end of a session or phase — future sessions depend on it being accurate.

## 8. Privacy & Compliance Notes

- Customer-identifying fields (name, IBAN, national ID equivalents) must never reach the landing zone or marts in raw form — pseudonymize or tokenize at the earliest point in the pipeline (ideally in the CDC/streaming layer, matching the AutoPulse project's precedent).
- Design for GDPR "right to erasure": since CDC captures an immutable event history, decide explicitly (and document the decision) how a deletion request propagates through Kafka topics, the landing zone, and dbt marts. This is a known hard problem in event-sourced systems — the point of this project is to show you've reasoned about it, not necessarily to solve it perfectly.
- Document data retention periods per layer (raw landing zone vs. marts) as if a compliance officer would ask.

## 9. Status

_(Update this section as work progresses — replace with current state.)_

- [x] Phase 1 — Architecture design (decisions: Python consumer, SHA-256 pseudonymization, DuckDB lakehouse)
- [x] Phase 2 — Synthetic OLTP schema + seed generator (3 tables, CDC-ready, realistic distributions)
- [x] Phase 3 — Debezium/Kafka Connect CDC pipeline (Confluent 7.7, Avro, 3 tables, pgoutput)
- [x] Phase 4 — Landing zone ingestion (confluent-kafka, AvroDeserializer, SHA-256 PII pseudonymization, Parquet/Snappy, MinIO S3 partitioned by table/year/month/day/hour)
- [x] Phase 5 — dbt staging/intermediate/marts + tests
  - Adapter: dbt-duckdb + httpfs, reads Parquet directly from MinIO (S3 endpoint)
  - dim_accounts: SCD Type 2 (valid_from/valid_to/is_current) — balance & status history
    WHY: CDC'nin batch'e göre üstünlüğünü somut olarak gösterir; batch export yalnızca anlık
    bakiyeyi verir, CDC ile tüm değişim geçmişi saklanır. Mülakatta bunu dim_accounts üzerinden anlatacağız.
  - dim_customers: SCD Type 1 (en son durum) — kişisel veri saklama süresini kısaltmak için bilinçli tercih
  - dbt run: manuel (Phase 7'de Airflow üstlenecek)
- [ ] Phase 6 — Anomaly detection layer
- [ ] Phase 7 — Airflow orchestration
- [ ] Phase 8 — GDPR/BaFin retention design
- [ ] Phase 9 — README + architecture diagram + interview notes

## 10. Non-Goals

To keep scope honest for a portfolio project: no real banking data, no production-grade auth/security hardening beyond what's needed to demonstrate the pattern, no multi-region/high-availability design (call out where it *would* matter in the README instead of building it).
