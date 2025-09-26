# Nitish — Data Engineering Portfolio (AWS • Python • Spark)

This repository showcases end‑to‑end data engineering projects aligned with AWS-first architectures and Python/PySpark.
Each project includes runnable code stubs, IaC/config templates, and concise READMEs explaining design choices,
trade‑offs, and how to run locally or on AWS.

> Tip: Start by skimming **/docs/portfolio_showcase.md** for how to present this portfolio on GitHub/LinkedIn/Resume.

## Projects
- [`projects/01-batch-s3-glue-emr/`](projects/01-batch-s3-glue-emr) — Batch ETL on S3 with Glue‑style PySpark, partitioning, Parquet, and Athena DDL.
- [`projects/02-streaming-kinesis-lambda/`](projects/02-streaming-kinesis-lambda) — Streaming events with Kinesis → Lambda → S3 (with a local simulator).
- [`projects/03-redshift-warehouse/`](projects/03-redshift-warehouse) — Dimensional model in Redshift, staging → dims/facts, stored procedures, and incremental loads.
- [`projects/04-orchestration-airflow/`](projects/04-orchestration-airflow) — Airflow DAGs orchestrating batch + dq checks and external task dependencies.
- [`projects/05-data-quality-gx/`](projects/05-data-quality-gx) — Great Expectations suite and checkpoints for table/file expectations.

## Repo highlights
- **Python 3.10+**, **PySpark 3.4+**, **Great Expectations**, **Airflow 2.9+** (MWAA compatible DAGs), **Redshift SQL**.
- **GitHub Actions**: Linting (flake8), formatting (black), unit tests (pytest), and basic static checks on DAGs.
- **/data/**: small synthetic datasets (CSV/JSON/Parquet) for local runs.
- **/docs/**: design notes, diagrams (PlantUML), and a showcase guide.

---

© 2025 Nitish Burma. MIT License.
