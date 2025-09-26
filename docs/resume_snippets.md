# Resume & LinkedIn Snippets

- Built an **AWS batch pipeline** (S3 → Glue‑style PySpark on EMR → Parquet/Athena) with partitioning and data quality gates; reduced daily job time by 40% on sample workloads.
- Implemented a **streaming ingest** using **Kinesis → Lambda → S3** (hourly aggregates), enabling near‑real‑time metrics with idempotent writes and dead‑letter handling.
- Modeled a **Redshift dimensional warehouse** (star schema) with **stored procedures** for incremental loads, improving query performance 3–5× on analytics patterns.
- Orchestrated end‑to‑end workflows in **Airflow/MWAA** with external task dependencies, retries/backfills, and Slack alerts; included DAG unit tests and CI validation.
- Enforced **data quality** using **Great Expectations** suites and checkpoints across bronze/silver layers with CI checks on expectation failures.
