# 01 — Batch ETL: S3 → Glue‑style PySpark → Parquet → Athena

**Goal:** Convert raw CSV to curated Parquet partitioned by `dt`, register external tables for analytics.

## Local quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install pyspark==3.4.1 pyarrow==15.0.0
python src/etl_batch.py --input ../../data/raw/trips_sample.csv --output data/curated --dt 2025-09-01
```
Outputs Parquet under `data/curated/dt=2025-09-01/`.

## AWS outline
- Land raw files in `s3://<bucket>/raw/trips/`.
- Run job on **EMR** or **Glue** (same PySpark code).
- Write to `s3://<bucket>/curated/trips/` partitioned by `dt` in **Parquet**.
- Create/update Athena external table (`sql/athena_trips.sql`).

## Design notes
- Schema-on-read; explicit schema; safe casting; null‑tolerant parsing.
- Idempotent writes by partition & overwrite mode; optional `jobbookmark` in Glue.
- Metrics logged to stdout (captured by CloudWatch on EMR/Glue).
