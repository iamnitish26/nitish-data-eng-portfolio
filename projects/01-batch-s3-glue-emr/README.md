

# 01 — Batch ETL: S3 → Glue-style PySpark → Parquet → Athena

**Goal:** Convert raw CSV to curated Parquet partitioned by `dt`, register external tables for analytics.

## Local quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install pyspark==3.4.1 pyarrow==15.0.0
python src/etl_batch.py --input ../../data/raw/trips_sample.csv --output data/curated --dt 2025-09-01

Outputs Parquet under `data/curated/dt=2025-09-01/`.

## AWS outline

* Land raw files in `s3://<bucket>/raw/trips/`.
* Run job on **EMR** or **Glue** (same PySpark code).
* Write to `s3://<bucket>/curated/trips/` partitioned by `dt` in **Parquet**.
* Create/update Athena external table (`sql/athena_trips.sql`).

## Design notes

* Schema-on-read; explicit schema; safe casting; null-tolerant parsing.
* Idempotent writes by partition & overwrite mode; optional `jobbookmark` in Glue.
* Metrics logged to stdout (captured by CloudWatch on EMR/Glue).

---

## 🚀 Advanced Mode (next-level)

### Scale data

Generate 100k–5M rows:

```bash
python src/gen_trips.py --out ../../data/raw/trips_big.csv --rows 1000000 --start 2025-08-25 --days 14
```

### Incremental loads

Process one-day windows with partition overwrite:

```bash
python src/etl_batch_advanced.py --input ../../data/raw/trips_big.csv \
  --output data/curated_advanced --format parquet \
  --since "2025-09-01 00:00:00" --until "2025-09-02 00:00:00" --big
```

### Optional: Iceberg + MERGE/UPSERT

1. Create table: `sql/athena_trips_iceberg.sql` (edit bucket + db).
2. Run with Iceberg:

   ```bash
   python src/etl_batch_advanced.py --format iceberg ...
   ```

   using proper Iceberg catalog configs on EMR/Glue.
3. Supports `MERGE INTO` for upserts by primary key (`trip_id`).

### Orchestration

* Airflow DAG: `projects/04-orchestration-airflow/dags/batch_trips_incremental.py`

### Testing

```bash
pytest projects/01-batch-s3-glue-emr/tests/test_transform.py
```

---

## 🏗 Suggested AWS Productionization

* **EMR Instance Fleet** on **Graviton** (r7g or m7g) + managed scaling.
* S3 layout:

  * `s3://<bucket>/raw/trips/`
  * `s3://<bucket>/lake/trips/` (Iceberg) or `.../curated/` (Parquet).
* **Metrics**: emit Spark job metrics to CloudWatch; alert on skew/spill/executor OOM.
* **DQ**: Great Expectations as a post-step; fail the pipeline on expectation failures.
* **Cost**: Spot instances for core/task nodes, S3 multipart upload & Parquet compression (`snappy`).

---

## 📊 Sample Run & Results

We generated **1M rows** over 14 days with `gen_trips.py`, and ran an **incremental load for 2025-09-01** using `etl_batch_advanced.py`.

### Row count

```bash
rows: 70993
+----------+------+
| dt       |count |
+----------+------+
|2025-09-01|70993 |
+----------+------+
```

### Top cities by trip volume and average fare

```bash
+-----------+------+----------+
| city      |   n  | avg_fare |
+-----------+------+----------+
| London    | 21180| 6.39     |
| Manchester| 10607| 6.45     |
| Birmingham|  8622| 6.51     |
| Liverpool |  7173| 6.47     |
| Watford   |  7033| 6.43     |
| Leeds     |  5782| 6.42     |
| Glasgow   |  5689| 6.38     |
| Bristol   |  4907| 6.37     |
+-----------+------+----------+
```

### Performance

* **1-day window (~70k rows)** processed in ~**10.0 seconds** on MacBook Air (16 partitions).
* Demonstrates scalability beyond the tiny sample dataset.

```


