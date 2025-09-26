# 05 — Data Quality with Great Expectations

**Goal:** Validate curated Parquet using expectations for schema, ranges, and nulls.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install great_expectations pandas pyarrow fastparquet
python run_checks.py --parquet ../01-batch-s3-glue-emr/data/curated
```
