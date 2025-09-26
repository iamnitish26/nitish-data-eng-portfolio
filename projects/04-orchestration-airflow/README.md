# 04 — Airflow Orchestration (MWAA-compatible)

**Goal:** Orchestrate batch ETL + data quality with sensors and external dependencies.

## Local lint/test
```bash
python -m venv .venv && source .venv/bin/activate
pip install apache-airflow==2.9.3
python -m pip install -r requirements.txt
flake8 dags && python tests/test_dag_imports.py
```
