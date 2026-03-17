# Genie Code Retail Pipeline: Hands-On Review

A hands-on evaluation of Databricks **Genie Code** (launched March 11, 2026) — testing whether it can autonomously build a production-grade retail analytics pipeline from natural language prompts.

## Objective

Test Genie Code across 5 phases of real data engineering work and document what works, what doesn't, and what it means for the data engineering craft.

## Architecture

```
Raw CSV (Kaggle retail data)
        │
        ▼
┌─────────────────────────────────────────┐
│  Phase 1: Data Engineering              │
│  Lakeflow Declarative Pipeline          │
│  Bronze → Silver → Gold (Medallion)     │
│  Auto Loader, CDC, Data Quality         │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  Phase 2: Exploratory Analysis          │
│  Genie Code Agent Mode (Notebook)       │
│  Profiling, Visualisations, Insights    │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  Phase 3: ML Model Training             │
│  Sales Forecasting / Customer Segments  │
│  MLflow Experiment Logging              │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  Phase 4: Dashboard Generation          │
│  AI/BI Dashboard from Gold Layer        │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  Phase 5: Genie Conversation API        │
│  Natural Language Queries via API       │
│  Programmatic Integration               │
└─────────────────────────────────────────┘
```

## Dataset

**Online Retail II** dataset from UCI / Kaggle — 1M+ transactions from a UK-based online retailer (2009-2011). Contains invoices, stock codes, descriptions, quantities, prices, customer IDs and countries.

Safe for public sharing. No proprietary or PII data.

## Prerequisites

- Databricks workspace (AWS / Azure / GCP) with Unity Catalog enabled
- Genie Code enabled (workspace admin setting)
- Partner-powered AI features enabled
- Genie Code Agent mode preview enabled
- A SQL Pro or Serverless SQL warehouse
- A catalog and schema for the project (e.g., `dev_sandbox.genie_code_demo`)

## Project Structure

```
genie-code-retail-pipeline/
├── README.md
├── scripts/
│   ├── 00_setup_catalog.sql          # Unity Catalog setup
│   └── 01_load_raw_data.py           # Upload CSV to Volume, create Bronze table
├── notebooks/
│   ├── Phase1_DataEngineering.py     # Genie Code prompts for Lakeflow pipeline
│   ├── Phase2_ExploratoryAnalysis.py # Genie Code prompts for EDA
│   ├── Phase3_MLModelTraining.py     # Genie Code prompts for ML
│   ├── Phase4_Dashboard.md           # Instructions for dashboard generation
│   └── Phase5_GenieAPI.py            # Genie Conversation API client
├── data/
│   └── download_instructions.md      # Where to get the dataset
└── docs/
    ├── genie_code_observations.md    # Log your findings here
    └── linkedin_post_2_template.md   # Post 2 template
```

## How to Use This Repo

### 1. Setup (15 mins)
- Run `scripts/00_setup_catalog.sql` in SQL Editor to create catalog, schema, and volume
- Download dataset from Kaggle (see `data/download_instructions.md`)
- Run `scripts/01_load_raw_data.py` to upload and create Bronze table

### 2. Phase 1: Data Engineering (30-45 mins)
- Open `notebooks/Phase1_DataEngineering.py` in Databricks
- Open Genie Code panel (top-right icon)
- Switch to Agent mode
- Use the prompts listed in the notebook
- Document observations in `docs/genie_code_observations.md`

### 3. Phase 2-5: Follow each notebook
Each notebook contains the exact prompts to use with Genie Code, what to look for, and what to document.

## Genie Code Prompts Used

Each phase documents the exact prompts given to Genie Code and the results. This allows others to reproduce the evaluation.

## Findings

See `docs/genie_code_observations.md` for detailed findings after testing.

## LinkedIn Series

This project is part of a 2-post LinkedIn series evaluating Genie Code:
- **Post 1**: Announcement analysis (published)
- **Post 2**: Hands-on review with findings (after testing)

## Author

**Nitish Kumar Behera**
Staff Data Engineer | Databricks on Azure | Unity Catalog
