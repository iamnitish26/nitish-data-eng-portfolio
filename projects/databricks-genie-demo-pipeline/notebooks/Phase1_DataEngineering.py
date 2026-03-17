# Databricks notebook source
# =============================================================
# Phase 1: Data Engineering with Genie Code
# =============================================================
# OBJECTIVE: Test Genie Code's ability to build a Lakeflow
# Declarative Pipeline with medallion architecture
# =============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC 1. Open the Genie Code panel (icon in top-right)
# MAGIC 2. Switch to **Agent** mode (bottom of panel)
# MAGIC 3. Use the prompts below one by one
# MAGIC 4. Document what Genie Code generates vs what you expected
# MAGIC 5. Note any manual corrections needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 1: Build the Lakeflow Pipeline
# MAGIC
# MAGIC **Copy this into Genie Code:**
# MAGIC
# MAGIC ```
# MAGIC I have a Bronze table at dev_sandbox.genie_code_demo.bronze_transactions
# MAGIC with retail transaction data containing columns: Invoice, StockCode,
# MAGIC Description, Quantity, InvoiceDate, Price, Customer ID, Country,
# MAGIC _ingested_at, _source_file.
# MAGIC
# MAGIC Build a Lakeflow Spark Declarative Pipeline with:
# MAGIC - Silver layer: clean nulls, filter negative quantities as returns,
# MAGIC   cast data types, add a total_amount column (Quantity * Price),
# MAGIC   deduplicate by Invoice + StockCode
# MAGIC - Gold layer: two tables
# MAGIC   1. gold_daily_sales: daily aggregated sales by country
# MAGIC   2. gold_customer_summary: customer lifetime value, total orders,
# MAGIC      avg order value, first/last purchase date
# MAGIC
# MAGIC Include data quality expectations on the Silver layer.
# MAGIC Write to dev_sandbox.genie_code_demo schema.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did it create proper DLT/Lakeflow syntax?
# MAGIC - Did it handle the returns (negative Quantity) correctly?
# MAGIC - What data quality expectations did it add?
# MAGIC - Did it understand medallion architecture patterns?
# MAGIC - Did it configure Auto Loader or use static reads?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 2: Add Data Quality Expectations
# MAGIC
# MAGIC **Copy this into Genie Code:**
# MAGIC
# MAGIC ```
# MAGIC Add stricter data quality expectations to the Silver layer:
# MAGIC - Invoice should not be null
# MAGIC - Price should be greater than 0 for non-return transactions
# MAGIC - Customer ID should not be null
# MAGIC - Quantity should not be 0
# MAGIC - InvoiceDate should be a valid timestamp
# MAGIC
# MAGIC Use expect_or_drop for critical checks and expect for warnings.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did it use the correct DLT expectations syntax?
# MAGIC - Did it differentiate between expect and expect_or_drop?
# MAGIC - Did it handle the returns edge case properly?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 3: Add Schema Evolution Handling
# MAGIC
# MAGIC **Copy this into Genie Code:**
# MAGIC
# MAGIC ```
# MAGIC Modify the pipeline to handle schema evolution gracefully.
# MAGIC If new columns appear in the source data, they should be
# MAGIC captured in Bronze and ignored in Silver unless explicitly mapped.
# MAGIC Add appropriate merge schema options.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - How did it handle schema evolution?
# MAGIC - Did it use mergeSchema option?
# MAGIC - Did it add any rescue columns?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 4: Test Pipeline Debugging
# MAGIC
# MAGIC Intentionally introduce an error (e.g., wrong column name) and ask:
# MAGIC
# MAGIC ```
# MAGIC The pipeline is failing with an AnalysisException.
# MAGIC Can you investigate and fix the issue?
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did Genie Code correctly identify the error?
# MAGIC - Did it propose the right fix?
# MAGIC - Did it need multiple attempts?
# MAGIC - How does this compare to debugging manually?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring
# MAGIC
# MAGIC Rate each prompt result (1-10):
# MAGIC
# MAGIC | Prompt | Correctness | Production-readiness | Speed vs manual | Notes |
# MAGIC |--------|------------|---------------------|-----------------|-------|
# MAGIC | Pipeline build | /10 | /10 | /10 | |
# MAGIC | Data quality | /10 | /10 | /10 | |
# MAGIC | Schema evolution | /10 | /10 | /10 | |
# MAGIC | Debugging | /10 | /10 | /10 | |
# MAGIC
# MAGIC **Overall Phase 1 Rating: /10**
