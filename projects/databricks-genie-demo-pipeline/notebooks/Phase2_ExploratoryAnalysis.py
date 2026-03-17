# Databricks notebook source
# =============================================================
# Phase 2: Exploratory Data Analysis with Genie Code
# =============================================================
# OBJECTIVE: Test Genie Code Agent mode for EDA in notebooks
# =============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 1. Open Genie Code panel > Agent mode
# MAGIC 2. Use prompts below sequentially

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 1: Data Profiling
# MAGIC
# MAGIC ```
# MAGIC Profile the table dev_sandbox.genie_code_demo.silver_transactions.
# MAGIC Show me: row count, column types, null percentages, unique value
# MAGIC counts, and basic statistics for numeric columns. Visualise the
# MAGIC distribution of total_amount and flag any outliers.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did it find the Silver table via Unity Catalog?
# MAGIC - Quality of profiling output?
# MAGIC - Did it generate charts or just text?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 2: Sales Trends
# MAGIC
# MAGIC ```
# MAGIC Using the gold_daily_sales table, show me:
# MAGIC 1. Monthly revenue trend over time
# MAGIC 2. Top 10 countries by total revenue
# MAGIC 3. Day-of-week sales patterns
# MAGIC 4. Any seasonal trends or anomalies
# MAGIC
# MAGIC Generate clear visualisations for each.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Chart quality and type selection
# MAGIC - Did it use appropriate aggregations?
# MAGIC - Insight quality beyond just the visuals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 3: Customer Analysis
# MAGIC
# MAGIC ```
# MAGIC Analyse the gold_customer_summary table:
# MAGIC - Distribution of customer lifetime value
# MAGIC - Cohort analysis by first purchase month
# MAGIC - Identify high-value customer characteristics
# MAGIC - What percentage of revenue comes from the top 20% of customers?
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did it apply Pareto analysis correctly?
# MAGIC - Cohort analysis approach
# MAGIC - Business insight quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 4: Cross-table Analysis
# MAGIC
# MAGIC ```
# MAGIC Join silver_transactions with gold_customer_summary and find:
# MAGIC - Which products do high-value customers buy most?
# MAGIC - Are there products that predict high lifetime value?
# MAGIC - Return rate by customer segment
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did it handle the join correctly using Unity Catalog context?
# MAGIC - SQL quality and optimisation
# MAGIC - Analytical depth

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring
# MAGIC
# MAGIC | Prompt | Correctness | Insight quality | Visual quality | Notes |
# MAGIC |--------|------------|-----------------|----------------|-------|
# MAGIC | Data profiling | /10 | /10 | /10 | |
# MAGIC | Sales trends | /10 | /10 | /10 | |
# MAGIC | Customer analysis | /10 | /10 | /10 | |
# MAGIC | Cross-table | /10 | /10 | /10 | |
# MAGIC
# MAGIC **Overall Phase 2 Rating: /10**
