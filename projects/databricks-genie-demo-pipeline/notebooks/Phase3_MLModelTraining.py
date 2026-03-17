# Databricks notebook source
# =============================================================
# Phase 3: ML Model Training with Genie Code
# =============================================================
# OBJECTIVE: Test Genie Code's ML workflow capabilities
# =============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 1. Open Genie Code panel > Agent mode
# MAGIC 2. This phase tests the ML engineer capabilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 1: Customer Segmentation
# MAGIC
# MAGIC ```
# MAGIC Using the gold_customer_summary table in
# MAGIC dev_sandbox.genie_code_demo, build a customer segmentation
# MAGIC model. Use K-means clustering on features like total_orders,
# MAGIC total_revenue, avg_order_value, and customer tenure.
# MAGIC
# MAGIC Find the optimal number of clusters, visualise the segments,
# MAGIC and generate business-friendly persona descriptions.
# MAGIC Log the experiment to MLflow.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did it handle feature scaling?
# MAGIC - Elbow method / silhouette analysis for optimal k?
# MAGIC - MLflow experiment logging quality
# MAGIC - Persona description quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 2: Sales Forecasting
# MAGIC
# MAGIC ```
# MAGIC Train a sales forecasting model using gold_daily_sales.
# MAGIC Predict daily revenue for the next 30 days.
# MAGIC
# MAGIC Try multiple approaches (Prophet, ARIMA, or gradient boosting),
# MAGIC compare their performance using RMSE and MAPE, and log all
# MAGIC experiments to MLflow. Generate forecast visualisations.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Did it handle time series correctly?
# MAGIC - Model comparison approach
# MAGIC - Hyperparameter tuning strategy
# MAGIC - MLflow logging (parameters, metrics, artifacts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 3: Feature Importance Analysis
# MAGIC
# MAGIC ```
# MAGIC From the best forecasting model, show me:
# MAGIC - Feature importance rankings
# MAGIC - SHAP analysis if applicable
# MAGIC - Residual analysis
# MAGIC - What features most impact revenue prediction?
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Explainability approach
# MAGIC - SHAP integration quality
# MAGIC - Business insight from feature importance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 4: Model Registration
# MAGIC
# MAGIC ```
# MAGIC Register the best performing model to Unity Catalog as
# MAGIC dev_sandbox.genie_code_demo.sales_forecast_model.
# MAGIC Add model description, tags, and input/output signature.
# MAGIC ```
# MAGIC
# MAGIC ### What to observe:
# MAGIC - Unity Catalog model registry usage
# MAGIC - Signature generation
# MAGIC - Metadata quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring
# MAGIC
# MAGIC | Prompt | Correctness | ML best practices | MLflow usage | Notes |
# MAGIC |--------|------------|-------------------|-------------|-------|
# MAGIC | Segmentation | /10 | /10 | /10 | |
# MAGIC | Forecasting | /10 | /10 | /10 | |
# MAGIC | Feature importance | /10 | /10 | /10 | |
# MAGIC | Model registration | /10 | /10 | /10 | |
# MAGIC
# MAGIC **Overall Phase 3 Rating: /10**
