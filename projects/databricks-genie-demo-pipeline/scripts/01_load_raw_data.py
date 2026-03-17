# Databricks notebook source
# =============================================================
# Genie Code Retail Pipeline: Load Raw Data
# =============================================================
# This notebook loads the Online Retail II dataset into Unity Catalog
# Run this BEFORE starting the Genie Code phases
# =============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Upload the dataset
# MAGIC
# MAGIC Upload `online_retail_II.xlsx` to:
# MAGIC `/Volumes/dev_sandbox/genie_code_demo/raw_data/`
# MAGIC
# MAGIC You can do this via:
# MAGIC - Databricks UI: Catalog > Volumes > Upload
# MAGIC - Or drag and drop in the Volume browser

# COMMAND ----------

# Configuration
CATALOG = "dev_sandbox"
SCHEMA = "genie_code_demo"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"

# COMMAND ----------

# Read the Excel file
import pandas as pd

file_path = f"{VOLUME_PATH}/online_retail_II.xlsx"

# Read both sheets (Year 2009-2010 and Year 2010-2011)
df_2009 = pd.read_excel(file_path, sheet_name="Year 2009-2010")
df_2010 = pd.read_excel(file_path, sheet_name="Year 2010-2011")

# Combine
pdf = pd.concat([df_2009, df_2010], ignore_index=True)
print(f"Total records loaded: {len(pdf):,}")
print(f"Columns: {list(pdf.columns)}")

# COMMAND ----------

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(pdf)

# Add ingestion metadata
from pyspark.sql import functions as F

spark_df = spark_df.withColumn("_ingested_at", F.current_timestamp()) \
                   .withColumn("_source_file", F.lit("online_retail_II.xlsx"))

# COMMAND ----------

# Write as Bronze table (raw, append-only)
spark_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"Bronze table created: {BRONZE_TABLE}")
print(f"Record count: {spark.table(BRONZE_TABLE).count():,}")

# COMMAND ----------

# Quick validation
display(spark.table(BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Bronze table is ready. Now open `Phase1_DataEngineering.py` and use
# MAGIC Genie Code Agent mode to build the Silver and Gold layers.
