# Dataset: Online Retail II

## Source
UCI Machine Learning Repository / Kaggle
https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset

## Alternative direct source
https://archive.ics.uci.edu/dataset/502/online+retail+ii

## About
- ~1M+ transaction records from a UK-based online retailer
- Date range: 01/12/2009 to 09/12/2011
- Columns: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

## Download Steps
1. Go to the Kaggle link above
2. Download the Excel file (online_retail_II.xlsx)
3. Save locally
4. You will upload this to a Databricks Volume using the setup script

## Why This Dataset
- Real transactional data (not synthetic)
- Large enough to test pipeline performance
- Has enough complexity for CDC, data quality checks, and ML
- Contains nulls, negative quantities (returns), and multi-country data
- Safe for public sharing (no PII, open license)
