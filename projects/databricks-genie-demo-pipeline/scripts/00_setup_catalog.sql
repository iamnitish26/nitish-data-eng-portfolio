-- =============================================================
-- Genie Code Retail Pipeline: Unity Catalog Setup
-- =============================================================
-- Run this in Databricks SQL Editor
-- Update the catalog name if your workspace uses a different convention
-- =============================================================

-- 1. Create catalog (skip if using an existing one)
-- CREATE CATALOG IF NOT EXISTS dev_sandbox;

-- 2. Create schema
CREATE SCHEMA IF NOT EXISTS dev_sandbox.genie_code_demo
COMMENT 'Schema for Genie Code hands-on evaluation project';

-- 3. Create volume for raw data uploads
CREATE VOLUME IF NOT EXISTS dev_sandbox.genie_code_demo.raw_data
COMMENT 'Volume for uploading raw CSV/Excel files';

-- 4. Verify setup
SHOW SCHEMAS IN dev_sandbox LIKE 'genie_code_demo';
SHOW VOLUMES IN dev_sandbox.genie_code_demo;

-- =============================================================
-- NOTES:
-- After running this, upload the Online Retail II dataset
-- to the volume using the UI or the load script:
--   /Volumes/dev_sandbox/genie_code_demo/raw_data/
-- =============================================================
