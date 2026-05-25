-- =============================================================================
-- Clipboard Health — Healthcare Metrics Pipeline
-- Snowflake TEARDOWN Script
--
-- WARNING: Drops all pipeline infrastructure including all data.
-- Only run this to fully reset the environment.
-- =============================================================================

USE ROLE SYSADMIN;

-- Drop database (cascades to all schemas and tables)
DROP DATABASE  IF EXISTS HEALTHCARE_METRICS;
DROP WAREHOUSE IF EXISTS HEALTHCARE_WH_XS;
DROP WAREHOUSE IF EXISTS HEALTHCARE_WH_QUERY;

-- Verify everything is gone
SHOW DATABASES  LIKE 'HEALTHCARE%';
SHOW WAREHOUSES LIKE 'HEALTHCARE%';
