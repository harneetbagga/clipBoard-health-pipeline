-- =============================================================================
-- Clipboard Health — Healthcare Metrics Pipeline
-- Snowflake Setup Script
--
-- Run this ONCE as ACCOUNTADMIN in your Snowflake worksheet.
-- Replace all <PLACEHOLDER> values before running.
-- Single admin user setup — no role segregation needed.
-- =============================================================================

USE ROLE SYSADMIN;


-- =============================================================================
-- SECTION 1: WAREHOUSES
-- Two warehouses — one for transforms, one for queries.
-- Auto-suspend keeps costs near zero when idle.
-- See docs/tradeoffs.md for two-warehouse tradeoff discussion.
-- =============================================================================

-- Transformation warehouse: used by dbt runs and COPY INTO ingestion
CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_WH_XS
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Used by dbt transformations and COPY INTO ingestion';

-- Query warehouse: used by Streamlit dashboard and ad-hoc SQL
-- Kept separate so dashboard queries never queue behind dbt runs
CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_WH_QUERY
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 120
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Used by Streamlit dashboard and analyst SQL queries';


-- =============================================================================
-- SECTION 2: DATABASE AND SCHEMAS
-- One database, five schemas matching the data model layers.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS HEALTHCARE_METRICS
    COMMENT = 'Clipboard Health nursing home metrics data warehouse';

USE DATABASE HEALTHCARE_METRICS;

-- Raw: direct COPY INTO targets — one table per source file, no transforms
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Direct COPY INTO targets from S3 Bronze. Not transformed.';

-- Silver: dbt staging and intermediate models — cleaned, typed, joined
CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'dbt staging and intermediate models. Cleaned and joined.';

-- Gold: dbt mart models — M1-M5 metric tables, dashboard-ready
CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'dbt mart models. M1-M5 metric tables. Dashboard-ready.';

-- Snapshots: dbt Type 2 SCD snapshot tables
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS
    COMMENT = 'dbt snapshot tables. dim_facility Type 2 SCD.';

-- Audit: pipeline operational metadata — load history watermark
CREATE SCHEMA IF NOT EXISTS AUDIT
    COMMENT = 'Pipeline operational metadata. Load history watermark table.';


-- =============================================================================
-- SECTION 3: FILE FORMAT AND EXTERNAL STAGE
-- Connects Snowflake to S3 Bronze for COPY INTO.
-- Replace <S3_BUCKET_NAME>, <AWS_KEY_ID>, <AWS_SECRET_KEY> before running.
-- =============================================================================

USE SCHEMA HEALTHCARE_METRICS.RAW;

-- Standard CSV format for all CMS source files
-- NULL_IF handles all null representations found in DQA
-- ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE allows CMS to add columns
-- without breaking existing loads
CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_METRICS.RAW.CSV_FORMAT
    TYPE                            = 'CSV'
    FIELD_DELIMITER                 = ','
    RECORD_DELIMITER                = '\n'
    SKIP_HEADER                     = 1
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    NULL_IF                         = ('NULL', 'null', 'N/A', 'NA', '', '---')
    EMPTY_FIELD_AS_NULL             = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH  = FALSE
    COMMENT = 'Standard format for CMS CSV files';

-- External stage pointing to S3 Bronze bucket
CREATE STAGE IF NOT EXISTS HEALTHCARE_METRICS.RAW.S3_BRONZE_STAGE
    URL         = 's3://<S3_BUCKET_NAME>/bronze/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<AWS_KEY_ID>'
        AWS_SECRET_KEY = '<AWS_SECRET_KEY>'
    )
    FILE_FORMAT = HEALTHCARE_METRICS.RAW.CSV_FORMAT
    COMMENT     = 'External stage pointing to S3 Bronze landing zone';


-- =============================================================================
-- SECTION 4: RAW SCHEMA TABLES
-- One table per source file — direct COPY INTO targets.
-- All dates loaded as VARCHAR — parsed to DATE in dbt staging.
-- All numeric columns as FLOAT — avoids COPY INTO type mismatch errors.
-- _LOADED_AT and _SOURCE_FILE metadata columns on every table
-- for lineage and debugging.
-- =============================================================================

USE SCHEMA HEALTHCARE_METRICS.RAW;

-- PBJ Daily Nurse Staffing (~1.3M rows per quarter)
CREATE TABLE IF NOT EXISTS RAW_PBJ_STAFFING (
    PROVNUM             VARCHAR(10),
    PROVNAME            VARCHAR(200),
    CITY                VARCHAR(100),
    STATE               VARCHAR(2),
    COUNTY_NAME         VARCHAR(100),
    COUNTY_FIPS         VARCHAR(10),
    CY_QTR              VARCHAR(10),
    WORKDATE            VARCHAR(10),    -- YYYYMMDD integer stored as string, parsed in staging
    MDSCENSUS           FLOAT,
    HRS_RNDON           FLOAT,
    HRS_RNDON_EMP       FLOAT,
    HRS_RNDON_CTR       FLOAT,
    HRS_RNADMIN         FLOAT,
    HRS_RNADMIN_EMP     FLOAT,
    HRS_RNADMIN_CTR     FLOAT,
    HRS_RN              FLOAT,
    HRS_RN_EMP          FLOAT,
    HRS_RN_CTR          FLOAT,
    HRS_LPNADMIN        FLOAT,
    HRS_LPNADMIN_EMP    FLOAT,
    HRS_LPNADMIN_CTR    FLOAT,
    HRS_LPN             FLOAT,
    HRS_LPN_EMP         FLOAT,
    HRS_LPN_CTR         FLOAT,
    HRS_CNA             FLOAT,
    HRS_CNA_EMP         FLOAT,
    HRS_CNA_CTR         FLOAT,
    HRS_NATRN           FLOAT,
    HRS_NATRN_EMP       FLOAT,
    HRS_NATRN_CTR       FLOAT,
    HRS_MEDAIDE         FLOAT,
    HRS_MEDAIDE_EMP     FLOAT,
    HRS_MEDAIDE_CTR     FLOAT,
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(500)
);

-- Drop and recreate RAW_PROVIDER_INFO with all 103 columns
USE SCHEMA HEALTHCARE_METRICS.RAW;

DROP TABLE IF EXISTS RAW_PROVIDER_INFO;

CREATE TABLE IF NOT EXISTS RAW_PROVIDER_INFO (
    -- ── Identity ──────────────────────────────────────────────────────────
    CCN                                             VARCHAR(10),
    PROVIDER_NAME                                   VARCHAR(200),
    PROVIDER_ADDRESS                                VARCHAR(300),
    CITY                                            VARCHAR(100),
    STATE                                           VARCHAR(2),
    ZIP_CODE                                        VARCHAR(10),
    TELEPHONE_NUMBER                                VARCHAR(20),
    PROVIDER_SSA_COUNTY_CODE                        VARCHAR(10),
    COUNTY_PARISH                                   VARCHAR(100),
    OWNERSHIP_TYPE                                  VARCHAR(100),

    -- ── Capacity & census ─────────────────────────────────────────────────
    NUMBER_OF_CERTIFIED_BEDS                        FLOAT,
    AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY             FLOAT,
    AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_FOOTNOTE    VARCHAR(10),

    -- ── Provider classification ───────────────────────────────────────────
    PROVIDER_TYPE                                   VARCHAR(100),
    PROVIDER_RESIDES_IN_HOSPITAL                    VARCHAR(5),
    LEGAL_BUSINESS_NAME                             VARCHAR(200),
    DATE_FIRST_APPROVED                             VARCHAR(20),
    AFFILIATED_ENTITY_NAME                          VARCHAR(200),
    AFFILIATED_ENTITY_ID                            VARCHAR(50),
    CONTINUING_CARE_RETIREMENT_COMMUNITY            VARCHAR(5),
    SPECIAL_FOCUS_STATUS                            VARCHAR(100),
    ABUSE_ICON                                      VARCHAR(5),
    MOST_RECENT_HEALTH_INSPECTION_MORE_THAN_2_YEARS VARCHAR(5),
    PROVIDER_CHANGED_OWNERSHIP_IN_LAST_12_MONTHS    VARCHAR(5),
    WITH_A_RESIDENT_AND_FAMILY_COUNCIL              VARCHAR(50),
    AUTOMATIC_SPRINKLER_SYSTEMS_IN_ALL_REQUIRED_AREAS VARCHAR(50),

    -- ── Overall ratings ───────────────────────────────────────────────────
    OVERALL_RATING                                  FLOAT,
    OVERALL_RATING_FOOTNOTE                         VARCHAR(10),
    HEALTH_INSPECTION_RATING                        FLOAT,
    HEALTH_INSPECTION_RATING_FOOTNOTE               VARCHAR(10),
    QM_RATING                                       FLOAT,
    QM_RATING_FOOTNOTE                              VARCHAR(10),
    LONG_STAY_QM_RATING                             FLOAT,
    LONG_STAY_QM_RATING_FOOTNOTE                    VARCHAR(10),
    SHORT_STAY_QM_RATING                            FLOAT,
    SHORT_STAY_QM_RATING_FOOTNOTE                   VARCHAR(10),
    STAFFING_RATING                                 FLOAT,
    STAFFING_RATING_FOOTNOTE                        VARCHAR(10),
    REPORTED_STAFFING_FOOTNOTE                      VARCHAR(10),
    PHYSICAL_THERAPIST_STAFFING_FOOTNOTE            VARCHAR(10),

    -- ── Reported staffing hours per resident per day ──────────────────────
    REPORTED_NURSE_AIDE_HRS_PER_RESIDENT            FLOAT,
    REPORTED_LPN_HRS_PER_RESIDENT                   FLOAT,
    REPORTED_RN_HRS_PER_RESIDENT                    FLOAT,
    REPORTED_LICENSED_HRS_PER_RESIDENT              FLOAT,
    REPORTED_TOTAL_NURSE_HRS_PER_RESIDENT           FLOAT,
    TOTAL_NURSE_HRS_PER_RESIDENT_WEEKEND            FLOAT,
    RN_HRS_PER_RESIDENT_WEEKEND                     FLOAT,
    REPORTED_PHYSICAL_THERAPIST_HRS_PER_RESIDENT    FLOAT,

    -- ── Turnover ──────────────────────────────────────────────────────────
    TOTAL_NURSING_STAFF_TURNOVER                    FLOAT,
    TOTAL_NURSING_STAFF_TURNOVER_FOOTNOTE           VARCHAR(10),
    REGISTERED_NURSE_TURNOVER                       FLOAT,
    REGISTERED_NURSE_TURNOVER_FOOTNOTE              VARCHAR(10),
    NUMBER_OF_ADMINISTRATORS_WHO_LEFT               FLOAT,
    ADMINISTRATOR_TURNOVER_FOOTNOTE                 VARCHAR(10),

    -- ── Case-mix adjusted staffing ────────────────────────────────────────
    NURSING_CASE_MIX_INDEX                          FLOAT,
    NURSING_CASE_MIX_INDEX_RATIO                    FLOAT,
    CASE_MIX_NURSE_AIDE_HRS_PER_RESIDENT            FLOAT,
    CASE_MIX_LPN_HRS_PER_RESIDENT                   FLOAT,
    CASE_MIX_RN_HRS_PER_RESIDENT                    FLOAT,
    CASE_MIX_TOTAL_NURSE_HRS_PER_RESIDENT           FLOAT,
    CASE_MIX_WEEKEND_TOTAL_NURSE_HRS_PER_RESIDENT   FLOAT,

    -- ── Adjusted staffing ─────────────────────────────────────────────────
    ADJUSTED_NURSE_AIDE_HRS_PER_RESIDENT            FLOAT,
    ADJUSTED_LPN_HRS_PER_RESIDENT                   FLOAT,
    ADJUSTED_RN_HRS_PER_RESIDENT                    FLOAT,
    ADJUSTED_TOTAL_NURSE_HRS_PER_RESIDENT           FLOAT,
    ADJUSTED_WEEKEND_TOTAL_NURSE_HRS_PER_RESIDENT   FLOAT,

    -- ── Rating cycle 1 ───────────────────────────────────────────────────
    RATING_CYCLE_1_STANDARD_SURVEY_HEALTH_DATE      VARCHAR(20),
    RATING_CYCLE_1_TOTAL_HEALTH_DEFICIENCIES        FLOAT,
    RATING_CYCLE_1_STANDARD_HEALTH_DEFICIENCIES     FLOAT,
    RATING_CYCLE_1_COMPLAINT_HEALTH_DEFICIENCIES    FLOAT,
    RATING_CYCLE_1_HEALTH_DEFICIENCY_SCORE          FLOAT,
    RATING_CYCLE_1_NUMBER_OF_HEALTH_REVISITS        FLOAT,
    RATING_CYCLE_1_HEALTH_REVISIT_SCORE             FLOAT,
    RATING_CYCLE_1_TOTAL_HEALTH_SCORE               FLOAT,

    -- ── Rating cycle 2 ───────────────────────────────────────────────────
    RATING_CYCLE_2_STANDARD_SURVEY_HEALTH_DATE      VARCHAR(20),
    RATING_CYCLE_2_TOTAL_HEALTH_DEFICIENCIES        FLOAT,
    RATING_CYCLE_2_STANDARD_HEALTH_DEFICIENCIES     FLOAT,
    RATING_CYCLE_2_COMPLAINT_HEALTH_DEFICIENCIES    FLOAT,
    RATING_CYCLE_2_HEALTH_DEFICIENCY_SCORE          FLOAT,
    RATING_CYCLE_2_NUMBER_OF_HEALTH_REVISITS        FLOAT,
    RATING_CYCLE_2_HEALTH_REVISIT_SCORE             FLOAT,
    RATING_CYCLE_2_TOTAL_HEALTH_SCORE               FLOAT,

    -- ── Rating cycle 3 ───────────────────────────────────────────────────
    RATING_CYCLE_3_STANDARD_SURVEY_HEALTH_DATE      VARCHAR(20),
    RATING_CYCLE_3_TOTAL_HEALTH_DEFICIENCIES        FLOAT,
    RATING_CYCLE_3_STANDARD_HEALTH_DEFICIENCIES     FLOAT,
    RATING_CYCLE_3_COMPLAINT_HEALTH_DEFICIENCIES    FLOAT,
    RATING_CYCLE_3_HEALTH_DEFICIENCY_SCORE          FLOAT,
    RATING_CYCLE_3_NUMBER_OF_HEALTH_REVISITS        FLOAT,
    RATING_CYCLE_3_HEALTH_REVISIT_SCORE             FLOAT,
    RATING_CYCLE_3_TOTAL_HEALTH_SCORE               FLOAT,

    -- ── Summary scores & penalties ────────────────────────────────────────
    TOTAL_WEIGHTED_HEALTH_SURVEY_SCORE              FLOAT,
    NUMBER_OF_FACILITY_REPORTED_INCIDENTS           FLOAT,
    NUMBER_OF_SUBSTANTIATED_COMPLAINTS              FLOAT,
    NUMBER_OF_CITATIONS_INFECTION_CONTROL           FLOAT,
    NUMBER_OF_FINES                                 FLOAT,
    TOTAL_AMOUNT_OF_FINES_IN_DOLLARS                FLOAT,
    NUMBER_OF_PAYMENT_DENIALS                       FLOAT,
    TOTAL_NUMBER_OF_PENALTIES                       FLOAT,

    -- ── Geolocation ───────────────────────────────────────────────────────
    LOCATION                                        VARCHAR(500),
    LATITUDE                                        FLOAT,
    LONGITUDE                                       FLOAT,
    GEOCODING_FOOTNOTE                              VARCHAR(10),

    -- ── Metadata ──────────────────────────────────────────────────────────
    PROCESSING_DATE                                 VARCHAR(20),
    _LOADED_AT                                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE                                    VARCHAR(500)
);


-- Drop and recreate RAW_SNF_VBP with all 20 columns including footnotes
DROP TABLE IF EXISTS RAW_SNF_VBP;

CREATE TABLE IF NOT EXISTS RAW_SNF_VBP (
    -- ── Rankings & identity ───────────────────────────────────────────────
    SNF_VBP_PROGRAM_RANKING                         VARCHAR(20),
    FOOTNOTE_SNF_VBP_PROGRAM_RANKING                VARCHAR(500),
    CCN                                             VARCHAR(10),
    PROVIDER_NAME                                   VARCHAR(200),
    PROVIDER_ADDRESS                                VARCHAR(300),
    CITY                                            VARCHAR(100),
    STATE                                           VARCHAR(2),
    ZIP_CODE                                        VARCHAR(10),

    -- ── Readmission rates ─────────────────────────────────────────────────
    -- Loaded as VARCHAR — CMS stores "---" for suppressed values
    -- TRY_CAST to FLOAT in dbt staging; "---" becomes NULL
    BASELINE_RSRR                                   VARCHAR(20),
    FOOTNOTE_BASELINE_RSRR                          VARCHAR(500),
    PERFORMANCE_RSRR                                FLOAT,
    FOOTNOTE_PERFORMANCE_RSRR                       VARCHAR(500),

    -- ── Scores ────────────────────────────────────────────────────────────
    ACHIEVEMENT_SCORE                               FLOAT,
    FOOTNOTE_ACHIEVEMENT_SCORE                      VARCHAR(500),
    -- Loaded as VARCHAR — "---" for facilities without baseline period
    IMPROVEMENT_SCORE                               VARCHAR(20),
    FOOTNOTE_IMPROVEMENT_SCORE                      VARCHAR(500),
    PERFORMANCE_SCORE                               FLOAT,
    FOOTNOTE_PERFORMANCE_SCORE                      VARCHAR(500),

    -- ── Payment ───────────────────────────────────────────────────────────
    INCENTIVE_PAYMENT_MULTIPLIER                    FLOAT,
    FOOTNOTE_INCENTIVE_PAYMENT_MULTIPLIER           VARCHAR(500),

    -- ── Metadata ──────────────────────────────────────────────────────────
    _LOADED_AT                                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE                                    VARCHAR(500)
);


-- NH Penalties (28,505 rows — monthly refresh)
CREATE TABLE IF NOT EXISTS RAW_PENALTIES (
    CCN                         VARCHAR(10),
    PROVIDER_NAME               VARCHAR(200),
    PROVIDER_ADDRESS            VARCHAR(300),
    CITY                        VARCHAR(100),
    STATE                       VARCHAR(2),
    ZIP_CODE                    VARCHAR(10),
    PENALTY_DATE                VARCHAR(20),
    PENALTY_TYPE                VARCHAR(50),
    FINE_AMOUNT                 FLOAT,
    PAYMENT_DENIAL_START_DATE   VARCHAR(20),
    PAYMENT_DENIAL_LENGTH_DAYS  FLOAT,
    LOCATION                    VARCHAR(500),
    PROCESSING_DATE             VARCHAR(20),
    _LOADED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE                VARCHAR(500)
);


-- =============================================================================
-- SECTION 5: AUDIT SCHEMA — WATERMARK TABLE
-- Single source of truth for all file ingestion state.
-- Replaces DynamoDB — queryable via SQL, permanent history, no expiry.
-- Glue job queries this before downloading each file from Drive.
-- =============================================================================

USE SCHEMA HEALTHCARE_METRICS.AUDIT;

CREATE TABLE IF NOT EXISTS LOAD_HISTORY (
    SOURCE_FILE         VARCHAR(500)    NOT NULL,
    S3_PATH             VARCHAR(1000),
    FILE_MODIFIED_AT    TIMESTAMP_NTZ,
    LOADED_AT           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    ROW_COUNT           INTEGER,
    TARGET_TABLE        VARCHAR(200),
    STATUS              VARCHAR(20)     DEFAULT 'success',
    ERROR_MESSAGE       VARCHAR(2000),
    PIPELINE_RUN_ID     VARCHAR(100),
    CONSTRAINT PK_LOAD_HISTORY PRIMARY KEY (SOURCE_FILE, LOADED_AT)
);


-- =============================================================================
-- SECTION 6: VERIFY SETUP
-- Run these after the script completes to confirm everything was created.
-- =============================================================================

SHOW WAREHOUSES     LIKE 'HEALTHCARE%';
SHOW SCHEMAS        IN DATABASE HEALTHCARE_METRICS;
SHOW STAGES         IN SCHEMA HEALTHCARE_METRICS.RAW;
SHOW TABLES         IN SCHEMA HEALTHCARE_METRICS.RAW;
SHOW TABLES         IN SCHEMA HEALTHCARE_METRICS.AUDIT;

-- =============================================================================
-- SETUP COMPLETE
-- Next steps:
-- 1. Replace <S3_BUCKET_NAME>, <AWS_KEY_ID>, <AWS_SECRET_KEY> before running
-- 2. Run as SYSADMIN in Snowflake worksheet
-- 3. Run: LIST @HEALTHCARE_METRICS.RAW.S3_BRONZE_STAGE to verify S3 connection
-- 4. Store AWS credentials used in the stage in AWS Secrets Manager
-- =============================================================================