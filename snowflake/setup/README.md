# Snowflake Setup

Run `01_snowflake_setup.sql` **once** as SYSADMIN before starting the pipeline.

## Prerequisites

- Snowflake account with SYSADMIN access
- S3 bucket already created
- AWS IAM user with S3 read access for the external stage

## Steps

### 1. Replace placeholders in the setup script

Open `01_snowflake_setup.sql` and replace:

| Placeholder | Value |
|-------------|-------|
| `<S3_BUCKET_NAME>` | Your S3 bucket name |
| `<AWS_KEY_ID>` | AWS access key ID with S3 read access |
| `<AWS_SECRET_KEY>` | AWS secret access key |

### 2. Run in Snowflake worksheet

Copy the entire contents into a Snowflake worksheet and run it.
Takes about 30 seconds.

### 3. Verify S3 stage

```sql
LIST @HEALTHCARE_METRICS.RAW.S3_BRONZE_STAGE;
```

Should return empty (no files yet) without errors.

### 4. Update your .env

Add Snowflake connection details to your local `.env` file for dbt.

## What gets created

| Object | Name | Purpose |
|--------|------|---------|
| Warehouse | HEALTHCARE_WH_XS | dbt + COPY INTO |
| Warehouse | HEALTHCARE_WH_QUERY | Dashboard + ad-hoc SQL |
| Database | HEALTHCARE_METRICS | Everything lives here |
| Schema | RAW | COPY INTO targets |
| Schema | SILVER | dbt staging + intermediate |
| Schema | GOLD | M1-M5 metric tables |
| Schema | SNAPSHOTS | dim_facility Type 2 SCD |
| Schema | AUDIT | Load history watermark |
| File format | CSV_FORMAT | CMS CSV parsing rules |
| Stage | S3_BRONZE_STAGE | S3 Bronze connection |
| Table | RAW.RAW_PBJ_STAFFING | PBJ staffing data |
| Table | RAW.RAW_PROVIDER_INFO | NH ProviderInfo |
| Table | RAW.RAW_SNF_VBP | VBP readmission scores |
| Table | RAW.RAW_PENALTIES | NH Penalties |
| Table | AUDIT.LOAD_HISTORY | Ingestion watermark |

## Resetting

```sql
-- Run 02_teardown.sql as SYSADMIN, then re-run 01_snowflake_setup.sql
```
