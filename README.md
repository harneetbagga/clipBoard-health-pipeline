# Clipboard Health — Healthcare Metrics Pipeline

## Overview

End-to-end data pipeline ingesting CMS nursing home staffing and quality data
from Google Drive, transforming it through a Snowflake data warehouse using dbt,
and serving five operational metrics through a Streamlit dashboard.

**Stack:** AWS Glue Python Shell · S3 (Bronze) · Snowflake · dbt Core ·
Apache Airflow · Streamlit · Terraform · GitHub Actions

---

## Architecture

```
Google Drive (CMS files)
        │
        ▼
EventBridge Scheduler ──► Airflow DAG trigger
        │
        ▼
Glue Python Shell Job
  ├── Query Snowflake audit.load_history (watermark check)
  ├── Download new/changed files from Drive API
  └── Write raw CSVs to S3 Bronze
        │
        ▼
Snowflake COPY INTO
  └── S3 Bronze ──► Snowflake RAW schema
        │
        ▼
dbt Core (runs inside Snowflake)
  ├── dbt snapshot  ──► dim_facility (Type 2 SCD)
  ├── staging/      ──► SILVER schema (cleaned + typed)
  ├── intermediate/ ──► joined analytical base tables
  └── marts/        ──► GOLD schema (M1–M5 metric tables)
        │
        ▼
Streamlit Dashboard
  └── Queries Snowflake Gold directly
```

---

## Metrics

| ID | Metric | Grain |
|----|--------|-------|
| M1 | Nurse-to-patient ratio | Facility × Month |
| M2 | Total nursing hours by role | Facility × Month |
| M3 | Contract vs employed staff ratio | Facility (Q2 2024) |
| M4 | Occupancy rate + throughput | Facility (Q2 2024) |
| M5 | Staffing vs readmission correlation | Facility (cross-period) |

---

## Quick start

### Prerequisites

- Python 3.11+
- Terraform 1.7+
- AWS CLI configured
- Snowflake account
- Google Drive service account with Drive API enabled

### 1. Clone and configure

```bash
git clone https://github.com/your-org/healthcare-pipeline.git
cd healthcare-pipeline
cp .env.example .env
# Edit .env with your actual credentials
```

### 2. Install dependencies

```bash
make setup
```

### 3. Provision infrastructure

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your values
make tf-init
make tf-plan   # review changes
make tf-apply  # provision
```

### 4. Run dbt

```bash
make dbt-run
make dbt-test
```

### 5. Validate Airflow DAG

```bash
make dag-validate
```

### 6. Run the dashboard locally

```bash
cd streamlit
pip install -r requirements.txt
streamlit run app.py
```

---

## CI/CD

### GitHub Actions workflows

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `pr_checks.yml` | Every PR to main | Terraform plan · SQLFluff lint · Python lint · dbt build (modified) · DAG validation |
| `deploy.yml` | Merge to main | Terraform apply · dbt snapshot · dbt full build + test · DAG deploy to S3 |

### Required GitHub Secrets

Set these in your repository under **Settings → Secrets → Actions**:

| Secret | Description |
|--------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS IAM user access key |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM user secret key |
| `AWS_REGION` | AWS region e.g. `us-east-1` |
| `S3_BUCKET_NAME` | S3 bucket name for the data lake |
| `GLUE_IAM_ROLE_ARN` | ARN of the IAM role Glue assumes |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake service account username |
| `SNOWFLAKE_PASSWORD` | Snowflake service account password |
| `SNOWFLAKE_ROLE` | Snowflake role for dbt e.g. `TRANSFORMER` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse e.g. `HEALTHCARE_WH_XS` |
| `SNOWFLAKE_DATABASE` | Snowflake database e.g. `HEALTHCARE_METRICS` |

---

## Project structure

```
healthcare_pipeline/
├── .github/workflows/          # GitHub Actions CI/CD
│   ├── pr_checks.yml           # PR: lint + plan + dbt modified + DAG validate
│   └── deploy.yml              # Main: terraform apply + dbt full + DAG deploy
├── terraform/                  # Infrastructure as code
│   ├── snowflake.tf            # Snowflake databases, schemas, warehouses, roles
│   ├── aws_s3.tf               # S3 bucket and folder structure
│   ├── aws_glue.tf             # Glue Python Shell job definition
│   ├── aws_iam.tf              # IAM roles and policies
│   └── aws_secrets.tf          # Secrets Manager secret definitions
├── ingestion/                  # AWS Glue Python Shell job
│   ├── glue_ingestion_job.py   # Main ingestion script
│   └── config.py               # Source file registry
├── dbt_project/                # dbt transformation layer
│   ├── models/staging/         # stg_* — raw to typed/cleaned
│   ├── models/intermediate/    # int_* — joins and enrichment
│   ├── models/marts/           # m1_ through m5_ — Gold metrics
│   └── snapshots/              # dim_facility Type 2 SCD
├── airflow/dags/               # Airflow pipeline DAG
├── streamlit/                  # Dashboard application
└── scripts/                    # Utility scripts
```

---

## Data model

See `SDD_Option3_Snowflake_dbt_Airflow.docx` Section 15 for full data model
documentation including the star schema, Type 2 SCD design, and Gold table schemas.

**Key design decisions:**
- `fact_daily_staffing` at facility + date grain (1.3M rows per quarter)
- `dim_facility` uses dbt snapshot for Type 2 SCD — monthly ProviderInfo changes
  are tracked with `dbt_valid_from` / `dbt_valid_to` columns
- All five Gold metric tables are dbt mart models materialised as Snowflake tables

---

## Data sources

| File | Frequency | Rows | Description |
|------|-----------|------|-------------|
| PBJ_Daily_Nurse_Staffing_Q2_2024.csv | Quarterly | ~1.3M | Daily staffing hours by role per facility |
| NH_ProviderInfo_Oct2024.csv | Monthly | 14,814 | Facility ratings, beds, provider type |
| FY_2024_SNF_VBP_Facility_Performance.csv | Annual | 10,858 | Risk-standardised readmission rates |
| NH_Penalties_Oct2024.csv | Monthly | 28,505 | Fines and payment denials |

---

## Known data quality issues

From pre-build data quality analysis:

| Issue | Rows affected | Resolution |
|-------|--------------|------------|
| MDScensus = 0 | 320 rows | Excluded in stg_pbj with WHERE mds_census > 0 |
| Hrs_LPN outlier (Marigold IL, 13,946 hrs) | 1 row | Excluded in stg_pbj with WHERE hrs_lpn < 5000 |
| Baseline RSRR stored as "---" string | 517 rows | Coerced to NULL with TRY_CAST in staging |
| PROVNUM mixed int/string | ~partial | Zero-padded to 6 chars with LPAD in staging |
| VBP data covers 72.7% of PBJ facilities | 3,969 missing | Documented — M5 covers Medicare-participating SNFs only |

---

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and run `make lint` before committing
3. Open a PR — CI checks run automatically
4. Merge to main triggers full deployment
