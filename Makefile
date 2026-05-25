.PHONY: help setup install lint test dbt-run dbt-test dbt-docs \
        tf-init tf-plan tf-apply dag-validate clean

# ── Default target ───────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Clipboard Health — Healthcare Metrics Pipeline"
	@echo "=============================================="
	@echo ""
	@echo "Setup:"
	@echo "  make setup          Install all dependencies and configure environment"
	@echo ""
	@echo "Development:"
	@echo "  make lint           Run SQLFluff (dbt) + flake8 (Python)"
	@echo "  make dbt-run        Run all dbt models"
	@echo "  make dbt-test       Run all dbt tests"
	@echo "  make dbt-docs       Generate and serve dbt documentation"
	@echo "  make dag-validate   Validate Airflow DAG imports"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make tf-init        Initialize Terraform"
	@echo "  make tf-plan        Show infrastructure changes (dry run)"
	@echo "  make tf-apply       Apply infrastructure changes"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean          Remove generated files and caches"
	@echo ""

# ── Setup ────────────────────────────────────────────────────────────────────
setup:
	@echo "Setting up local environment..."
	@cp -n .env.example .env || true
	pip install dbt-snowflake sqlfluff flake8 apache-airflow snowflake-connector-python
	cd dbt_project && dbt deps
	@echo "Done. Edit .env with your actual credentials before running."

install:
	pip install -r ingestion/requirements.txt
	pip install -r airflow/requirements.txt
	pip install -r streamlit/requirements.txt
	cd dbt_project && dbt deps

# ── Linting ──────────────────────────────────────────────────────────────────
lint: lint-sql lint-python

lint-sql:
	@echo "Running SQLFluff on dbt models..."
	cd dbt_project && sqlfluff lint models/ --dialect snowflake

lint-python:
	@echo "Running flake8 on Python files..."
	flake8 ingestion/ airflow/ scripts/ --max-line-length=100 --exclude=__pycache__

# ── dbt ──────────────────────────────────────────────────────────────────────
dbt-run:
	@echo "Running all dbt models..."
	cd dbt_project && dbt run --profiles-dir . --target prod

dbt-run-modified:
	@echo "Running modified dbt models only..."
	cd dbt_project && dbt run --profiles-dir . --target prod --select state:modified+

dbt-test:
	@echo "Running all dbt tests..."
	cd dbt_project && dbt test --profiles-dir . --target prod

dbt-test-modified:
	cd dbt_project && dbt test --profiles-dir . --target prod --select state:modified+

dbt-snapshot:
	@echo "Running dbt snapshots (dim_facility SCD)..."
	cd dbt_project && dbt snapshot --profiles-dir . --target prod

dbt-docs:
	@echo "Generating dbt documentation..."
	cd dbt_project && dbt docs generate --profiles-dir . --target prod
	cd dbt_project && dbt docs serve --port 8080

dbt-freshness:
	cd dbt_project && dbt source freshness --profiles-dir . --target prod

# ── Airflow ──────────────────────────────────────────────────────────────────
dag-validate:
	@echo "Validating Airflow DAG..."
	python scripts/validate_dag.py

# ── Terraform ────────────────────────────────────────────────────────────────
tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan -var-file=terraform.tfvars -out=tfplan

tf-apply:
	cd terraform && terraform apply tfplan

tf-destroy:
	@echo "WARNING: This will destroy all infrastructure. Are you sure? (ctrl+c to cancel)"
	@sleep 5
	cd terraform && terraform destroy -var-file=terraform.tfvars

# ── Cleanup ──────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf dbt_project/target dbt_project/logs
	rm -f terraform/tfplan
	@echo "Cleaned up generated files."
