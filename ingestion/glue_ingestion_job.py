# ingestion/glue_ingestion_job.py
#
# AWS Glue Python Shell job — Healthcare Metrics Pipeline
#
# Runs on a schedule triggered by Airflow (Task 3: download_to_s3).
# Accepts a single Glue job parameter: --file_name
# One job invocation per file — called once per file via
# Airflow dynamic task mapping for full per-file isolation.
#
# Error handling coverage (from error handling map):
#   Stage 1 — Drive API: retry 3x exponential backoff (retryable HTTP errors)
#   Stage 1 — S3 write:  retry 3x fixed 60s delay (throttling cooldown)
#   Stage 1 — Checksum:  MD5 validation after download, retry once on mismatch
#   Stage 1 — Auth:      fail fast on 401/403 — needs human fix
#   Stage 2 — COPY INTO: column mismatch detected and classified non-retryable
#   All stages — structured JSON logging to CloudWatch
#   All stages — audit table updated on success AND failure

import sys
import os
import io
import json
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import snowflake.connector
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
    retry_if_exception_type,
    before_sleep_log,
    RetryError,
)

# ── Glue job parameters ───────────────────────────────────────────────────────
# When running locally set these as environment variables.
# When running on Glue these come from --job-parameters.
try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, [
        'file_name',
        'pipeline_run_id',
        'snowflake_secret_name',
        's3_bucket',
        'google_drive_folder_id',
        'google_credentials_secret_name',
    ])
except Exception:
    # Local development fallback — read from environment variables
    args = {
        'file_name':                      os.environ['GLUE_FILE_NAME'],
        'pipeline_run_id':                os.environ.get('PIPELINE_RUN_ID', 'local-dev'),
        'snowflake_secret_name':          os.environ['SNOWFLAKE_SECRET_NAME'],
        's3_bucket':                      os.environ['S3_BUCKET_NAME'],
        'google_drive_folder_id':         os.environ['GOOGLE_DRIVE_FOLDER_ID'],
        'google_credentials_secret_name': os.environ['GOOGLE_CREDENTIALS_SECRET_NAME'],
    }

FILE_NAME       = args['file_name']
PIPELINE_RUN_ID = args['pipeline_run_id']
S3_BUCKET       = args['s3_bucket']
DRIVE_FOLDER_ID = args['google_drive_folder_id']
SF_SECRET_NAME  = args['snowflake_secret_name']
GCP_SECRET_NAME = args['google_credentials_secret_name']


# ── Structured logger ─────────────────────────────────────────────────────────
# Every log entry is a JSON object with consistent fields.
# Streams to CloudWatch Logs automatically when running on Glue.
# See error handling strategy: "Structured JSON logging — consistent
# fields across Glue, Airflow, dbt"

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # raw JSON — no extra formatting
    stream=sys.stdout,
)
_logger = logging.getLogger("glue_ingestion")


def log(level: str, event: str, **kwargs):
    """Emit a structured JSON log entry."""
    entry = {
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "level":           level.upper(),
        "component":       "glue_ingestion",
        "pipeline_run_id": PIPELINE_RUN_ID,
        "file_name":       FILE_NAME,
        "event":           event,
        **kwargs,
    }
    getattr(_logger, level.lower())(json.dumps(entry))


# ── Secrets helper ────────────────────────────────────────────────────────────

def get_secret(secret_name: str) -> dict:
    """
    Fetch a secret from AWS Secrets Manager.
    Fail fast on any error — if we can't get credentials
    there is nothing safe to do.
    """
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        log("error", "secrets_fetch_failed",
            secret_name=secret_name,
            error=str(e))
        raise


# ── Google Drive client ───────────────────────────────────────────────────────

def build_drive_service(credentials_dict: dict):
    """Build an authenticated Google Drive API service client."""
    creds = service_account.Credentials.from_service_account_info(
        credentials_dict,
        scopes=['https://www.googleapis.com/auth/drive.readonly'],
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


# Error handling: Drive API retries
# Retryable: HTTP 429 (rate limit), 500, 503 (transient server errors)
# Non-retryable: 401 (auth), 403 (permissions), 404 (file not found)
# Strategy: exponential backoff — hitting rate limit again immediately makes worse
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=30, max=300),
    retry=retry_if_exception_type(HttpError),
    before_sleep=before_sleep_log(_logger, logging.WARNING),
    reraise=True,
)
def get_file_metadata(service, folder_id: str, filename: str) -> Optional[dict]:
    """
    Get file metadata from Google Drive by filename.
    Returns None if file not found — not an error, means no new file.
    Retries on transient HTTP errors. Fails fast on auth/permission errors.
    """
    try:
        results = service.files().list(
            q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
            fields="files(id, name, modifiedTime, md5Checksum, size)",
        ).execute()

        files = results.get('files', [])
        if not files:
            log("info", "file_not_found_in_drive", filename=filename)
            return None

        # If multiple versions exist, take the most recently modified
        return sorted(files, key=lambda x: x['modifiedTime'], reverse=True)[0]

    except HttpError as e:
        # Fail fast on auth/permission errors — retrying won't help
        if e.resp.status in (401, 403, 404):
            log("error", "drive_auth_or_permission_error",
                status_code=e.resp.status,
                error=str(e))
            raise RuntimeError(
                f"Non-retryable Drive API error {e.resp.status}: {e}"
            ) from e
        # Re-raise for tenacity to handle retryable errors
        log("warning", "drive_api_transient_error",
            status_code=e.resp.status,
            error=str(e))
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=30, max=300),
    retry=retry_if_exception_type(HttpError),
    before_sleep=before_sleep_log(_logger, logging.WARNING),
    reraise=True,
)
def download_file_from_drive(service, file_id: str) -> bytes:
    """
    Download file content from Google Drive.
    Returns raw bytes. Validates MD5 checksum after download.
    Error handling: exponential backoff on transient HTTP errors.
    """
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request, chunksize=50 * 1024 * 1024)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    return buffer.getvalue()


# ── Snowflake client ──────────────────────────────────────────────────────────

def get_snowflake_connection(sf_creds: dict):
    """Create a Snowflake connection from credentials dict."""
    return snowflake.connector.connect(
        account=sf_creds['account'],
        user=sf_creds['user'],
        password=sf_creds['password'],
        warehouse=sf_creds.get('warehouse', 'HEALTHCARE_WH_XS'),
        database=sf_creds.get('database', 'HEALTHCARE_METRICS'),
        schema='AUDIT',
    )


def check_watermark(conn, filename: str) -> Optional[datetime]:
    """
    Query audit.load_history for the last successful load of this file.
    Returns the file_modified_at timestamp of the last success,
    or None if the file has never been loaded.

    This is the core of the incremental detection logic:
    - If Drive modifiedTime > watermark → file is new, proceed
    - If Drive modifiedTime <= watermark → file unchanged, skip
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT MAX(FILE_MODIFIED_AT)
            FROM HEALTHCARE_METRICS.AUDIT.LOAD_HISTORY
            WHERE SOURCE_FILE = %s
              AND STATUS = 'success'
        """, (filename,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    finally:
        cursor.close()


def update_audit_table(
    conn,
    filename: str,
    s3_path: str,
    file_modified_at: datetime,
    status: str,
    target_table: str,
    row_count: Optional[int] = None,
    error_message: Optional[str] = None,
):
    """
    Insert a record into audit.load_history.
    Called on BOTH success and failure — the audit table records everything.

    Error handling: this is called in a finally block so it always runs.
    If this insert fails after 3 retries, the watermark stays at previous
    state — the next pipeline run will safely retry the file.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO HEALTHCARE_METRICS.AUDIT.LOAD_HISTORY (
                SOURCE_FILE, S3_PATH, FILE_MODIFIED_AT,
                LOADED_AT, ROW_COUNT, TARGET_TABLE,
                STATUS, ERROR_MESSAGE, PIPELINE_RUN_ID
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            filename,
            s3_path,
            file_modified_at,
            datetime.now(timezone.utc),
            row_count,
            target_table,
            status,
            error_message[:2000] if error_message else None,
            PIPELINE_RUN_ID,
        ))
        conn.commit()
        log("info", "audit_table_updated",
            status=status,
            row_count=row_count)
    finally:
        cursor.close()


# ── S3 operations ─────────────────────────────────────────────────────────────

# Error handling: S3 write retries
# Strategy: fixed 60s delay — S3 throttling has a cooldown period,
# not a gradual recovery, so exponential backoff is less effective here
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(60),
    retry=retry_if_exception_type(ClientError),
    before_sleep=before_sleep_log(_logger, logging.WARNING),
    reraise=True,
)
def write_to_s3(content: bytes, bucket: str, key: str) -> None:
    """
    Write file content to S3.
    Error handling: fixed 60s retry for throttling.
    Fails fast on permission errors (403).
    """
    s3 = boto3.client('s3')

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ServerSideEncryption='AES256',
        )
    except ClientError as e:
        error_code = e.response['Error']['Code']
        # Fail fast on permission errors — retrying won't help
        if error_code in ('AccessDenied', 'InvalidAccessKeyId'):
            log("error", "s3_permission_error",
                bucket=bucket, key=key,
                error_code=error_code)
            raise RuntimeError(
                f"Non-retryable S3 error {error_code}"
            ) from e
        log("warning", "s3_transient_error",
            bucket=bucket, key=key,
            error_code=error_code)
        raise


def validate_checksum(content: bytes, expected_md5: Optional[str]) -> bool:
    """
    Validate MD5 checksum of downloaded content against Drive metadata.
    Error handling: if checksum fails, the download is corrupted.
    Caller retries the full download once before failing.
    """
    if not expected_md5:
        # Drive doesn't always provide MD5 (e.g. Google Docs native files)
        return True
    actual_md5 = hashlib.md5(content).hexdigest()
    return actual_md5 == expected_md5


# ── COPY INTO ─────────────────────────────────────────────────────────────────

def run_copy_into(conn, s3_path: str, raw_table: str) -> int:
    """
    Run COPY INTO to load file from S3 into Snowflake RAW table.
    Returns number of rows loaded.

    Error handling (Stage 2 from error handling map):
    - Column count mismatch: non-retryable, fail fast with clear message
    - Data type error: non-retryable, fail fast
    - Snowflake timeout: Airflow handles retry at task level
    - Deduplication: Snowflake built-in — same file loaded twice is safe
    """
    cursor = conn.cursor()
    try:
        # Switch to RAW schema for COPY INTO
        cursor.execute("USE SCHEMA HEALTHCARE_METRICS.RAW")
        cursor.execute("USE WAREHOUSE HEALTHCARE_WH_XS")

        copy_sql = f"""
            COPY INTO {raw_table}
            FROM '@HEALTHCARE_METRICS.RAW.S3_BRONZE_STAGE/{s3_path}'
            FILE_FORMAT = (FORMAT_NAME = 'HEALTHCARE_METRICS.RAW.CSV_FORMAT')
            ON_ERROR = 'ABORT_STATEMENT'
            PURGE = FALSE
        """

        log("info", "copy_into_started",
            raw_table=raw_table,
            s3_path=s3_path)

        cursor.execute(copy_sql)
        results = cursor.fetchall()

        # COPY INTO returns one row per file with load status
        total_rows = 0
        for row in results:
            status   = row[0]   # LOADED, LOAD_FAILED, PARTIALLY_LOADED
            rows_loaded = row[3] if len(row) > 3 else 0
            errors   = row[5] if len(row) > 5 else ''

            if status == 'LOAD_FAILED':
                # Classify the error — column mismatch is non-retryable
                if 'Number of columns in file' in str(errors):
                    log("error", "copy_into_column_mismatch",
                        raw_table=raw_table,
                        error=str(errors),
                        note="CMS may have changed file schema — needs investigation")
                    raise ValueError(
                        f"COPY INTO column mismatch for {raw_table}. "
                        f"CMS may have changed the file schema. Error: {errors}"
                    )
                raise RuntimeError(
                    f"COPY INTO failed for {raw_table}: {errors}"
                )

            total_rows += rows_loaded or 0

        log("info", "copy_into_completed",
            raw_table=raw_table,
            rows_loaded=total_rows)

        return total_rows

    finally:
        cursor.close()


# ── Main ingestion function ───────────────────────────────────────────────────

def ingest_file(file_config: dict) -> dict:
    """
    Full ingestion pipeline for a single file:
    1. Check watermark
    2. Get Drive metadata
    3. Compare timestamps
    4. Download if new
    5. Validate checksum
    6. Write to S3
    7. COPY INTO Snowflake
    8. Update audit table

    Returns dict with status, rows_loaded, and any error details.
    Always updates the audit table — on success AND failure.
    """
    filename    = file_config['drive_filename']
    s3_prefix   = file_config['s3_prefix']
    raw_table   = file_config['raw_table']

    log("info", "ingestion_started", filename=filename)

    # Fetch credentials from Secrets Manager
    sf_creds  = get_secret(SF_SECRET_NAME)
    gcp_creds = get_secret(GCP_SECRET_NAME)

    sf_conn = get_snowflake_connection(sf_creds)
    drive   = build_drive_service(gcp_creds)

    s3_path     = None
    file_meta   = None
    row_count   = None
    error_msg   = None
    status      = 'failed'

    try:
        # ── Step 1: Check watermark ─────────────────────────────────────────
        watermark = check_watermark(sf_conn, filename)
        log("info", "watermark_checked",
            filename=filename,
            last_loaded=watermark.isoformat() if watermark else None)

        # ── Step 2: Get Drive metadata ──────────────────────────────────────
        file_meta = get_file_metadata(drive, DRIVE_FOLDER_ID, filename)

        if file_meta is None:
            log("warning", "file_not_in_drive", filename=filename)
            return {"status": "skipped", "reason": "file not found in Drive"}

        drive_modified = datetime.fromisoformat(
            file_meta['modifiedTime'].replace('Z', '+00:00')
        )

        # ── Step 3: Compare timestamps ──────────────────────────────────────
        if watermark and drive_modified <= watermark:
            log("info", "file_unchanged_skipping",
                filename=filename,
                drive_modified=drive_modified.isoformat(),
                watermark=watermark.isoformat())
            return {"status": "skipped", "reason": "file unchanged since last load"}

        log("info", "new_file_detected",
            filename=filename,
            drive_modified=drive_modified.isoformat(),
            file_size_bytes=file_meta.get('size', 'unknown'))

        # ── Step 4: Download from Drive ─────────────────────────────────────
        # Retry download once if checksum fails (corrupted download)
        for attempt in range(2):
            t0 = time.time()
            content = download_file_from_drive(drive, file_meta['id'])
            duration = round(time.time() - t0, 2)

            # ── Step 5: Validate checksum ───────────────────────────────────
            if validate_checksum(content, file_meta.get('md5Checksum')):
                log("info", "download_complete",
                    filename=filename,
                    size_bytes=len(content),
                    duration_seconds=duration)
                break
            else:
                if attempt == 0:
                    log("warning", "checksum_mismatch_retrying",
                        filename=filename,
                        attempt=attempt + 1)
                    continue
                else:
                    raise RuntimeError(
                        f"Checksum validation failed after 2 attempts for {filename}"
                    )

        # ── Step 6: Write to S3 Bronze ──────────────────────────────────────
        date_prefix = datetime.now(timezone.utc).strftime('%Y/%m/%d')
        s3_key      = f"{s3_prefix}/{date_prefix}/{filename}"
        s3_path     = f"s3://{S3_BUCKET}/{s3_key}"

        write_to_s3(content, S3_BUCKET, s3_key)
        log("info", "s3_write_complete",
            s3_path=s3_path,
            size_bytes=len(content))

        # ── Step 7: COPY INTO Snowflake ─────────────────────────────────────
        row_count = run_copy_into(sf_conn, s3_key, raw_table)
        status    = 'success'

        log("info", "ingestion_complete",
            filename=filename,
            rows_loaded=row_count,
            s3_path=s3_path)

        return {
            "status":      "success",
            "file":        filename,
            "rows_loaded": row_count,
            "s3_path":     s3_path,
        }

    except Exception as e:
        error_msg = str(e)
        status    = 'failed'

        log("error", "ingestion_failed",
            filename=filename,
            error=error_msg,
            error_type=type(e).__name__)

        return {
            "status": "failed",
            "file":   filename,
            "error":  error_msg,
        }

    finally:
        # ── Step 8: Always update audit table ───────────────────────────────
        # This runs whether the ingestion succeeded or failed.
        # The watermark is only considered valid when status = 'success'.
        # A failed record means the next run will safely retry this file.
        if file_meta:
            try:
                update_audit_table(
                    conn=sf_conn,
                    filename=filename,
                    s3_path=s3_path or '',
                    file_modified_at=drive_modified,
                    status=status,
                    target_table=raw_table,
                    row_count=row_count,
                    error_message=error_msg,
                )
            except Exception as audit_err:
                # Audit table update failed — log but don't mask the original error
                # The watermark stays at its previous state so next run retries safely
                log("error", "audit_table_update_failed",
                    filename=filename,
                    error=str(audit_err))

        sf_conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # Load file config from registry
    from config import FILE_REGISTRY

    config = next(
        (f for f in FILE_REGISTRY if f['drive_filename'] == FILE_NAME),
        None
    )

    if config is None:
        log("error", "file_not_in_registry", file_name=FILE_NAME)
        sys.exit(1)

    result = ingest_file(config)

    log("info", "job_result", **result)

    # Exit with non-zero code if ingestion failed
    # Airflow detects this and marks the task as failed
    if result['status'] == 'failed':
        sys.exit(1)

    sys.exit(0)