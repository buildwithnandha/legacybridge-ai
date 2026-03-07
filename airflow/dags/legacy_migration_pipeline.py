"""
LegacyBridge AI — Main Migration Pipeline DAG

5-task DAG:
  1. extract_from_source   — PySpark reads source DB → staging Parquet
  2. transform_data        — PySpark applies DB2→PG type mappings
  3. load_to_target        — PySpark upserts into target PostgreSQL
  4. run_reconciliation    — Recon engine: schema diff, row count, CDC, sample diff
  5. run_rca_agent         — Claude RCA agent (only if issues found)

Schedule: Manual trigger (for demo) or daily at 02:00 UTC
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from legacybridge_operator import (
    SparkSubmitJobOperator,
    ReconTriggerOperator,
    RcaAgentOperator,
)

# ── DAG Config ───────────────────────────────────────────────
SPARK_JOBS_DIR = "/opt/airflow/spark/jobs"

default_args = {
    "owner": "legacybridge",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="legacy_migration_pipeline",
    default_args=default_args,
    description="End-to-end ETL pipeline: Extract → Transform → Load → Recon → RCA",
    schedule=None,  # Manual trigger for demo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["legacybridge", "etl", "migration", "recon"],
    doc_md="""
    ## LegacyBridge AI — Migration Pipeline

    Runs the full ETL + reconciliation + AI root cause analysis pipeline.

    **Tasks:**
    1. `extract_from_source` — PySpark extracts 5 tables from source DB
    2. `transform_data` — Applies DB2→PostgreSQL type mappings
    3. `load_to_target` — Upserts transformed data into target DB
    4. `run_reconciliation` — Schema diff, row counts, CDC gaps, sample diffs
    5. `run_rca_agent` — Claude AI diagnoses root causes (skipped if no issues)
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Task 1: Extract ──────────────────────────────────────
    extract_from_source = SparkSubmitJobOperator(
        task_id="extract_from_source",
        job_path=f"{SPARK_JOBS_DIR}/extract_source.py",
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    # ── Task 2: Transform ────────────────────────────────────
    transform_data = SparkSubmitJobOperator(
        task_id="transform_data",
        job_path=f"{SPARK_JOBS_DIR}/transform_data.py",
    )

    # ── Task 3: Load ─────────────────────────────────────────
    load_to_target = SparkSubmitJobOperator(
        task_id="load_to_target",
        job_path=f"{SPARK_JOBS_DIR}/load_target.py",
    )

    # ── Task 4: Reconciliation ───────────────────────────────
    run_reconciliation = ReconTriggerOperator(
        task_id="run_reconciliation",
        timeout=300,
    )

    # ── Task 5: RCA Agent (conditional) ──────────────────────
    run_rca_agent = RcaAgentOperator(
        task_id="run_rca_agent",
        timeout=600,
        trigger_rule="all_success",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # ── DAG dependency chain ─────────────────────────────────
    start >> extract_from_source >> transform_data >> load_to_target
    load_to_target >> run_reconciliation >> run_rca_agent >> end
