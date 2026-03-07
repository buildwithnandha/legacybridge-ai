"""
Pipeline Log Reader — Tool 5: get_pipeline_logs

Reads Airflow task logs and extracts structured metrics for the RCA agent.
Parses log files from the Airflow logs directory, or returns simulated
pipeline metrics when logs are unavailable (demo mode).
"""

import os
import re
import logging
from typing import Any

from config import AIRFLOW_LOGS_DIR

logger = logging.getLogger(__name__)

# Simulated pipeline run metrics for demo mode
# These match a realistic run of the 5-task DAG
DEMO_PIPELINE_LOGS = {
    "extract_from_source": {
        "task": "extract_from_source",
        "status": "SUCCESS",
        "duration_seconds": 45,
        "records_processed": 92,
        "records_failed": 0,
        "error_message": None,
        "spark_metrics": {
            "partitions": 4,
            "shuffled_bytes": 24576,
            "failed_tasks": 0,
        },
    },
    "transform_data": {
        "task": "transform_data",
        "status": "SUCCESS",
        "duration_seconds": 38,
        "records_processed": 92,
        "records_failed": 0,
        "error_message": None,
        "spark_metrics": {
            "partitions": 4,
            "shuffled_bytes": 18432,
            "failed_tasks": 0,
        },
    },
    "load_to_target": {
        "task": "load_to_target",
        "status": "SUCCESS",
        "duration_seconds": 52,
        "records_processed": 86,
        "records_failed": 0,
        "error_message": None,
        "spark_metrics": {
            "partitions": 4,
            "shuffled_bytes": 20480,
            "failed_tasks": 0,
        },
    },
    "run_reconciliation": {
        "task": "run_reconciliation",
        "status": "SUCCESS",
        "duration_seconds": 12,
        "records_processed": 5,
        "records_failed": 0,
        "error_message": None,
        "spark_metrics": None,
    },
    "run_rca_agent": {
        "task": "run_rca_agent",
        "status": "SUCCESS",
        "duration_seconds": 28,
        "records_processed": 7,
        "records_failed": 0,
        "error_message": None,
        "spark_metrics": None,
    },
}


def _parse_airflow_log(log_path: str) -> dict[str, Any]:
    """Parse an Airflow task log file for structured metrics."""
    with open(log_path, "r") as f:
        content = f.read()

    status = "SUCCESS"
    error_message = None
    duration_seconds = 0
    records_processed = 0
    records_failed = 0

    # Parse task state
    if "State set to FAILED" in content or "ERROR" in content:
        status = "FAILED"
        # Extract last error line
        error_lines = re.findall(r"ERROR.*?$", content, re.MULTILINE)
        if error_lines:
            error_message = error_lines[-1][:500]

    if "State set to SKIPPED" in content:
        status = "SKIPPED"

    # Parse duration
    duration_match = re.search(r"duration:\s*([\d.]+)", content, re.IGNORECASE)
    if duration_match:
        duration_seconds = int(float(duration_match.group(1)))

    # Parse record counts from Spark job output
    rows_match = re.search(r"Total rows extracted:\s*(\d+)", content)
    if rows_match:
        records_processed = int(rows_match.group(1))

    rows_match = re.search(r"rows_upserted.*?(\d+)", content)
    if rows_match:
        records_processed = int(rows_match.group(1))

    # Parse Spark metrics
    spark_metrics = None
    partitions_match = re.search(r"partitions.*?(\d+)", content, re.IGNORECASE)
    if partitions_match:
        spark_metrics = {
            "partitions": int(partitions_match.group(1)),
            "shuffled_bytes": 0,
            "failed_tasks": 0,
        }

        shuffle_match = re.search(r"shuffle.*?bytes.*?(\d+)", content, re.IGNORECASE)
        if shuffle_match:
            spark_metrics["shuffled_bytes"] = int(shuffle_match.group(1))

        failed_match = re.search(r"failed tasks.*?(\d+)", content, re.IGNORECASE)
        if failed_match:
            spark_metrics["failed_tasks"] = int(failed_match.group(1))

    return {
        "status": status,
        "duration_seconds": duration_seconds,
        "records_processed": records_processed,
        "records_failed": records_failed,
        "error_message": error_message,
        "spark_metrics": spark_metrics,
    }


def _find_latest_log(dag_id: str, run_id: str, task_id: str) -> str | None:
    """Find the most recent log file for a given DAG/run/task."""
    # Airflow log structure: {base}/{dag_id}/{task_id}/{run_id}/{attempt}.log
    log_dir = os.path.join(AIRFLOW_LOGS_DIR, dag_id, task_id)

    if not os.path.isdir(log_dir):
        return None

    # If run_id specified, look in that subdirectory
    if run_id:
        run_dir = os.path.join(log_dir, run_id)
        if os.path.isdir(run_dir):
            logs = sorted(
                [f for f in os.listdir(run_dir) if f.endswith(".log")],
                reverse=True,
            )
            if logs:
                return os.path.join(run_dir, logs[0])

    # Fallback: search all run directories for the latest log
    latest_path = None
    latest_mtime = 0.0
    for entry in os.scandir(log_dir):
        if entry.is_dir():
            for log_file in os.scandir(entry.path):
                if log_file.name.endswith(".log") and log_file.stat().st_mtime > latest_mtime:
                    latest_mtime = log_file.stat().st_mtime
                    latest_path = log_file.path

    return latest_path


def get_pipeline_logs(
    dag_id: str = "legacy_migration_pipeline",
    run_id: str = "",
    task_id: str = "",
) -> dict[str, Any]:
    """Get pipeline task logs and metrics.

    Args:
        dag_id: Airflow DAG ID.
        run_id: Specific run ID (empty for latest).
        task_id: Specific task ID to inspect.

    Returns the structure expected by the RCA agent's get_pipeline_logs tool.
    """
    logger.info(f"Pipeline logs: dag={dag_id} run={run_id} task={task_id}")

    # Try to read real Airflow logs
    if task_id:
        log_path = _find_latest_log(dag_id, run_id, task_id)
        if log_path:
            logger.info(f"  Reading log: {log_path}")
            parsed = _parse_airflow_log(log_path)
            result = {"task": task_id, **parsed}
            logger.info(f"  {task_id}: {result['status']}")
            return result

    # Fallback to demo metrics
    if task_id and task_id in DEMO_PIPELINE_LOGS:
        logger.info(f"  Using demo metrics for: {task_id}")
        return DEMO_PIPELINE_LOGS[task_id]

    # If no specific task, return all tasks
    if not task_id:
        logger.info("  Returning all task metrics (demo mode)")
        return {
            "dag_id": dag_id,
            "run_id": run_id or "demo_run_001",
            "tasks": DEMO_PIPELINE_LOGS,
        }

    # Unknown task
    return {
        "task": task_id,
        "status": "NOT_FOUND",
        "duration_seconds": 0,
        "records_processed": 0,
        "records_failed": 0,
        "error_message": f"No logs found for task: {task_id}",
        "spark_metrics": None,
    }
