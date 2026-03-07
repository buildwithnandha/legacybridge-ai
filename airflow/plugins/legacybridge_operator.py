"""
Custom Airflow Operator for LegacyBridge AI.

SparkSubmitJobOperator: Submits PySpark jobs to the Spark master.
ReconTriggerOperator:   Calls the FastAPI backend to trigger reconciliation.
RcaAgentOperator:       Calls the FastAPI backend to trigger the RCA agent
                        (only runs if reconciliation found issues).
"""

import os
import logging
import requests
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException, AirflowSkipException

logger = logging.getLogger(__name__)

BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")


class SparkSubmitJobOperator(BaseOperator):
    """Submit a PySpark job to the Spark cluster via spark-submit."""

    template_fields = ("job_path", "job_args")
    ui_color = "#f9a825"

    def __init__(
        self,
        job_path: str,
        job_args: list[str] | None = None,
        spark_master: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_path = job_path
        self.job_args = job_args or []
        self.spark_master = spark_master or os.getenv(
            "SPARK_MASTER_URL", "spark://spark-master:7077"
        )

    def execute(self, context: Context) -> dict[str, Any]:
        import subprocess

        cmd = [
            "spark-submit",
            "--master", self.spark_master,
            "--deploy-mode", "client",
            "--driver-memory", "512m",
            "--executor-memory", "512m",
            "--packages", "org.postgresql:postgresql:42.7.1",
            self.job_path,
            *self.job_args,
        ]

        logger.info(f"Submitting Spark job: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )

        if result.returncode != 0:
            logger.error(f"Spark job STDERR:\n{result.stderr}")
            raise AirflowException(
                f"Spark job failed with exit code {result.returncode}: "
                f"{result.stderr[-500:]}"
            )

        logger.info(f"Spark job STDOUT:\n{result.stdout[-2000:]}")

        return {
            "job_path": self.job_path,
            "exit_code": result.returncode,
            "stdout_tail": result.stdout[-1000:],
        }


class ReconTriggerOperator(BaseOperator):
    """Trigger the reconciliation engine via the FastAPI backend."""

    ui_color = "#1e88e5"

    def __init__(self, timeout: int = 300, **kwargs):
        super().__init__(**kwargs)
        self.timeout = timeout

    def execute(self, context: Context) -> dict[str, Any]:
        url = f"{BACKEND_URL}/api/recon/run"

        logger.info(f"Triggering reconciliation: POST {url}")

        resp = requests.post(url, json={}, timeout=self.timeout)

        if resp.status_code != 200:
            raise AirflowException(
                f"Recon trigger failed: HTTP {resp.status_code} — {resp.text[:500]}"
            )

        data = resp.json()
        run_id = data.get("run_id")
        issues_found = data.get("issues_found", 0)
        health_score = data.get("health_score", 100)

        logger.info(
            f"Reconciliation complete — run_id={run_id}, "
            f"issues={issues_found}, health={health_score}/100"
        )

        # Push to XCom for downstream tasks
        context["ti"].xcom_push(key="recon_run_id", value=run_id)
        context["ti"].xcom_push(key="issues_found", value=issues_found)
        context["ti"].xcom_push(key="health_score", value=health_score)

        return {
            "run_id": run_id,
            "issues_found": issues_found,
            "health_score": health_score,
        }


class RcaAgentOperator(BaseOperator):
    """Trigger the Claude RCA agent — only if reconciliation found issues."""

    ui_color = "#e53935"

    def __init__(self, timeout: int = 600, **kwargs):
        super().__init__(**kwargs)
        self.timeout = timeout

    def execute(self, context: Context) -> dict[str, Any]:
        # Pull recon results from XCom
        ti = context["ti"]
        run_id = ti.xcom_pull(task_ids="run_reconciliation", key="recon_run_id")
        issues_found = ti.xcom_pull(task_ids="run_reconciliation", key="issues_found")

        if not issues_found or issues_found == 0:
            logger.info("No issues found — skipping RCA agent.")
            raise AirflowSkipException("No reconciliation issues to investigate.")

        url = f"{BACKEND_URL}/api/recon/{run_id}/rca"

        logger.info(
            f"Triggering RCA agent for run_id={run_id} "
            f"({issues_found} issues) — POST {url}"
        )

        resp = requests.post(url, json={}, timeout=self.timeout)

        if resp.status_code != 200:
            raise AirflowException(
                f"RCA agent trigger failed: HTTP {resp.status_code} — {resp.text[:500]}"
            )

        data = resp.json()
        logger.info(
            f"RCA agent complete — root_causes={data.get('root_causes_found', 0)}, "
            f"pdf_ready={data.get('pdf_ready', False)}"
        )

        return data
