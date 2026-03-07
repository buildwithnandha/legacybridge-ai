"""Pipeline status endpoints."""

from fastapi import APIRouter

from engines.pipeline_logger import get_pipeline_logs

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@router.get("/status")
def pipeline_status():
    """Get current Airflow DAG run status for all tasks."""
    return get_pipeline_logs(dag_id="legacy_migration_pipeline")
