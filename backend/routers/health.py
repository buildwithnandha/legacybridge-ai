"""Health check endpoint."""

import psycopg2
from fastapi import APIRouter

from config import SOURCE_DB, TARGET_DB

router = APIRouter(prefix="/api", tags=["health"])


def _check_db(dsn: str) -> bool:
    try:
        conn = psycopg2.connect(dsn, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


@router.get("/health")
def health_check():
    source_ok = _check_db(SOURCE_DB.dsn)
    target_ok = _check_db(TARGET_DB.dsn)
    return {
        "status": "healthy" if (source_ok and target_ok) else "degraded",
        "source_db": "connected" if source_ok else "unreachable",
        "target_db": "connected" if target_ok else "unreachable",
    }
