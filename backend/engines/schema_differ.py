"""
Schema Differ Engine — Tool 1: get_schema_diff

Compares source and target table schemas by querying information_schema.columns
in both databases. Detects:
  - Missing columns (in source but not target)
  - Extra columns (in target but not source)
  - Type mismatches (same column, different data types)
"""

import logging
from typing import Any

import psycopg2

from config import SOURCE_DB, TARGET_DB

logger = logging.getLogger(__name__)

# Known type mappings that are intentional (not mismatches)
EXPECTED_MAPPINGS = {
    # These are the deliberate drift points we want to DETECT, not suppress
}


def _get_columns(dsn: str, table_name: str) -> dict[str, dict[str, Any]]:
    """Fetch column metadata from information_schema."""
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, character_maximum_length,
                       numeric_precision, numeric_scale, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    columns = {}
    for name, dtype, char_len, num_prec, num_scale, nullable in rows:
        full_type = dtype
        if char_len:
            full_type = f"{dtype}({char_len})"
        elif num_prec and num_scale:
            full_type = f"{dtype}({num_prec},{num_scale})"

        columns[name] = {
            "type": full_type,
            "raw_type": dtype,
            "nullable": nullable == "YES",
        }

    return columns


def get_schema_diff(table_name: str) -> dict[str, Any]:
    """Compare schema between source and target for a given table.

    Returns the structure expected by the RCA agent's get_schema_diff tool.
    """
    logger.info(f"Schema diff: {table_name}")

    source_cols = _get_columns(SOURCE_DB.dsn, table_name)
    target_cols = _get_columns(TARGET_DB.dsn, table_name)

    source_names = set(source_cols.keys())
    target_names = set(target_cols.keys())

    # Missing columns: in source but not target
    missing_columns = [
        {
            "column": col,
            "type": source_cols[col]["type"],
            "nullable": source_cols[col]["nullable"],
        }
        for col in sorted(source_names - target_names)
    ]

    # Extra columns: in target but not source
    extra_columns = [
        {
            "column": col,
            "type": target_cols[col]["type"],
            "nullable": target_cols[col]["nullable"],
        }
        for col in sorted(target_names - source_names)
    ]

    # Type mismatches: same column name, different type
    type_mismatches = []
    for col in sorted(source_names & target_names):
        src_type = source_cols[col]["raw_type"]
        tgt_type = target_cols[col]["raw_type"]
        if src_type != tgt_type:
            type_mismatches.append({
                "column": col,
                "source_type": source_cols[col]["type"],
                "target_type": target_cols[col]["type"],
            })

    # Severity classification
    if missing_columns or any(
        m["column"] in ("active_flag", "vendor_tier") for m in type_mismatches
    ):
        severity = "CRITICAL"
    elif type_mismatches or extra_columns:
        severity = "WARNING"
    else:
        severity = "HEALTHY"

    result = {
        "table": table_name,
        "source_column_count": len(source_cols),
        "target_column_count": len(target_cols),
        "missing_columns": missing_columns,
        "type_mismatches": type_mismatches,
        "extra_columns": extra_columns,
        "severity": severity,
    }

    logger.info(
        f"  {table_name}: {len(missing_columns)} missing, "
        f"{len(type_mismatches)} type mismatches, "
        f"{len(extra_columns)} extra → {severity}"
    )

    return result
