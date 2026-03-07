"""
Sample Differ Engine — Tool 4: get_sample_diff

Fetches sample rows from source and target for a specific column,
compares them side-by-side, and reports value-level differences.
Detects:
  - DECIMAL→FLOAT rounding loss
  - Timestamp TZ drift
  - NULL vs empty string mismatches
  - Missing values from dropped columns
  - CHAR→BOOLEAN coercion differences
"""

import logging
from typing import Any

import psycopg2

from config import SOURCE_DB, TARGET_DB

logger = logging.getLogger(__name__)

TABLE_PK = {
    "vendor": "vendor_id",
    "inventory": "item_id",
    "purchase_order": "po_number",
    "inventory_transaction": "txn_id",
    "supplier_contract": "contract_id",
}


def _column_exists(dsn: str, table_name: str, column: str) -> bool:
    """Check if a column exists in a table."""
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                  AND column_name = %s
                """,
                (table_name, column),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _fetch_column_values(
    dsn: str, table_name: str, pk_col: str, column: str, limit: int
) -> dict[str, Any]:
    """Fetch pk → value mapping for a column."""
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {pk_col}::TEXT, {column}::TEXT
                FROM {table_name}
                ORDER BY {pk_col}
                LIMIT %s
                """,  # noqa: S608
                (limit,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()


def _count_affected(
    source_vals: dict[str, Any], target_vals: dict[str, Any]
) -> int:
    """Count how many rows have differing values."""
    common_keys = set(source_vals.keys()) & set(target_vals.keys())
    diffs = sum(
        1 for k in common_keys if source_vals[k] != target_vals[k]
    )
    # Also count keys in source but not target
    diffs += len(set(source_vals.keys()) - set(target_vals.keys()))
    return diffs


def get_sample_diff(
    table_name: str, column: str, limit: int = 10
) -> dict[str, Any]:
    """Compare sample values for a specific column between source and target.

    Args:
        table_name: Name of the table.
        column: Column to compare.
        limit: Max number of sample rows to return.

    Returns the structure expected by the RCA agent's get_sample_diff tool.
    """
    logger.info(f"Sample diff: {table_name}.{column} (limit={limit})")

    pk_col = TABLE_PK.get(table_name)
    if not pk_col:
        raise ValueError(f"Unknown table: {table_name}")

    source_exists = _column_exists(SOURCE_DB.dsn, table_name, column)
    target_exists = _column_exists(TARGET_DB.dsn, table_name, column)

    # Handle column missing in target (e.g., vendor_tier)
    if source_exists and not target_exists:
        source_vals = _fetch_column_values(
            SOURCE_DB.dsn, table_name, pk_col, column, limit
        )
        samples = [
            {
                "pk": pk,
                "source_value": val,
                "target_value": None,
                "diff_type": "MISSING_COLUMN",
            }
            for pk, val in list(source_vals.items())[:limit]
        ]

        # Get total count in source
        conn = psycopg2.connect(SOURCE_DB.dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NOT NULL"  # noqa: S608
                )
                total_affected = cur.fetchone()[0]
        finally:
            conn.close()

        return {
            "table": table_name,
            "column": column,
            "samples": samples,
            "total_affected": total_affected,
            "diff_type": "MISSING_COLUMN",
        }

    # Handle column missing in source (e.g., is_deleted)
    if not source_exists and target_exists:
        target_vals = _fetch_column_values(
            TARGET_DB.dsn, table_name, pk_col, column, limit
        )
        samples = [
            {
                "pk": pk,
                "source_value": None,
                "target_value": val,
                "diff_type": "EXTRA_COLUMN",
            }
            for pk, val in list(target_vals.items())[:limit]
        ]
        return {
            "table": table_name,
            "column": column,
            "samples": samples,
            "total_affected": len(target_vals),
            "diff_type": "EXTRA_COLUMN",
        }

    # Both columns exist — compare values
    source_vals = _fetch_column_values(
        SOURCE_DB.dsn, table_name, pk_col, column, limit * 5
    )
    target_vals = _fetch_column_values(
        TARGET_DB.dsn, table_name, pk_col, column, limit * 5
    )

    samples = []
    for pk in sorted(source_vals.keys()):
        src_val = source_vals.get(pk)
        tgt_val = target_vals.get(pk)

        if src_val != tgt_val:
            diff_type = _classify_diff(column, src_val, tgt_val)
            samples.append({
                "pk": pk,
                "source_value": src_val,
                "target_value": tgt_val,
                "diff_type": diff_type,
            })

        if len(samples) >= limit:
            break

    # Count total affected across all rows
    total_affected = _count_affected(source_vals, target_vals)

    result = {
        "table": table_name,
        "column": column,
        "samples": samples,
        "total_affected": total_affected,
    }

    logger.info(
        f"  {table_name}.{column}: {len(samples)} sample diffs, "
        f"{total_affected} total affected"
    )

    return result


def _classify_diff(column: str, src_val: Any, tgt_val: Any) -> str:
    """Classify the type of difference between source and target values."""
    if src_val is not None and tgt_val is None:
        if src_val == "":
            return "NULL_EMPTY"
        return "MISSING_VALUE"

    if src_val == "" and tgt_val is None:
        return "NULL_EMPTY"

    # CHAR Y/N vs BOOLEAN true/false
    if src_val in ("Y", "N") and tgt_val in ("true", "false", "t", "f"):
        return "TYPE_COERCION_BOOL"

    # Numeric rounding (DECIMAL vs FLOAT)
    try:
        src_float = float(src_val)
        tgt_float = float(tgt_val)
        if abs(src_float - tgt_float) < 0.01:
            return "FLOAT_ROUNDING"
    except (ValueError, TypeError):
        pass

    # Timestamp drift detection
    if "ts" in column or "timestamp" in column.lower():
        return "TZ_DRIFT"

    return "VALUE_MISMATCH"
