"""
Row Reconciler Engine — Tool 2: get_row_recon

Compares row counts and checksums between source and target tables.
Detects:
  - Row count mismatches (delta and delta_pct)
  - Checksum mismatches (MD5 over ordered PKs)
"""

import logging
from typing import Any

import psycopg2

from config import SOURCE_DB, TARGET_DB

logger = logging.getLogger(__name__)

# Primary key column per table
TABLE_PK = {
    "vendor": "vendor_id",
    "inventory": "item_id",
    "purchase_order": "po_number",
    "inventory_transaction": "txn_id",
    "supplier_contract": "contract_id",
}


def _get_row_count(dsn: str, table_name: str) -> int:
    """Get exact row count for a table."""
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
            return cur.fetchone()[0]
    finally:
        conn.close()


def _get_pk_checksum(dsn: str, table_name: str, pk_col: str) -> str:
    """Compute MD5 checksum over ordered primary keys."""
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT MD5(STRING_AGG({pk_col}::TEXT, ',' ORDER BY {pk_col}))
                FROM {table_name}
                """  # noqa: S608
            )
            result = cur.fetchone()[0]
            return result or ""
    finally:
        conn.close()


def get_row_recon(table_name: str) -> dict[str, Any]:
    """Compare row counts and checksums between source and target.

    Returns the structure expected by the RCA agent's get_row_recon tool.
    """
    logger.info(f"Row recon: {table_name}")

    pk_col = TABLE_PK.get(table_name)
    if not pk_col:
        raise ValueError(f"Unknown table: {table_name}")

    source_count = _get_row_count(SOURCE_DB.dsn, table_name)
    target_count = _get_row_count(TARGET_DB.dsn, table_name)

    delta = source_count - target_count
    delta_pct = round((delta / source_count * 100), 2) if source_count > 0 else 0.0

    source_checksum = _get_pk_checksum(SOURCE_DB.dsn, table_name, pk_col)
    target_checksum = _get_pk_checksum(TARGET_DB.dsn, table_name, pk_col)
    checksum_match = source_checksum == target_checksum

    status = "MATCH" if delta == 0 and checksum_match else "MISMATCH"

    result = {
        "table": table_name,
        "source_count": source_count,
        "target_count": target_count,
        "delta": delta,
        "delta_pct": delta_pct,
        "checksum_match": checksum_match,
        "status": status,
    }

    logger.info(
        f"  {table_name}: source={source_count} target={target_count} "
        f"delta={delta} ({delta_pct}%) checksum={'MATCH' if checksum_match else 'MISMATCH'} "
        f"→ {status}"
    )

    return result
