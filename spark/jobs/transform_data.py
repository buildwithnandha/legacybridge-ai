"""
PySpark Transform Job — Layer 1, Task 2
Reads staging Parquet files, applies DB2→PostgreSQL type mappings,
and writes transformed Parquet files to the transform output directory.

Transformations applied per mismatch matrix:
  - vendor:                drop vendor_tier, CHAR(1)→BOOLEAN on active_flag
  - inventory:             DECIMAL(10,4)→FLOAT on unit_cost, +5hr TZ on last_counted_ts
  - purchase_order:        passthrough (clean baseline)
  - inventory_transaction: status_code→is_deleted BOOLEAN, filter DEL rows, +5hr TZ on txn_ts
  - supplier_contract:     empty string→NULL on terms_notes, CHAR(1)→BOOLEAN on auto_renew
"""

import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.spark_session import get_spark_session
from utils.schema_mapper import TRANSFORM_REGISTRY
from schemas.source_schema import SOURCE_TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("transform_data")

# ── Config ───────────────────────────────────────────────────
STAGING_DIR   = os.getenv("STAGING_DIR", "/tmp/legacybridge/staging")
TRANSFORM_DIR = os.getenv("TRANSFORM_DIR", "/tmp/legacybridge/transformed")


def transform_table(spark, table_name: str) -> dict:
    """Read staging Parquet, apply transform, write to transform dir."""
    input_path = os.path.join(STAGING_DIR, table_name)
    output_path = os.path.join(TRANSFORM_DIR, table_name)

    logger.info(f"Transforming: {table_name}")

    df = spark.read.parquet(input_path)
    input_count = df.count()

    transform_fn = TRANSFORM_REGISTRY[table_name]
    df_transformed = transform_fn(df)
    output_count = df_transformed.count()

    df_transformed.write.mode("overwrite").parquet(output_path)

    delta = input_count - output_count
    logger.info(
        f"  {table_name}: {input_count} in → {output_count} out"
        + (f" ({delta} rows filtered)" if delta > 0 else "")
    )

    return {
        "status": "SUCCESS",
        "input_rows": input_count,
        "output_rows": output_count,
        "rows_filtered": delta,
        "columns_in": len(df.columns),
        "columns_out": len(df_transformed.columns),
    }


def main():
    """Transform all staged tables and write to transform directory."""
    spark = get_spark_session("LegacyBridge-Transform")

    logger.info("=" * 60)
    logger.info("LegacyBridge AI — Data Transform Started")
    logger.info(f"Input:  {STAGING_DIR}")
    logger.info(f"Output: {TRANSFORM_DIR}")
    logger.info("=" * 60)

    os.makedirs(TRANSFORM_DIR, exist_ok=True)

    results = {}

    for table in SOURCE_TABLES:
        try:
            results[table] = transform_table(spark, table)
        except Exception as e:
            logger.error(f"  Failed to transform {table}: {e}")
            results[table] = {"status": "FAILED", "error": str(e)}

    # ── Summary ──────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Transform Summary")
    logger.info("-" * 60)
    logger.info(f"  {'Table':<25} {'In':>6} {'Out':>6} {'Filter':>7} {'Cols':>5}")
    logger.info("-" * 60)

    for table, r in results.items():
        if r["status"] == "SUCCESS":
            logger.info(
                f"  {table:<25} {r['input_rows']:>6} {r['output_rows']:>6} "
                f"{r['rows_filtered']:>7} {r['columns_out']:>5}"
            )
        else:
            logger.info(f"  {table:<25} FAILED: {r['error']}")

    logger.info("=" * 60)

    # ── Log transform details per table ──────────────────────
    logger.info("Transform Details:")
    logger.info("  vendor:                dropped vendor_tier, active_flag CHAR→BOOLEAN")
    logger.info("  inventory:             unit_cost DECIMAL→FLOAT, last_counted_ts +5hr TZ")
    logger.info("  purchase_order:        passthrough (clean)")
    logger.info("  inventory_transaction: status_code→is_deleted, DEL rows filtered, txn_ts +5hr TZ")
    logger.info("  supplier_contract:     terms_notes empty→NULL, auto_renew CHAR→BOOLEAN")

    failed = [t for t, r in results.items() if r["status"] == "FAILED"]
    if failed:
        logger.error(f"Transform completed with failures: {failed}")
        spark.stop()
        sys.exit(1)

    logger.info("Transform completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
