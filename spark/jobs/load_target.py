"""
PySpark Load Job — Layer 1, Task 3
Reads transformed Parquet files and upserts them into the target PostgreSQL database.

Upsert strategy:
  - For each table, read the transformed Parquet
  - Write to a temp staging table in the target DB
  - Execute INSERT ... ON CONFLICT (pk) DO UPDATE for each table
  - Drop the temp staging table
  - Log row counts and any failures
"""

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.spark_session import get_spark_session, jdbc_url, jdbc_properties

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load_target")

# ── Config ───────────────────────────────────────────────────
TRANSFORM_DIR = os.getenv("TRANSFORM_DIR", "/tmp/legacybridge/transformed")

TARGET_HOST = os.getenv("TARGET_DB_HOST", "db-target")
TARGET_PORT = os.getenv("TARGET_DB_PORT", "5432")
TARGET_DB   = os.getenv("TARGET_DB_NAME", "legacybridge_target")
TARGET_USER = os.getenv("TARGET_DB_USER", "postgres")
TARGET_PASS = os.getenv("TARGET_DB_PASSWORD", "postgres")

# ── Table metadata for upsert ────────────────────────────────
TABLE_CONFIG = {
    "vendor": {
        "pk": "vendor_id",
        "columns": [
            "vendor_id", "vendor_name", "payment_terms", "lead_time_days",
            "active_flag", "country_code", "currency_code",
            "created_ts", "updated_ts", "updated_by",
        ],
    },
    "inventory": {
        "pk": "item_id",
        "columns": [
            "item_id", "location_id", "on_hand_qty", "reorder_point",
            "reorder_qty", "unit_cost", "status_code", "last_counted_ts",
            "updated_ts", "updated_by",
        ],
    },
    "purchase_order": {
        "pk": "po_number",
        "columns": [
            "po_number", "vendor_id", "order_date", "expected_date",
            "total_amount", "po_status", "line_count", "approved_by",
            "created_ts", "updated_ts",
        ],
    },
    "inventory_transaction": {
        "pk": "txn_id",
        "columns": [
            "txn_id", "item_id", "txn_type", "txn_qty", "txn_ts",
            "location_id", "reference_id", "created_by", "is_deleted",
        ],
    },
    "supplier_contract": {
        "pk": "contract_id",
        "columns": [
            "contract_id", "vendor_id", "start_date", "end_date",
            "contract_value", "terms_notes", "payment_freq",
            "auto_renew", "created_ts",
        ],
    },
}

# Tables must load in FK-safe order
LOAD_ORDER = [
    "vendor",
    "inventory",
    "purchase_order",
    "supplier_contract",
    "inventory_transaction",
]


def build_upsert_sql(table_name: str, staging_table: str) -> str:
    """Build an INSERT ... ON CONFLICT DO UPDATE statement."""
    config = TABLE_CONFIG[table_name]
    pk = config["pk"]
    columns = config["columns"]

    col_list = ", ".join(columns)
    select_list = ", ".join(f"s.{c}" for c in columns)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != pk
    )

    return f"""
        INSERT INTO {table_name} ({col_list})
        SELECT {select_list} FROM {staging_table} s
        ON CONFLICT ({pk}) DO UPDATE SET
        {update_set}
    """


def execute_sql(url: str, user: str, password: str, sql: str):
    """Execute raw SQL against the target database via JDBC."""
    import java.sql  # noqa — available in Spark JVM context
    # We use psycopg2 instead since we need DDL outside Spark
    import subprocess
    # Actually, use the Spark driver connection approach
    pass


def load_table_via_spark(spark: SparkSession, table_name: str) -> dict:
    """Load a single table using Spark JDBC write + raw SQL upsert."""
    input_path = os.path.join(TRANSFORM_DIR, table_name)
    staging_table = f"_staging_{table_name}"

    url = jdbc_url(TARGET_HOST, TARGET_PORT, TARGET_DB)
    props = jdbc_properties(TARGET_USER, TARGET_PASS)

    logger.info(f"Loading: {table_name}")

    # Read transformed parquet
    df = spark.read.parquet(input_path)
    # Select only the columns the target expects
    config = TABLE_CONFIG[table_name]
    df = df.select(*config["columns"])
    row_count = df.count()

    # Step 1: Write to temporary staging table (overwrite each run)
    logger.info(f"  Writing {row_count} rows to staging table: {staging_table}")
    (
        df.write
        .mode("overwrite")
        .jdbc(url=url, table=staging_table, properties=props)
    )

    # Step 2: Execute upsert from staging → target via JDBC connection
    upsert_sql = build_upsert_sql(table_name, staging_table)
    drop_sql = f"DROP TABLE IF EXISTS {staging_table}"

    import psycopg2
    conn = psycopg2.connect(
        host=TARGET_HOST,
        port=TARGET_PORT,
        dbname=TARGET_DB,
        user=TARGET_USER,
        password=TARGET_PASS,
    )
    try:
        with conn.cursor() as cur:
            logger.info(f"  Executing upsert: {table_name}")
            cur.execute(upsert_sql)
            upserted = cur.rowcount
            logger.info(f"  Upserted {upserted} rows into {table_name}")

            # Get final count in target
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            target_count = cur.fetchone()[0]

            # Clean up staging table
            cur.execute(drop_sql)
            conn.commit()
    finally:
        conn.close()

    logger.info(f"  {table_name}: {row_count} staged → {upserted} upserted (target total: {target_count})")

    return {
        "status": "SUCCESS",
        "rows_staged": row_count,
        "rows_upserted": upserted,
        "target_total": target_count,
    }


def load_table_overwrite(spark: SparkSession, table_name: str) -> dict:
    """Fallback: truncate + insert (simpler, used if upsert has issues)."""
    input_path = os.path.join(TRANSFORM_DIR, table_name)
    url = jdbc_url(TARGET_HOST, TARGET_PORT, TARGET_DB)
    props = jdbc_properties(TARGET_USER, TARGET_PASS)

    df = spark.read.parquet(input_path)
    config = TABLE_CONFIG[table_name]
    df = df.select(*config["columns"])
    row_count = df.count()

    logger.info(f"Loading (overwrite): {table_name} — {row_count} rows")

    import psycopg2
    conn = psycopg2.connect(
        host=TARGET_HOST,
        port=TARGET_PORT,
        dbname=TARGET_DB,
        user=TARGET_USER,
        password=TARGET_PASS,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
            conn.commit()
    finally:
        conn.close()

    (
        df.write
        .mode("append")
        .jdbc(url=url, table=table_name, properties=props)
    )

    return {
        "status": "SUCCESS",
        "rows_loaded": row_count,
        "method": "truncate_insert",
    }


def main():
    """Load all transformed tables into target PostgreSQL."""
    spark = get_spark_session("LegacyBridge-Load")

    logger.info("=" * 60)
    logger.info("LegacyBridge AI — Target Load Started")
    logger.info(f"Input:  {TRANSFORM_DIR}")
    logger.info(f"Target: {TARGET_HOST}:{TARGET_PORT}/{TARGET_DB}")
    logger.info("=" * 60)

    results = {}

    for table in LOAD_ORDER:
        try:
            results[table] = load_table_via_spark(spark, table)
        except Exception as e:
            logger.warning(f"  Upsert failed for {table}, falling back to overwrite: {e}")
            try:
                results[table] = load_table_overwrite(spark, table)
            except Exception as e2:
                logger.error(f"  Load completely failed for {table}: {e2}")
                results[table] = {"status": "FAILED", "error": str(e2)}

    # ── Summary ──────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Load Summary")
    logger.info("-" * 60)

    for table, r in results.items():
        if r["status"] == "SUCCESS":
            if "rows_upserted" in r:
                logger.info(
                    f"  {table:<25} {r['rows_staged']:>6} staged → "
                    f"{r['rows_upserted']:>6} upserted (total: {r['target_total']})"
                )
            else:
                logger.info(
                    f"  {table:<25} {r['rows_loaded']:>6} loaded ({r['method']})"
                )
        else:
            logger.info(f"  {table:<25} FAILED: {r['error']}")

    logger.info("=" * 60)

    failed = [t for t, r in results.items() if r["status"] == "FAILED"]
    if failed:
        logger.error(f"Load completed with failures: {failed}")
        spark.stop()
        sys.exit(1)

    logger.info("Load completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
