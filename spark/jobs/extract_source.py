"""
PySpark Extract Job — Layer 1, Task 1
Reads all 5 tables from the source DB (Mock DB2 / PostgreSQL)
and writes them as Parquet files to the staging directory.
"""

import os
import sys
import logging

from pyspark.sql import SparkSession

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.spark_session import get_spark_session, jdbc_url, jdbc_properties
from schemas.source_schema import SOURCE_TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("extract_source")

# ── Config ───────────────────────────────────────────────────
STAGING_DIR = os.getenv("STAGING_DIR", "/tmp/legacybridge/staging")

SOURCE_HOST = os.getenv("SOURCE_DB_HOST", "db-source")
SOURCE_PORT = os.getenv("SOURCE_DB_PORT", "5432")
SOURCE_DB   = os.getenv("SOURCE_DB_NAME", "legacybridge_source")
SOURCE_USER = os.getenv("SOURCE_DB_USER", "postgres")
SOURCE_PASS = os.getenv("SOURCE_DB_PASSWORD", "postgres")


def extract_table(spark: SparkSession, table_name: str) -> int:
    """Extract a single table from source DB to Parquet. Returns row count."""
    url = jdbc_url(SOURCE_HOST, SOURCE_PORT, SOURCE_DB)
    props = jdbc_properties(SOURCE_USER, SOURCE_PASS)

    logger.info(f"Extracting table: {table_name}")

    df = (
        spark.read
        .jdbc(url=url, table=table_name, properties=props)
    )

    row_count = df.count()
    output_path = os.path.join(STAGING_DIR, table_name)

    df.write.mode("overwrite").parquet(output_path)

    logger.info(f"  → {table_name}: {row_count} rows → {output_path}")
    return row_count


def main():
    """Extract all source tables to Parquet staging files."""
    spark = get_spark_session("LegacyBridge-Extract")

    logger.info("=" * 60)
    logger.info("LegacyBridge AI — Source Extraction Started")
    logger.info(f"Source: {SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}")
    logger.info(f"Staging: {STAGING_DIR}")
    logger.info("=" * 60)

    os.makedirs(STAGING_DIR, exist_ok=True)

    total_rows = 0
    results = {}

    for table in SOURCE_TABLES:
        try:
            count = extract_table(spark, table)
            results[table] = {"status": "SUCCESS", "rows": count}
            total_rows += count
        except Exception as e:
            logger.error(f"  Failed to extract {table}: {e}")
            results[table] = {"status": "FAILED", "error": str(e)}

    # ── Summary ──────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Extraction Summary")
    logger.info("-" * 40)
    for table, result in results.items():
        if result["status"] == "SUCCESS":
            logger.info(f"  {table:<25} {result['rows']:>6} rows")
        else:
            logger.info(f"  {table:<25} FAILED: {result['error']}")
    logger.info("-" * 40)
    logger.info(f"  Total rows extracted: {total_rows}")
    logger.info("=" * 60)

    failed = [t for t, r in results.items() if r["status"] == "FAILED"]
    if failed:
        logger.error(f"Extraction completed with failures: {failed}")
        spark.stop()
        sys.exit(1)

    logger.info("Extraction completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
