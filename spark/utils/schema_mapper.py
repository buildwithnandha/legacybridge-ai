"""
Type mapping rules for DB2 (source) → PostgreSQL (target) transformations.

Each mapping function takes a PySpark DataFrame and returns a transformed DataFrame
that matches the target schema. These intentionally reproduce the real-world
drift patterns defined in the mismatch matrix.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, FloatType


# ── Primitive transforms ─────────────────────────────────────

def char_to_boolean(df: DataFrame, column: str) -> DataFrame:
    """Convert CHAR(1) Y/N flag to BOOLEAN."""
    return df.withColumn(
        column,
        F.when(F.upper(F.col(column)) == "Y", True)
         .when(F.upper(F.col(column)) == "N", False)
         .otherwise(None)
         .cast(BooleanType())
    )


def decimal_to_float(df: DataFrame, column: str) -> DataFrame:
    """Convert DECIMAL to FLOAT (introduces rounding loss)."""
    return df.withColumn(column, F.col(column).cast(FloatType()))


def shift_timestamp(df: DataFrame, column: str, hours: int = 5) -> DataFrame:
    """Shift a timestamp column by N hours (simulates TZ drift)."""
    return df.withColumn(
        column,
        F.col(column) + F.expr(f"INTERVAL {hours} HOURS")
    )


def empty_to_null(df: DataFrame, column: str) -> DataFrame:
    """Convert empty strings to NULL."""
    return df.withColumn(
        column,
        F.when(F.trim(F.col(column)) == "", None).otherwise(F.col(column))
    )


def status_code_to_is_deleted(df: DataFrame) -> DataFrame:
    """Replace status_code CHAR(3) with is_deleted BOOLEAN.
    ACT → false, DEL → true (but DEL rows are filtered out in load step)."""
    return (
        df.withColumn(
            "is_deleted",
            F.when(F.col("status_code") == "DEL", True).otherwise(False)
        )
        .drop("status_code")
    )


# ── Per-table transform pipelines ────────────────────────────

def transform_vendor(df: DataFrame) -> DataFrame:
    """Vendor: drop vendor_tier, convert active_flag to BOOLEAN."""
    df = df.drop("vendor_tier")
    df = char_to_boolean(df, "active_flag")
    return df


def transform_inventory(df: DataFrame) -> DataFrame:
    """Inventory: DECIMAL→FLOAT on unit_cost, +5hr TZ drift on last_counted_ts."""
    df = decimal_to_float(df, "unit_cost")
    df = shift_timestamp(df, "last_counted_ts", hours=5)
    return df


def transform_purchase_order(df: DataFrame) -> DataFrame:
    """Purchase order: clean table — no transforms needed."""
    return df


def transform_inventory_transaction(df: DataFrame) -> DataFrame:
    """Inventory transaction: status_code→is_deleted, +5hr TZ drift on txn_ts,
    filter out soft-deleted rows."""
    df = shift_timestamp(df, "txn_ts", hours=5)
    df = status_code_to_is_deleted(df)
    # Filter out soft-deleted rows (simulates target not receiving them)
    df = df.filter(F.col("is_deleted") == False)  # noqa: E712
    return df


def transform_supplier_contract(df: DataFrame) -> DataFrame:
    """Supplier contract: empty→NULL on terms_notes, Y/N→BOOLEAN on auto_renew."""
    df = empty_to_null(df, "terms_notes")
    df = char_to_boolean(df, "auto_renew")
    return df


# ── Registry ─────────────────────────────────────────────────

TRANSFORM_REGISTRY = {
    "vendor": transform_vendor,
    "inventory": transform_inventory,
    "purchase_order": transform_purchase_order,
    "inventory_transaction": transform_inventory_transaction,
    "supplier_contract": transform_supplier_contract,
}
