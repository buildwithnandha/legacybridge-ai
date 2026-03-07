"""Target (Modern PostgreSQL) schema definitions for PySpark."""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, FloatType, DecimalType, TimestampType, LongType,
)

VENDOR_SCHEMA = StructType([
    StructField("vendor_id", StringType(), False),
    StructField("vendor_name", StringType(), False),
    # vendor_tier intentionally absent — MISSING_COLUMN drift
    StructField("payment_terms", IntegerType(), False),
    StructField("lead_time_days", IntegerType(), False),
    StructField("active_flag", BooleanType(), False),      # CHAR→BOOLEAN
    StructField("country_code", StringType(), False),
    StructField("currency_code", StringType(), False),
    StructField("created_ts", TimestampType(), False),
    StructField("updated_ts", TimestampType(), False),
    StructField("updated_by", StringType(), False),
])

INVENTORY_SCHEMA = StructType([
    StructField("item_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("on_hand_qty", DecimalType(10, 2), False),
    StructField("reorder_point", IntegerType(), False),
    StructField("reorder_qty", IntegerType(), False),
    StructField("unit_cost", FloatType(), False),           # DECIMAL→FLOAT
    StructField("status_code", StringType(), False),
    StructField("last_counted_ts", TimestampType(), False), # TZ drift applied
    StructField("updated_ts", TimestampType(), False),
    StructField("updated_by", StringType(), False),
])

PURCHASE_ORDER_SCHEMA = StructType([
    StructField("po_number", StringType(), False),
    StructField("vendor_id", StringType(), False),
    StructField("order_date", StringType(), False),
    StructField("expected_date", StringType(), False),
    StructField("total_amount", DecimalType(15, 2), False),
    StructField("po_status", StringType(), False),
    StructField("line_count", IntegerType(), False),
    StructField("approved_by", StringType(), False),
    StructField("created_ts", TimestampType(), False),
    StructField("updated_ts", TimestampType(), False),
])

INVENTORY_TRANSACTION_SCHEMA = StructType([
    StructField("txn_id", LongType(), False),
    StructField("item_id", StringType(), False),
    StructField("txn_type", StringType(), False),
    StructField("txn_qty", DecimalType(10, 2), False),
    StructField("txn_ts", TimestampType(), False),          # TZ drift applied
    StructField("location_id", StringType(), False),
    StructField("reference_id", StringType(), True),
    StructField("created_by", StringType(), False),
    StructField("is_deleted", BooleanType(), False),        # Replaces status_code
])

SUPPLIER_CONTRACT_SCHEMA = StructType([
    StructField("contract_id", StringType(), False),
    StructField("vendor_id", StringType(), False),
    StructField("start_date", StringType(), False),
    StructField("end_date", StringType(), False),
    StructField("contract_value", DecimalType(15, 2), False),
    StructField("terms_notes", StringType(), True),         # Empty→NULL
    StructField("payment_freq", StringType(), False),
    StructField("auto_renew", BooleanType(), False),        # CHAR→BOOLEAN
    StructField("created_ts", TimestampType(), False),
])

TARGET_SCHEMAS = {
    "vendor": VENDOR_SCHEMA,
    "inventory": INVENTORY_SCHEMA,
    "purchase_order": PURCHASE_ORDER_SCHEMA,
    "inventory_transaction": INVENTORY_TRANSACTION_SCHEMA,
    "supplier_contract": SUPPLIER_CONTRACT_SCHEMA,
}
