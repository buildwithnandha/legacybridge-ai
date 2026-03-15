"""
RCA Agent Tool Definitions — All 6 tools for Claude tool_use.

Each tool has:
  - A JSON schema definition for the Anthropic API
  - A handler function that calls the corresponding engine
"""

from typing import Any

from engines.schema_differ import get_schema_diff
from engines.row_reconciler import get_row_recon
from engines.cdc_analyzer import get_cdc_events
from engines.sample_differ import get_sample_diff
from engines.pipeline_logger import get_pipeline_logs

# ── Tool Definitions (Anthropic tool_use format) ─────────────

TOOL_DEFINITIONS = [
    {
        "name": "get_schema_diff",
        "description": (
            "Compare the schema of a table between the source (DB2) and target (PostgreSQL) databases. "
            "Returns missing columns, extra columns, and type mismatches with severity classification. "
            "Use this first when investigating a table to understand structural differences."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to compare schemas for.",
                    "enum": ["vendor", "inventory", "purchase_order", "inventory_transaction", "supplier_contract"],
                },
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "get_row_recon",
        "description": (
            "Compare row counts and checksums between source and target for a table. "
            "Returns source count, target count, delta, delta percentage, and checksum match status. "
            "Use this to quantify the data loss or mismatch scope."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to reconcile.",
                    "enum": ["vendor", "inventory", "purchase_order", "inventory_transaction", "supplier_contract"],
                },
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "get_cdc_events",
        "description": (
            "Analyze CDC (Change Data Capture) event logs for a table. "
            "Returns total events, captured count, missed count, gap rate, and gap patterns. "
            "Use this to check if data sync issues are caused by CDC trigger gaps."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to check CDC events for.",
                    "enum": ["vendor", "inventory", "purchase_order", "inventory_transaction", "supplier_contract"],
                },
                "hours_back": {
                    "type": "integer",
                    "description": "How many hours back to analyze. Default 168 (7 days).",
                    "default": 168,
                },
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "get_sample_diff",
        "description": (
            "Fetch and compare sample row values for a specific column between source and target. "
            "Returns side-by-side comparisons showing exact value differences. "
            "Use this to confirm the nature of a mismatch (rounding, TZ drift, NULL handling, etc.)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table.",
                    "enum": ["vendor", "inventory", "purchase_order", "inventory_transaction", "supplier_contract"],
                },
                "column": {
                    "type": "string",
                    "description": "Column name to compare values for.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of sample rows to return. Default 10.",
                    "default": 10,
                },
            },
            "required": ["table_name", "column"],
        },
    },
    {
        "name": "get_pipeline_logs",
        "description": (
            "Read Airflow pipeline task logs and extract structured metrics. "
            "Returns task status, duration, records processed/failed, and Spark metrics. "
            "Use this to check if ETL failures or performance issues contributed to data problems."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "Airflow DAG ID. Default: legacy_migration_pipeline.",
                    "default": "legacy_migration_pipeline",
                },
                "run_id": {
                    "type": "string",
                    "description": "Specific DAG run ID. Empty for latest.",
                    "default": "",
                },
                "task_id": {
                    "type": "string",
                    "description": "Specific task ID to inspect. Empty for all tasks.",
                    "default": "",
                },
            },
            "required": [],
        },
    },
    {
        "name": "classify_root_cause",
        "description": (
            "Given collected evidence from previous tool calls, classify the root cause of a data mismatch. "
            "Returns the root cause category, confidence score, affected row count, recommended fix, and priority. "
            "Use this as the FINAL step after gathering sufficient evidence from other tools. "
            "Call this for EVERY table including healthy ones (use mismatch_type='HEALTHY' for clean tables)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "evidence": {
                    "type": "object",
                    "description": (
                        "Collected evidence object containing findings from prior tool calls. "
                        "Should include table_name, mismatch_type, schema_diff results, "
                        "row_recon results, cdc analysis, and sample diffs as applicable."
                    ),
                },
            },
            "required": ["evidence"],
        },
    },
]


# ── Root Cause Classifier ────────────────────────────────────

ROOT_CAUSE_RULES = {
    "MISSING_COLUMN": {
        "root_cause": "CDC_SCHEMA_DRIFT",
        "confidence": 0.94,
        "recommended_fix": "Add missing column to target schema and backfill from source. Update CDC configuration to include the column in change payloads.",
        "priority": "P1",
    },
    "TYPE_COERCION_BOOL": {
        "root_cause": "TYPE_COERCION",
        "confidence": 0.91,
        "recommended_fix": "Standardize column type across source and target. Add explicit CHAR→BOOLEAN mapping in the ETL transform layer.",
        "priority": "P2",
    },
    "CDC_TRIGGER_SKIP": {
        "root_cause": "CDC_TRIGGER_GAP",
        "confidence": 0.96,
        "recommended_fix": "Modify CDC triggers to capture BATCH_JOB updates. Consider using log-based CDC instead of trigger-based to capture all DML.",
        "priority": "P1",
    },
    "FLOAT_ROUNDING": {
        "root_cause": "TYPE_COERCION",
        "confidence": 0.88,
        "recommended_fix": "Change target column from FLOAT to DECIMAL with matching precision. Re-run ETL to restore precision.",
        "priority": "P2",
    },
    "TZ_DRIFT": {
        "root_cause": "TZ_MISMATCH",
        "confidence": 0.92,
        "recommended_fix": "Normalize all timestamps to UTC in the transform layer. Store with TIMESTAMPTZ type in target. Apply retroactive correction to affected rows.",
        "priority": "P2",
    },
    "SOFT_DELETE": {
        "root_cause": "SOFT_DELETE_MISMATCH",
        "confidence": 0.95,
        "recommended_fix": "Include soft-deleted rows in target with is_deleted flag. Modify ETL to preserve all rows and map status_code DEL to is_deleted=true.",
        "priority": "P1",
    },
    "NULL_EMPTY": {
        "root_cause": "NULL_EMPTY_MISMATCH",
        "confidence": 0.89,
        "recommended_fix": "Standardize NULL handling: treat empty strings as NULL consistently in the transform layer. Apply COALESCE/NULLIF in ETL.",
        "priority": "P3",
    },
    "HEALTHY": {
        "root_cause": "HEALTHY",
        "confidence": 0.99,
        "recommended_fix": "No action required. Table is healthy.",
        "priority": "P3",
    },
}


def _is_healthy(
    missing_columns: list,
    type_mismatches: list,
    gap_rate: float,
    delta: int,
    diff_type: str,
    schema_diff: dict,
    row_recon: dict,
    cdc_analysis: dict,
) -> bool:
    """Return True if all evidence indicates the table is clean."""
    if missing_columns or type_mismatches:
        return False
    if gap_rate > 0:
        return False
    if delta != 0:
        return False
    if diff_type:
        return False
    if schema_diff.get("severity") in ("CRITICAL", "WARNING"):
        return False
    if row_recon.get("status") == "MISMATCH":
        return False
    if cdc_analysis.get("missed", 0) > 0:
        return False
    return True


def _resolve_affected_rows(
    rule_key: str,
    evidence: dict,
    schema_diff: dict,
    row_recon: dict,
    cdc_analysis: dict,
    sample_diff: dict,
) -> int:
    """Determine correct affected_rows based on the root cause type.

    Priority: explicit evidence.affected_rows > type-specific source > 0
    """
    root_cause = ROOT_CAUSE_RULES.get(rule_key, {}).get("root_cause", "")

    if root_cause == "HEALTHY":
        return 0

    # If caller explicitly set affected_rows, use it
    explicit = evidence.get("affected_rows", 0)
    if explicit and explicit > 0:
        return explicit

    # Type-specific fallbacks
    if root_cause in ("CDC_SCHEMA_DRIFT", "CDC_TRIGGER_GAP"):
        missed = cdc_analysis.get("missed", 0)
        if missed > 0:
            return missed
        # Fallback: all rows affected (e.g. missing column affects every row)
        return row_recon.get("source_count", 0)

    if root_cause in ("TYPE_COERCION", "TZ_MISMATCH", "NULL_EMPTY_MISMATCH"):
        if sample_diff.get("total_affected"):
            return sample_diff["total_affected"]
        sample_diffs = evidence.get("sample_diffs", {})
        for sd in sample_diffs.values():
            if sd.get("total_affected"):
                return sd["total_affected"]
        return row_recon.get("source_count", 0)

    if root_cause == "SOFT_DELETE_MISMATCH":
        return abs(row_recon.get("delta", 0))

    return 0


def classify_root_cause(evidence: dict[str, Any]) -> dict[str, Any]:
    """Classify root cause based on collected evidence.

    The classifier inspects the evidence dict for known patterns and returns
    a structured root cause classification. It handles both flat evidence
    (with top-level keys) and nested evidence (with schema_diff, row_recon, etc.).
    """
    mismatch_type = evidence.get("mismatch_type", "")
    table_name = evidence.get("table_name", "unknown")

    # Extract nested evidence if present
    schema_diff = evidence.get("schema_diff", {})
    row_recon = evidence.get("row_recon", {})
    cdc_analysis = evidence.get("cdc_analysis", {})
    sample_diff = evidence.get("sample_diff", {})

    # Merge nested fields into top-level for matching
    missing_columns = evidence.get("missing_columns") or schema_diff.get("missing_columns", [])
    type_mismatches = schema_diff.get("type_mismatches", [])
    gap_rate = evidence.get("gap_rate", 0) or cdc_analysis.get("gap_rate", 0)
    delta = evidence.get("delta", 0) or row_recon.get("delta", 0)
    diff_type = evidence.get("diff_type", "") or sample_diff.get("diff_type", "")

    # Determine matching rule key
    rule_key = None

    # Try explicit mismatch_type first
    if mismatch_type in ROOT_CAUSE_RULES:
        rule_key = mismatch_type
    else:
        # Infer from evidence fields (check most specific patterns first)
        if missing_columns:
            rule_key = "MISSING_COLUMN"
        elif mismatch_type == "TYPE_COERCION_BOOL" or any(
            m.get("column") == "active_flag" for m in type_mismatches
        ):
            rule_key = "TYPE_COERCION_BOOL"
        elif gap_rate > 0 or mismatch_type == "CDC_TRIGGER_SKIP":
            rule_key = "CDC_TRIGGER_SKIP"
        elif diff_type == "FLOAT_ROUNDING" or mismatch_type == "FLOAT_ROUNDING":
            rule_key = "FLOAT_ROUNDING"
        elif diff_type == "TZ_DRIFT" or mismatch_type == "TZ_DRIFT":
            rule_key = "TZ_DRIFT"
        elif diff_type == "NULL_EMPTY" or mismatch_type == "NULL_EMPTY":
            rule_key = "NULL_EMPTY"
        elif diff_type == "TYPE_COERCION_BOOL":
            rule_key = "TYPE_COERCION_BOOL"
        elif delta > 0:
            rule_key = "SOFT_DELETE"
        elif schema_diff.get("severity") in ("CRITICAL", "WARNING"):
            rule_key = "MISSING_COLUMN"
        elif row_recon.get("status") == "MISMATCH":
            rule_key = "SOFT_DELETE"
        elif _is_healthy(missing_columns, type_mismatches, gap_rate, delta, diff_type, schema_diff, row_recon, cdc_analysis):
            rule_key = "HEALTHY"

    if rule_key and rule_key in ROOT_CAUSE_RULES:
        rule = ROOT_CAUSE_RULES[rule_key]
    else:
        rule_key = "UNKNOWN"
        rule = {
            "root_cause": "UNKNOWN",
            "confidence": 0.50,
            "recommended_fix": "Manual investigation required. Review ETL logs and source/target data.",
            "priority": "P2",
        }

    # Resolve affected_rows based on root cause type
    affected_rows = _resolve_affected_rows(
        rule_key, evidence, schema_diff, row_recon, cdc_analysis, sample_diff
    )

    return {
        "root_cause": rule["root_cause"],
        "confidence": rule["confidence"],
        "affected_rows": affected_rows,
        "recommended_fix": rule["recommended_fix"],
        "priority": rule["priority"],
        "table": table_name,
        "evidence_summary": {
            k: v for k, v in evidence.items()
            if k not in ("samples",)  # exclude large sample arrays
        },
    }


# ── Tool Dispatcher ──────────────────────────────────────────

TOOL_HANDLERS = {
    "get_schema_diff": lambda args: get_schema_diff(args["table_name"]),
    "get_row_recon": lambda args: get_row_recon(args["table_name"]),
    "get_cdc_events": lambda args: get_cdc_events(
        args["table_name"], args.get("hours_back", 168)
    ),
    "get_sample_diff": lambda args: get_sample_diff(
        args["table_name"], args["column"], args.get("limit", 10)
    ),
    "get_pipeline_logs": lambda args: get_pipeline_logs(
        args.get("dag_id", "legacy_migration_pipeline"),
        args.get("run_id", ""),
        args.get("task_id", ""),
    ),
    "classify_root_cause": lambda args: classify_root_cause(args["evidence"]),
}


def execute_tool(tool_name: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    """Execute a tool by name with the given input arguments."""
    handler = TOOL_HANDLERS.get(tool_name)
    if not handler:
        return {"error": f"Unknown tool: {tool_name}"}
    return handler(tool_input)
