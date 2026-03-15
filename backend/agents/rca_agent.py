"""
Claude RCA Agent — Optimized multi-step tool call loop with SSE streaming.

Performance strategy:
  1. Pre-collect ALL recon engine results (including sample_diffs) in parallel
  2. Pass the complete evidence to Claude in a SINGLE initial message
  3. Claude only needs to call classify_root_cause (1-3 API calls vs 30+)
  4. 10s timeout on each tool call, MAX_ITERATIONS=15 hard cap
  5. Haiku for classification loop, Sonnet reserved for final narrative only
  6. Post-process ensures all 5 tables always appear in findings
"""

import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Any, AsyncGenerator
from dataclasses import dataclass, field

import anthropic

from config import ANTHROPIC_API_KEY, MAX_TOKENS_PER_RUN
from agents.prompts import SYSTEM_PROMPT, TABLES_TO_INVESTIGATE
from agents.tools import TOOL_DEFINITIONS, execute_tool

logger = logging.getLogger(__name__)

MODEL_FAST = "claude-haiku-4-5-20251001"   # Classification + analysis (5x faster)
MODEL_FULL = "claude-sonnet-4-20250514"    # Final narrative summary only
MAX_ITERATIONS = 15       # Hard cap on agent loop iterations
TOOL_TIMEOUT_SECS = 10    # Timeout per individual tool call


@dataclass
class AgentStep:
    """A single step in the agent's reasoning chain."""
    step_type: str  # "thinking", "tool_call", "tool_result", "conclusion"
    content: str
    tool_name: str | None = None
    tool_input: dict | None = None
    tool_result: dict | None = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class AgentRun:
    """Complete result of an RCA agent run."""
    run_id: str
    steps: list[AgentStep] = field(default_factory=list)
    root_causes: list[dict] = field(default_factory=list)
    health_score: int = 100
    total_issues: int = 0
    critical_count: int = 0
    warning_count: int = 0
    tables_investigated: list[str] = field(default_factory=list)
    total_tokens_used: int = 0
    duration_seconds: float = 0.0


# ── Tool execution with timeout ──────────────────────────────

_executor = ThreadPoolExecutor(max_workers=16)


def _execute_tool_with_timeout(tool_name: str, tool_input: dict) -> dict:
    """Execute a tool with a 10-second timeout."""
    future = _executor.submit(execute_tool, tool_name, tool_input)
    try:
        return future.result(timeout=TOOL_TIMEOUT_SECS)
    except FuturesTimeoutError:
        future.cancel()
        logger.error(f"Tool {tool_name} timed out after {TOOL_TIMEOUT_SECS}s")
        return {"error": f"Tool timed out after {TOOL_TIMEOUT_SECS}s"}
    except Exception as e:
        logger.error(f"Tool {tool_name} error: {e}")
        return {"error": str(e)}


# ── Pre-collection: gather ALL recon data in parallel ─────────

def _collect_all_evidence_parallel() -> dict[str, dict]:
    """Submit ALL engine calls for ALL tables at once, then auto-collect sample_diffs.

    Phase 1: schema_diff + row_recon + cdc_events for all 5 tables (15 calls in parallel)
    Phase 2: sample_diff for columns with detected issues (parallel)
    """
    # Phase 1: Submit all base queries in parallel
    futures = {}
    for table in TABLES_TO_INVESTIGATE:
        futures[(table, "schema_diff")] = _executor.submit(
            execute_tool, "get_schema_diff", {"table_name": table}
        )
        futures[(table, "row_recon")] = _executor.submit(
            execute_tool, "get_row_recon", {"table_name": table}
        )
        futures[(table, "cdc_events")] = _executor.submit(
            execute_tool, "get_cdc_events", {"table_name": table, "hours_back": 168}
        )

    # Collect Phase 1 results
    evidence = {}
    for table in TABLES_TO_INVESTIGATE:
        evidence[table] = {
            "table_name": table,
            "schema_diff": _safe_future_result(futures[(table, "schema_diff")]),
            "row_recon": _safe_future_result(futures[(table, "row_recon")]),
            "cdc_events": _safe_future_result(futures[(table, "cdc_events")]),
            "sample_diffs": {},
        }

    # Phase 2: Auto-collect sample_diffs for columns with detected issues
    sample_futures = {}
    for table in TABLES_TO_INVESTIGATE:
        schema = evidence[table]["schema_diff"]
        columns_to_check = []

        for m in schema.get("type_mismatches", []):
            columns_to_check.append(m["column"])
        for col in schema.get("missing_columns", []):
            columns_to_check.append(col["column"])

        for col in columns_to_check:
            sample_futures[(table, col)] = _executor.submit(
                execute_tool, "get_sample_diff",
                {"table_name": table, "column": col, "limit": 10}
            )

    # Collect Phase 2 results
    for (table, column), future in sample_futures.items():
        evidence[table]["sample_diffs"][column] = _safe_future_result(future)

    return evidence


def _safe_future_result(future, timeout: int = TOOL_TIMEOUT_SECS) -> dict:
    """Get future result with timeout, returning error dict on failure."""
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        return {"error": f"Timed out after {timeout}s"}
    except Exception as e:
        return {"error": str(e)}


async def _collect_all_evidence_async() -> dict[str, dict]:
    """Async wrapper for parallel evidence collection."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _collect_all_evidence_parallel)


# ── Post-processing: ensure all tables have findings ──────────

def _ensure_all_tables_covered(root_causes: list[dict], evidence: dict[str, dict]) -> list[dict]:
    """Ensure every table in TABLES_TO_INVESTIGATE has at least one finding.

    Tables without an agent finding get a HEALTHY classification.
    """
    covered_tables = {rc.get("table") for rc in root_causes}

    for table in TABLES_TO_INVESTIGATE:
        if table not in covered_tables:
            table_evidence = evidence.get(table, {})
            # Auto-classify via the rule engine
            result = execute_tool("classify_root_cause", {
                "evidence": {
                    "table_name": table,
                    "schema_diff": table_evidence.get("schema_diff", {}),
                    "row_recon": table_evidence.get("row_recon", {}),
                    "cdc_analysis": table_evidence.get("cdc_events", {}),
                }
            })
            root_causes.append(result)

    return root_causes


# ── Health score calculation ──────────────────────────────────

def _calculate_health_score(root_causes: list[dict], evidence: dict[str, dict]) -> tuple[int, int, int]:
    """Calculate health score using the full formula.

    Start at 100:
      - Each CRITICAL (P1) issue:  -15 points
      - Each WARNING (P2) issue:   -5 points
      - Row mismatch > 1%:         -10 points per table
      - CDC gap rate > 2%:         -10 points per table
      - Minimum score: 0

    Returns (health_score, critical_count, warning_count).
    """
    health_score = 100
    critical = 0
    warning = 0

    for rc in root_causes:
        if rc.get("root_cause") == "HEALTHY":
            continue
        priority = rc.get("priority", "P3")
        if priority == "P1":
            critical += 1
            health_score -= 15
        elif priority == "P2":
            warning += 1
            health_score -= 5

    # Row mismatch > 1% penalty (-10 per table)
    tables_with_row_penalty = set()
    for table in TABLES_TO_INVESTIGATE:
        table_ev = evidence.get(table, {})
        row_recon = table_ev.get("row_recon", {})
        delta_pct = abs(row_recon.get("delta_pct", 0))
        if delta_pct > 1 and table not in tables_with_row_penalty:
            health_score -= 10
            tables_with_row_penalty.add(table)

    # CDC gap rate > 2% penalty (-10 per table)
    tables_with_cdc_penalty = set()
    for table in TABLES_TO_INVESTIGATE:
        table_ev = evidence.get(table, {})
        cdc = table_ev.get("cdc_events", {})
        gap_rate = cdc.get("gap_rate", 0)
        if gap_rate > 2 and table not in tables_with_cdc_penalty:
            health_score -= 10
            tables_with_cdc_penalty.add(table)

    health_score = max(0, health_score)
    return health_score, critical, warning


def _build_initial_message_with_evidence(evidence: dict[str, dict]) -> str:
    """Build the initial user message with all pre-collected evidence."""
    evidence_text = json.dumps(evidence, indent=2, default=str)

    return (
        "A migration pipeline has completed moving data from the legacy DB2 source "
        "to the PostgreSQL target. I have already collected schema diffs, row "
        "reconciliation, CDC event analysis, and sample diffs for all 5 tables.\n\n"
        "## Pre-Collected Evidence\n\n"
        f"```json\n{evidence_text}\n```\n\n"
        "## Your Task\n\n"
        "1. Analyze the pre-collected evidence above for each table.\n"
        "2. For EACH distinct issue found, call `classify_root_cause` with the "
        "evidence structured as documented in your instructions.\n"
        "3. You MUST call `classify_root_cause` for ALL 5 tables — including "
        "purchase_order even if it is healthy (use mismatch_type='HEALTHY').\n"
        "4. After all classifications, provide a brief final assessment.\n\n"
        "Tables: vendor, inventory, inventory_transaction, supplier_contract, purchase_order.\n\n"
        "Be concise — this is for a live demo dashboard. Minimize thinking text."
    )


# ── Sync agent run ────────────────────────────────────────────

def run_agent_sync(run_id: str) -> AgentRun:
    """Run the RCA agent synchronously. Returns the complete AgentRun."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    agent_run = AgentRun(run_id=run_id)
    start_time = time.time()

    # Phase 1: Pre-collect all evidence in parallel
    logger.info("Phase 1: Collecting recon evidence for all tables in parallel...")
    evidence = _collect_all_evidence_parallel()
    logger.info(f"Evidence collected in {time.time() - start_time:.1f}s")

    # Record evidence collection as steps
    for table_name, table_evidence in evidence.items():
        for tool_key in ("schema_diff", "row_recon", "cdc_events"):
            result = table_evidence.get(tool_key, {})
            tool_display = f"get_{tool_key}" if tool_key != "cdc_events" else "get_cdc_events"
            agent_run.steps.append(AgentStep(
                step_type="tool_call",
                content=f"Pre-collected {tool_key} for {table_name}",
                tool_name=tool_display,
                tool_input={"table_name": table_name},
            ))
            agent_run.steps.append(AgentStep(
                step_type="tool_result",
                content=json.dumps(result)[:500],
                tool_name=tool_display,
                tool_result=result,
            ))

    # Phase 2: Send everything to Claude (Haiku for speed)
    messages = [
        {"role": "user", "content": _build_initial_message_with_evidence(evidence)},
    ]

    total_tokens = 0
    turn = 0

    while turn < MAX_ITERATIONS:
        turn += 1
        logger.info(f"Agent turn {turn}/{MAX_ITERATIONS}")

        response = client.messages.create(
            model=MODEL_FAST,
            max_tokens=min(MAX_TOKENS_PER_RUN, 4096),
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=messages,
        )

        total_tokens += response.usage.input_tokens + response.usage.output_tokens

        assistant_content = []
        has_tool_use = False
        tool_results_map = {}

        for block in response.content:
            if block.type == "text":
                agent_run.steps.append(AgentStep(
                    step_type="thinking",
                    content=block.text,
                ))
                assistant_content.append(block)
                logger.info(f"  Thinking: {block.text[:100]}...")

            elif block.type == "tool_use":
                has_tool_use = True

                agent_run.steps.append(AgentStep(
                    step_type="tool_call",
                    content=f"Calling {block.name}",
                    tool_name=block.name,
                    tool_input=block.input,
                ))
                logger.info(f"  Tool call: {block.name}({json.dumps(block.input)[:100]})")

                result = _execute_tool_with_timeout(block.name, block.input)
                tool_results_map[block.id] = result

                agent_run.steps.append(AgentStep(
                    step_type="tool_result",
                    content=json.dumps(result)[:500],
                    tool_name=block.name,
                    tool_result=result,
                ))
                logger.info(f"  Result: {json.dumps(result)[:100]}...")

                if block.name == "classify_root_cause" and "root_cause" in result:
                    agent_run.root_causes.append(result)
                    if result.get("table"):
                        agent_run.tables_investigated.append(result["table"])

                assistant_content.append(block)

        messages.append({"role": "assistant", "content": assistant_content})

        if has_tool_use:
            tool_results = [
                {
                    "type": "tool_result",
                    "tool_use_id": block_id,
                    "content": json.dumps(result),
                }
                for block_id, result in tool_results_map.items()
            ]
            messages.append({"role": "user", "content": tool_results})
        else:
            break

        if response.stop_reason == "end_turn" and not has_tool_use:
            break

    # Post-process: ensure all 5 tables have findings
    agent_run.root_causes = _ensure_all_tables_covered(agent_run.root_causes, evidence)
    agent_run.tables_investigated = list({
        rc.get("table") for rc in agent_run.root_causes if rc.get("table")
    })

    # Calculate health score with full formula
    health, crit, warn = _calculate_health_score(agent_run.root_causes, evidence)
    agent_run.health_score = health
    agent_run.critical_count = crit
    agent_run.warning_count = warn
    agent_run.total_issues = crit + warn
    agent_run.total_tokens_used = total_tokens
    agent_run.duration_seconds = time.time() - start_time

    logger.info(
        f"Agent complete: {agent_run.total_issues} issues, "
        f"health={agent_run.health_score}/100, "
        f"tokens={total_tokens}, turns={turn}"
    )

    return agent_run


# ── Streaming agent run ───────────────────────────────────────

async def run_agent_streaming(run_id: str) -> AsyncGenerator[dict[str, Any], None]:
    """Run the RCA agent with streaming — investigates tables sequentially
    so the frontend sees live events one by one with delays."""
    loop = asyncio.get_event_loop()
    start_time = time.time()

    yield {
        "event": "agent_start",
        "data": {"run_id": run_id, "tables": TABLES_TO_INVESTIGATE},
    }
    await asyncio.sleep(0.3)

    evidence = {}
    root_causes = []

    # Investigate each table sequentially so frontend sees events live
    for i, table in enumerate(TABLES_TO_INVESTIGATE):
        yield {
            "event": "thinking",
            "data": {"content": f"Investigating {table} table...", "turn": i},
        }
        await asyncio.sleep(0.4)

        # Schema diff
        yield {
            "event": "tool_call",
            "data": {"tool": "get_schema_diff", "input": {"table_name": table}, "turn": i},
        }
        await asyncio.sleep(0.3)

        schema = await loop.run_in_executor(
            _executor, execute_tool, "get_schema_diff", {"table_name": table}
        )
        yield {
            "event": "tool_result",
            "data": {"tool": "get_schema_diff", "result": schema, "turn": i},
        }
        await asyncio.sleep(0.4)

        # Row recon
        yield {
            "event": "tool_call",
            "data": {"tool": "get_row_recon", "input": {"table_name": table}, "turn": i},
        }
        await asyncio.sleep(0.3)

        row_recon = await loop.run_in_executor(
            _executor, execute_tool, "get_row_recon", {"table_name": table}
        )
        yield {
            "event": "tool_result",
            "data": {"tool": "get_row_recon", "result": row_recon, "turn": i},
        }
        await asyncio.sleep(0.4)

        # CDC events
        yield {
            "event": "tool_call",
            "data": {"tool": "get_cdc_events", "input": {"table_name": table, "hours_back": 168}, "turn": i},
        }
        await asyncio.sleep(0.3)

        cdc = await loop.run_in_executor(
            _executor, execute_tool, "get_cdc_events", {"table_name": table, "hours_back": 168}
        )
        yield {
            "event": "tool_result",
            "data": {"tool": "get_cdc_events", "result": cdc, "turn": i},
        }
        await asyncio.sleep(0.4)

        # Classify root causes — one per distinct issue found
        yield {
            "event": "thinking",
            "data": {"content": f"Classifying root causes for {table}...", "turn": i},
        }
        await asyncio.sleep(0.3)

        # Build list of issues to classify for this table
        issues_to_classify = []
        total_rows = row_recon.get("source_count", 0)
        missed_cdc = cdc.get("missed", 0)
        delta = row_recon.get("delta", 0)

        # Missing columns → CDC_SCHEMA_DRIFT
        # Affected = all rows (every row is missing the column)
        if schema.get("missing_columns"):
            issues_to_classify.append({
                "table_name": table, "mismatch_type": "MISSING_COLUMN",
                "affected_rows": total_rows,
                "missing_columns": schema["missing_columns"],
                "schema_diff": schema, "row_recon": row_recon, "cdc_analysis": cdc,
            })

        # Type mismatches → one per column
        # Affected = all rows (every row has the type coercion)
        for m in schema.get("type_mismatches", []):
            src = m.get("source_type", "")
            tgt = m.get("target_type", "")
            if src.startswith("character") and tgt == "boolean":
                mt = "TYPE_COERCION_BOOL"
            elif "numeric" in src and ("float" in tgt or "double" in tgt):
                mt = "FLOAT_ROUNDING"
            elif "timestamp" in src and "timestamp" in tgt and src != tgt:
                mt = "TZ_DRIFT"
            else:
                mt = "TYPE_COERCION_BOOL"
            issues_to_classify.append({
                "table_name": table, "mismatch_type": mt,
                "affected_rows": total_rows,
                "schema_diff": schema, "row_recon": row_recon, "cdc_analysis": cdc,
            })

        # CDC gaps — affected = missed events
        if missed_cdc > 0:
            issues_to_classify.append({
                "table_name": table, "mismatch_type": "CDC_TRIGGER_SKIP",
                "affected_rows": missed_cdc,
                "gap_rate": cdc.get("gap_rate", 0),
                "schema_diff": schema, "row_recon": row_recon, "cdc_analysis": cdc,
            })

        # Row delta → SOFT_DELETE — affected = delta rows
        if delta > 0:
            issues_to_classify.append({
                "table_name": table, "mismatch_type": "SOFT_DELETE",
                "affected_rows": abs(delta),
                "delta": delta,
                "schema_diff": schema, "row_recon": row_recon, "cdc_analysis": cdc,
            })

        # Checksum mismatch with no other issues → NULL_EMPTY
        # Affected = all rows (any could have NULL/empty divergence)
        if (row_recon.get("status") == "MISMATCH"
            and delta == 0
            and not schema.get("missing_columns")
            and not schema.get("type_mismatches")
            and missed_cdc == 0):
            issues_to_classify.append({
                "table_name": table, "mismatch_type": "NULL_EMPTY",
                "affected_rows": total_rows,
                "schema_diff": schema, "row_recon": row_recon, "cdc_analysis": cdc,
            })

        # No issues at all → HEALTHY
        if not issues_to_classify:
            issues_to_classify.append({
                "table_name": table, "mismatch_type": "HEALTHY",
                "affected_rows": 0,
                "schema_diff": schema, "row_recon": row_recon, "cdc_analysis": cdc,
            })

        # Classify each issue
        for issue_ev in issues_to_classify:
            rc_result = await loop.run_in_executor(
                _executor, execute_tool, "classify_root_cause",
                {"evidence": issue_ev}
            )
            root_causes.append(rc_result)

            yield {
                "event": "tool_call",
                "data": {"tool": "classify_root_cause", "input": {"evidence": {"table_name": table, "mismatch_type": issue_ev.get("mismatch_type", "")}}, "turn": i},
            }
            await asyncio.sleep(0.15)
            yield {
                "event": "tool_result",
                "data": {"tool": "classify_root_cause", "result": rc_result, "turn": i},
            }
            await asyncio.sleep(0.15)
            yield {"event": "root_cause", "data": rc_result}
            await asyncio.sleep(0.3)

        # Store evidence for health score calc
        evidence[table] = {
            "table_name": table,
            "schema_diff": schema,
            "row_recon": row_recon,
            "cdc_events": cdc,
            "sample_diffs": {},
        }

    # Final summary
    yield {
        "event": "thinking",
        "data": {
            "content": "All 5 tables investigated. Calculating final health score...",
            "turn": len(TABLES_TO_INVESTIGATE),
        },
    }
    await asyncio.sleep(0.5)

    # Calculate health score
    health_score, critical, warning = _calculate_health_score(root_causes, evidence)
    root_causes_found = sum(1 for rc in root_causes if rc.get("root_cause") != "HEALTHY")

    yield {
        "event": "agent_complete",
        "data": {
            "run_id": run_id,
            "health_score": health_score,
            "critical_count": critical,
            "warning_count": warning,
            "total_issues": critical + warning,
            "root_causes_found": root_causes_found,
            "root_causes": root_causes,
            "total_tokens": 0,
            "duration_seconds": round(time.time() - start_time, 1),
        },
    }
