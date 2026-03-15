"""
Reconciliation endpoints — trigger recon, stream agent reasoning, get report/PDF.

Endpoints:
  POST /api/recon/run            — Trigger full recon + RCA agent, returns run_id
  GET  /api/recon/{run_id}/stream — SSE stream of live agent reasoning
  GET  /api/recon/{run_id}/report — Structured JSON report for dashboard
  GET  /api/recon/{run_id}/pdf    — Download PDF incident report
  GET  /api/recon/{run_id}/rca    — Trigger RCA agent only (used by Airflow operator)
  GET  /api/recon/history          — List past reconciliation runs
"""

import json
import os
import uuid
import logging
import asyncio
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse, Response
from sse_starlette.sse import EventSourceResponse

from config import DEMO_MODE
from agents.prompts import TABLES_TO_INVESTIGATE

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/recon", tags=["reconciliation"])

# In-memory store for runs (replace with DB in production)
_runs: dict[str, dict[str, Any]] = {}

# ── Demo data ─────────────────────────────────────────────────

_demo_data: dict | None = None


def _load_demo_data() -> dict:
    """Load and cache the demo result JSON."""
    global _demo_data
    if _demo_data is None:
        demo_path = os.path.join(os.path.dirname(__file__), "..", "data", "demo_result.json")
        with open(demo_path, "r") as f:
            _demo_data = json.load(f)
    return _demo_data


# ── Real engine runner ────────────────────────────────────────

def _run_quick_recon() -> dict[str, Any]:
    """Run the reconciliation engines across all tables (no AI agent)."""
    from engines.schema_differ import get_schema_diff
    from engines.row_reconciler import get_row_recon
    from engines.cdc_analyzer import get_cdc_events

    results = {}
    issues_found = 0
    critical = 0
    warning = 0

    for table in TABLES_TO_INVESTIGATE:
        schema = get_schema_diff(table)
        rows = get_row_recon(table)
        cdc = get_cdc_events(table)

        table_issues = []

        if schema["missing_columns"]:
            for col in schema["missing_columns"]:
                table_issues.append({
                    "type": "MISSING_COLUMN",
                    "severity": "CRITICAL",
                    "detail": f"Column '{col['column']}' ({col['type']}) missing in target",
                })
                critical += 1

        if schema["type_mismatches"]:
            for m in schema["type_mismatches"]:
                severity = "CRITICAL" if m["column"] in ("active_flag",) else "WARNING"
                table_issues.append({
                    "type": "TYPE_MISMATCH",
                    "severity": severity,
                    "detail": f"Column '{m['column']}': source={m['source_type']} target={m['target_type']}",
                })
                if severity == "CRITICAL":
                    critical += 1
                else:
                    warning += 1

        if schema["extra_columns"]:
            for col in schema["extra_columns"]:
                table_issues.append({
                    "type": "EXTRA_COLUMN",
                    "severity": "WARNING",
                    "detail": f"Column '{col['column']}' ({col['type']}) only in target",
                })
                warning += 1

        if rows["status"] == "MISMATCH":
            table_issues.append({
                "type": "ROW_COUNT_MISMATCH",
                "severity": "CRITICAL" if rows["delta_pct"] > 5 else "WARNING",
                "detail": f"source={rows['source_count']} target={rows['target_count']} delta={rows['delta']} ({rows['delta_pct']}%)",
            })
            if rows["delta_pct"] > 5:
                critical += 1
            else:
                warning += 1

        if cdc["missed"] > 0:
            table_issues.append({
                "type": "CDC_GAP",
                "severity": "CRITICAL",
                "detail": f"{cdc['missed']} events missed ({cdc['gap_rate']}% gap rate)",
            })
            critical += 1

        issues_found += len(table_issues)

        results[table] = {
            "schema_diff": schema,
            "row_recon": rows,
            "cdc_analysis": cdc,
            "issues": table_issues,
            "status": "CRITICAL" if any(i["severity"] == "CRITICAL" for i in table_issues)
                      else "WARNING" if table_issues
                      else "HEALTHY",
        }

    # Health score: full formula
    health_score = 100 - (critical * 15) - (warning * 5)

    # Row mismatch > 1% penalty (-10 per table)
    for table in TABLES_TO_INVESTIGATE:
        table_data = results.get(table, {})
        recon = table_data.get("row_recon", {})
        if abs(recon.get("delta_pct", 0)) > 1:
            health_score -= 10

    # CDC gap rate > 2% penalty (-10 per table)
    for table in TABLES_TO_INVESTIGATE:
        table_data = results.get(table, {})
        cdc = table_data.get("cdc_analysis", {})
        if cdc.get("gap_rate", 0) > 2:
            health_score -= 10

    health_score = max(0, health_score)

    return {
        "tables": results,
        "issues_found": issues_found,
        "critical_count": critical,
        "warning_count": warning,
        "health_score": health_score,
    }


# ── Endpoints ─────────────────────────────────────────────────

@router.post("/run")
async def trigger_recon_run():
    """Trigger a full reconciliation run. Returns run_id immediately."""
    run_id = str(uuid.uuid4())[:8]
    logger.info(f"Starting recon run: {run_id} (demo_mode={DEMO_MODE})")

    if DEMO_MODE:
        demo = _load_demo_data()
        recon_results = demo["recon"]

        _runs[run_id] = {
            "run_id": run_id,
            "status": "recon_complete",
            "started_at": datetime.utcnow().isoformat(),
            "recon": recon_results,
            "agent_run": None,
            "demo_mode": True,
        }

        return {
            "run_id": run_id,
            "status": "recon_complete",
            "issues_found": recon_results["issues_found"],
            "health_score": recon_results["health_score"],
            "stream_url": f"/api/recon/{run_id}/stream",
            "report_url": f"/api/recon/{run_id}/report",
            "demo_mode": True,
        }

    # Real mode
    recon_results = _run_quick_recon()

    _runs[run_id] = {
        "run_id": run_id,
        "status": "recon_complete",
        "started_at": datetime.utcnow().isoformat(),
        "recon": recon_results,
        "agent_run": None,
        "demo_mode": False,
    }

    return {
        "run_id": run_id,
        "status": "recon_complete",
        "issues_found": recon_results["issues_found"],
        "health_score": recon_results["health_score"],
        "stream_url": f"/api/recon/{run_id}/stream",
        "report_url": f"/api/recon/{run_id}/report",
        "demo_mode": False,
    }


@router.get("/{run_id}/stream")
async def stream_agent_reasoning(run_id: str):
    """SSE stream of live RCA agent reasoning.

    In demo mode, streams pre-saved steps with a 0.3s delay between each.
    In live mode, streams from the real Claude agent.
    """
    run_data = _runs.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    if run_data.get("demo_mode"):
        # Demo mode: stream pre-saved steps with delay
        async def demo_event_generator():
            demo = _load_demo_data()
            steps = demo["agent_steps"]

            for step in steps:
                # Store completion data BEFORE yielding the event
                # so the frontend can immediately use the PDF endpoint
                if step["event"] == "agent_complete":
                    _runs[run_id]["status"] = "complete"
                    _runs[run_id]["agent_run"] = step["data"]

                yield {
                    "event": step["event"],
                    "data": json.dumps(step["data"]),
                }
                await asyncio.sleep(0.3)

        return EventSourceResponse(
            demo_event_generator(),
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    # Live mode: stream from real agent
    from agents.rca_agent import run_agent_streaming

    async def event_generator():
        async for event in run_agent_streaming(run_id):
            # Store completion data BEFORE yielding the event
            if event["event"] == "agent_complete":
                _runs[run_id]["status"] = "complete"
                _runs[run_id]["agent_run"] = event["data"]

            yield {
                "event": event["event"],
                "data": json.dumps(event["data"]),
            }

    return EventSourceResponse(
        event_generator(),
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/{run_id}/report")
async def get_report(run_id: str):
    """Get the structured JSON report for the dashboard."""
    run_data = _runs.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return {
        "run_id": run_id,
        "status": run_data["status"],
        "started_at": run_data["started_at"],
        "recon": run_data["recon"],
        "agent_run": run_data.get("agent_run"),
        "demo_mode": run_data.get("demo_mode", False),
    }


@router.get("/{run_id}/pdf")
async def download_pdf(run_id: str):
    """Download the PDF incident report."""
    run_data = _runs.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    current_status = run_data.get("status", "unknown")
    logger.info(f"PDF requested for run {run_id}, current status: {current_status}")

    valid_complete_states = {"complete", "completed", "done", "success"}
    if current_status not in valid_complete_states:
        # Also allow if agent_run data exists (race condition workaround)
        if run_data.get("agent_run") is not None:
            logger.info(f"Run {run_id} has agent_run data, allowing PDF despite status={current_status}")
            _runs[run_id]["status"] = "complete"
        else:
            raise HTTPException(
                status_code=409,
                detail=f"Agent run not yet complete (status={current_status})",
            )

    # Generate PDF on demand
    from reports.pdf_generator import generate_pdf_from_run

    pdf_bytes = generate_pdf_from_run(run_data)

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=legacybridge-{run_id}.pdf"},
    )


@router.post("/{run_id}/rca")
async def trigger_rca_agent(run_id: str):
    """Trigger the RCA agent for an existing recon run (used by Airflow operator)."""
    run_data = _runs.get(run_id)

    if DEMO_MODE:
        # In demo mode, return pre-saved agent data
        demo = _load_demo_data()
        agent_steps = demo["agent_steps"]
        # Find the agent_complete event
        complete_data = next(
            (s["data"] for s in agent_steps if s["event"] == "agent_complete"),
            {},
        )
        _runs.setdefault(run_id, {
            "run_id": run_id,
            "status": "complete",
            "started_at": datetime.utcnow().isoformat(),
            "recon": demo["recon"],
            "agent_run": complete_data,
            "demo_mode": True,
        })
        _runs[run_id]["status"] = "complete"
        _runs[run_id]["agent_run"] = complete_data
        return complete_data

    # Real mode
    from agents.rca_agent import run_agent_sync

    if not run_data:
        recon_results = _run_quick_recon()
        _runs[run_id] = {
            "run_id": run_id,
            "status": "recon_complete",
            "started_at": datetime.utcnow().isoformat(),
            "recon": recon_results,
            "agent_run": None,
            "demo_mode": False,
        }
        run_data = _runs[run_id]

    agent_run = run_agent_sync(run_id)

    agent_data = {
        "run_id": run_id,
        "health_score": agent_run.health_score,
        "root_causes_found": sum(
            1 for rc in agent_run.root_causes if rc.get("root_cause") != "HEALTHY"
        ),
        "critical_count": agent_run.critical_count,
        "warning_count": agent_run.warning_count,
        "total_tokens": agent_run.total_tokens_used,
        "duration_seconds": agent_run.duration_seconds,
        "root_causes": agent_run.root_causes,
        "pdf_ready": True,
    }

    _runs[run_id]["status"] = "complete"
    _runs[run_id]["agent_run"] = agent_data

    return agent_data


@router.get("/history")
async def recon_history():
    """List past reconciliation runs."""
    return {
        "runs": [
            {
                "run_id": r["run_id"],
                "status": r["status"],
                "started_at": r["started_at"],
                "health_score": r["recon"]["health_score"],
                "issues_found": r["recon"]["issues_found"],
                "demo_mode": r.get("demo_mode", False),
            }
            for r in _runs.values()
        ]
    }
