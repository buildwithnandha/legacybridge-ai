"""
CDC Analyzer Engine — Tool 3: get_cdc_events

Reads the simulated CDC event log (cdc_events.json) and analyzes
event capture gaps for a given table within a time window.
Detects:
  - Total vs captured vs missed events
  - Gap rate percentage
  - Gap patterns with reasons and counts
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from config import CDC_EVENTS_PATH

logger = logging.getLogger(__name__)

_cdc_data: dict | None = None


def _load_cdc_data() -> dict:
    """Load and cache the CDC events JSON file."""
    global _cdc_data
    if _cdc_data is None:
        with open(CDC_EVENTS_PATH, "r") as f:
            _cdc_data = json.load(f)
    return _cdc_data


def get_cdc_events(table_name: str, hours_back: int = 168) -> dict[str, Any]:
    """Analyze CDC event gaps for a table within the given time window.

    Args:
        table_name: Name of the table to analyze.
        hours_back: How many hours back to look (default 168 = 7 days).

    Returns the structure expected by the RCA agent's get_cdc_events tool.
    """
    logger.info(f"CDC analysis: {table_name} (last {hours_back}h)")

    data = _load_cdc_data()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    # Use the pre-computed gap_analysis if available for the table
    gap_info = data.get("gap_analysis", {}).get(table_name)

    if gap_info:
        result = {
            "table": table_name,
            "total_events": gap_info["total_events"],
            "captured": gap_info["captured"],
            "missed": gap_info["missed"],
            "gap_rate": gap_info["gap_rate"],
            "gap_patterns": gap_info["gap_patterns"],
        }
    else:
        # Fallback: compute from individual events
        table_events = [
            e for e in data.get("events", [])
            if e["table_name"] == table_name
        ]

        # Filter by time window
        filtered = []
        for evt in table_events:
            try:
                evt_ts = datetime.fromisoformat(
                    evt["timestamp"].replace("Z", "+00:00")
                )
                if evt_ts >= cutoff:
                    filtered.append(evt)
            except (ValueError, KeyError):
                filtered.append(evt)  # include if timestamp unparseable

        total = len(filtered)
        captured = sum(1 for e in filtered if e.get("captured", True))
        missed = total - captured

        # Aggregate gap patterns
        patterns: dict[str, int] = {}
        for e in filtered:
            if not e.get("captured", True):
                reason = e.get("gap_reason", "UNKNOWN")
                patterns[reason] = patterns.get(reason, 0) + 1

        gap_patterns = [
            {"reason": reason, "count": count}
            for reason, count in sorted(patterns.items(), key=lambda x: -x[1])
        ]

        gap_rate = round((missed / total * 100), 2) if total > 0 else 0.0

        result = {
            "table": table_name,
            "total_events": total,
            "captured": captured,
            "missed": missed,
            "gap_rate": gap_rate,
            "gap_patterns": gap_patterns,
        }

    logger.info(
        f"  {table_name}: {result['total_events']} total, "
        f"{result['captured']} captured, {result['missed']} missed "
        f"({result['gap_rate']}% gap rate)"
    )

    return result
