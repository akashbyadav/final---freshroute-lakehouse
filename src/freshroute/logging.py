"""Structured operational logs without customer payloads or credentials."""

import json
import logging
from datetime import datetime, timezone


def log(event: str, **fields) -> None:
    """Emit an event that is equally readable in a terminal and CloudWatch."""
    # Keep payloads out of logs: callers pass counts, identifiers and execution status.
    record = {"timestamp": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
    logging.getLogger("freshroute").info(json.dumps(record, default=str, sort_keys=True))


def configure() -> None:
    """Use one JSON object per line so query tools can parse every record."""
    # Executable scripts call this once at startup. A plain message format avoids
    # wrapping the JSON in a second timestamp/prefix that would hinder parsing.
    logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)
