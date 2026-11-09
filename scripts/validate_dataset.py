"""Independently reconcile the generated files without importing the generator or SQL."""

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def validate(folder: Path):
    """Reconcile prepared source files independently and return expected business totals."""
    # Validate transport completeness first: every listed file must retain its
    # published bytes and record count before business assertions can be trusted.
    manifest = json.loads((folder / "manifest.json").read_text())
    records, envelope_counts = {}, Counter()
    for batch in manifest["batches"]:
        body = (folder / batch["file"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == batch["sha256"], batch["file"]
        lines = body.splitlines()
        assert len(lines) == batch["records"]
        for line in lines:
            envelope = json.loads(line)
            envelope_counts[batch["scenario"]] += 1
            row = json.loads(envelope["payload"])
            key = (envelope["entity_type"], envelope["record_id"])
            # Transport duplication may repeat a record, but it cannot alter the revision.
            assert key not in records or records[key] == row, key
            records[key] = row
    # Build independent lookup maps after removing exact retries. Business checks
    # intentionally do not reuse the generator or transformation implementation.
    entities = defaultdict(dict)
    for (entity, identifier), row in records.items():
        entities[entity][identifier] = row
    # Deliberate invalid records belong to the quarantine scenario, not the
    # expected order population. They are checked separately before returning.
    valid_orders = {
        key: row for key, row in entities["orders"].items() if not key.startswith("INVALID-")
    }
    assert len(valid_orders) == manifest["days"] * manifest["orders_per_day"]
    items, stages = defaultdict(list), defaultdict(dict)
    # A line must reference an existing order/product and reconcile on its own.
    # Group those checked lines by order for the header-to-basket checks below.
    for row in entities["order_items"].values():
        assert row["order_id"] in valid_orders and row["product_id"] in entities["products"]
        assert (
            row["quantity"] > 0
            and row["line_total_paise"] == row["quantity"] * row["unit_price_paise"]
        )
        items[row["order_id"]].append(row)
    # After transport deduplication, an order may have only one event per stage.
    # Index stage timestamps to check ordering and independently derive outcomes.
    for row in entities["events"].values():
        assert row["order_id"] in valid_orders
        assert row["event_type"] not in stages[row["order_id"]]
        stages[row["order_id"]][row["event_type"]] = row["event_time"]
    totals = Counter()
    business_dates = Counter()
    for key, order in valid_orders.items():
        # Reconcile the header against all basket lines, including fees/discounts.
        # This catches missing items even when each surviving line is individually valid.
        assert order["store_id"] in entities["stores"]
        assert sum(row["quantity"] for row in items[key]) == order["item_count"]
        assert sum(row["line_total_paise"] for row in items[key]) == order["subtotal_paise"]
        assert (
            order["total_paise"]
            == order["subtotal_paise"] + order["delivery_fee_paise"] - order["discount_paise"]
        )
        assert order["total_paise"] >= 0 and order["promised_at"] > order["created_at"]
        # Terminal states are exclusive. Missing future stages are valid for a
        # pending order, but the stages already present must remain chronological.
        lifecycle = stages[key]
        assert not ("delivered" in lifecycle and "cancelled" in lifecycle)
        sequence = [order["created_at"]] + [
            lifecycle[stage]
            for stage in (
                "accepted",
                "picking_started",
                "packed",
                "dispatched",
                "delivered",
                "cancelled",
            )
            if stage in lifecycle
        ]
        assert sequence == sorted(sequence), key
        # The serialized source timestamp is UTC. Assign the order to its Indian
        # business date so a late UTC evening does not land on the wrong report day.
        day = (
            datetime.fromisoformat(order["created_at"])
            .replace(tzinfo=timezone.utc)
            .astimezone(ZoneInfo("Asia/Kolkata"))
            .date()
        )
        business_dates[str(day)] += 1
        # Use the event's business timestamp for on-time performance. Transport
        # delay must not make an actually on-time delivery appear late.
        if "delivered" in lifecycle:
            totals["delivered_orders"] += 1
            totals["delivered_order_value_paise"] += order["total_paise"]
            totals[
                "on_time_orders"
                if lifecycle["delivered"] <= order["promised_at"]
                else "late_orders"
            ] += 1
        elif "cancelled" in lifecycle:
            totals["cancelled_orders"] += 1
        else:
            totals["pending_orders"] += 1
    # Count deliberately corrupt source orders separately from the valid business totals.
    invalid = [row for key, row in entities["orders"].items() if key.startswith("INVALID-")]
    assert all(row["total_paise"] < 0 for row in invalid)
    # CodeBuild compares this deterministic summary with the committed expectation;
    # checksums alone would not detect a coherently replaced but incorrect dataset.
    return {
        "valid_orders": len(valid_orders),
        "invalid_order_records": len(invalid),
        "stores": len(entities["stores"]),
        "products": len(entities["products"]),
        "envelopes": dict(envelope_counts),
        "orders_by_business_date": dict(sorted(business_dates.items())),
        "after_all_events": dict(totals),
    }


if __name__ == "__main__":
    import argparse

    # Default to the committed files; tests or maintenance can select another folder.
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", nargs="?", type=Path, default=Path("data"))
    arguments = parser.parse_args()
    print(json.dumps(validate(arguments.folder), indent=2))
