"""Execute the actual transformation SQL locally against independently checked data."""

import json
from datetime import date, datetime

import duckdb
import pytest

from freshroute.data import generate, write_dataset
from freshroute.model import GOLD_SCHEMA, SCHEMAS
from freshroute.pipeline import day_range
from freshroute.sql import (
    candidate_metrics,
    conflicting_revisions,
    integrity_checks,
    merge_entity,
    typed_source,
    valid_record,
)


@pytest.fixture
def database():
    # Use an in-memory engine with the production column contracts; no AWS clients
    # are created, and closing the fixture removes all test tables.
    connection = duckdb.connect()
    # These macros bridge names only; the MERGE, validation and aggregation SQL below
    # is the same generated SQL submitted to Athena by the application.
    connection.execute("SET TimeZone='UTC'")
    connection.execute("CREATE MACRO json_parse(v) AS CAST(v AS JSON)")
    connection.execute("CREATE MACRO json_extract_scalar(v,p) AS json_extract_string(v,p)")
    connection.execute("CREATE MACRO with_timezone(v,z) AS timezone(z,v)")
    connection.execute("CREATE MACRO at_timezone(v,z) AS timezone(z,v)")
    connection.execute(
        "CREATE TABLE raw(entity_type VARCHAR,record_id VARCHAR,"
        'ingested_at VARCHAR,payload VARCHAR,"$path" VARCHAR)'
    )
    connection.execute("CREATE SCHEMA silver")
    for entity, fields in SCHEMAS.items():
        columns = ",".join(
            f"{name} {'VARCHAR' if dtype == 'string' else dtype}" for name, dtype in fields
        )
        connection.execute(f"CREATE TABLE silver.{entity}({columns})")
    yield connection
    connection.close()


def ingest(connection, rows):
    # Replace the current input batch while retaining Silver state. This models
    # a new ingestion/replay arriving after an earlier successful merge.
    """Load the transport envelope, then apply the production silver contracts."""
    connection.execute("DELETE FROM raw")
    connection.executemany(
        "INSERT INTO raw VALUES (?,?,?,?,?)",
        [
            (
                row["entity_type"],
                row["record_id"],
                row["ingested_at"],
                row["payload"],
                "s3://source/input",
            )
            for row in rows
        ],
    )
    for entity in SCHEMAS:
        source = typed_source(entity, "raw", ["s3://source/input"])
        connection.execute(merge_entity(entity, source, f"silver.{entity}"))


def metrics(connection, dates):
    # Execute the actual reporting SQL with fixed publication metadata so repeated
    # results can be compared without wall-clock timestamps changing between calls.
    query = candidate_metrics("silver", dates, "test-release", "2026-10-24 00:00:00")
    return connection.execute(query).fetchall()


@pytest.mark.parametrize("as_of", ["2026-10-25 10:38:09", "2026-10-25 10:38:09.123"])
def test_candidate_publication_timestamp_has_millisecond_precision(database, as_of):
    # A reference store produces a zero-order report without generating source
    # data. Materialize the actual query so the persisted column type is checked.
    database.execute(
        "INSERT INTO silver.stores (store_id, store_name, city) "
        "VALUES ('BLR-IND', 'FreshRoute Indiranagar', 'Bengaluru')"
    )
    query = candidate_metrics("silver", ["2026-10-01"], "test-release", as_of)
    database.execute(f"CREATE TABLE candidate AS {query}")
    actual_type, actual_value = database.execute(
        "SELECT typeof(published_at), published_at FROM candidate"
    ).fetchone()

    # DuckDB names millisecond timestamps TIMESTAMP_MS. This checks the output
    # contract, not just SQL text; Athena itself still needs deployed verification.
    assert actual_type == "TIMESTAMP_MS"
    assert actual_value == datetime.fromisoformat(as_of)


def test_generated_orders_reconcile_and_follow_possible_lifecycles():
    # Reconstruct business records from envelopes independently of the generator
    # logic, then reconcile basket totals and valid lifecycle ordering.
    base, late = generate(date(2026, 10, 1), 3, 150)
    parsed = {}
    for envelope in base + late:
        parsed[(envelope["entity_type"], envelope["record_id"])] = json.loads(envelope["payload"])
    orders = {
        key: row
        for (entity, key), row in parsed.items()
        if entity == "orders" and not key.startswith("INVALID-")
    }
    items = [row for (entity, _), row in parsed.items() if entity == "order_items"]
    events = [row for (entity, _), row in parsed.items() if entity == "events"]
    assert len(orders) == 450
    for identifier, order in orders.items():
        basket = [item for item in items if item["order_id"] == identifier]
        assert sum(item["line_total_paise"] for item in basket) == order["subtotal_paise"]
        assert sum(item["quantity"] for item in basket) == order["item_count"]
        assert (
            order["total_paise"]
            == order["subtotal_paise"] + order["delivery_fee_paise"] - order["discount_paise"]
        )
        stages = {event["event_type"]: event for event in events if event["order_id"] == identifier}
        assert not ("cancelled" in stages and "delivered" in stages)
        if "delivered" in stages:
            sequence = [order["created_at"]] + [
                stages[kind]["event_time"]
                for kind in ("accepted", "picking_started", "packed", "dispatched", "delivered")
            ]
            assert sequence == sorted(sequence)
    assert late and all(json.loads(row["payload"])["event_type"] == "delivered" for row in late)


def test_replay_is_idempotent_and_late_events_correct_history(database):
    # First establish an initial report, replay the same input, and then deliver
    # the held-back events. Counts may change only when new business events arrive.
    base, late = generate(date(2026, 10, 1), 2, 80)
    ingest(database, base)
    initial = metrics(database, ["2026-10-01", "2026-10-02"])
    ingest(database, base)
    assert metrics(database, ["2026-10-01", "2026-10-02"]) == initial
    ingest(database, late)
    corrected = metrics(database, ["2026-10-01", "2026-10-02"])
    names = [name for name, _ in GOLD_SCHEMA]
    initial_rows = [dict(zip(names, row, strict=True)) for row in initial]
    corrected_rows = [dict(zip(names, row, strict=True)) for row in corrected]
    assert sum(row["order_count"] for row in corrected_rows) == 160
    expected_late = len({row["record_id"] for row in late})
    assert (
        sum(row["delivered_count"] for row in corrected_rows)
        - sum(row["delivered_count"] for row in initial_rows)
        == expected_late
    )
    assert (
        sum(row["pending_count"] for row in initial_rows)
        - sum(row["pending_count"] for row in corrected_rows)
        == expected_late
    )
    for name, query in integrity_checks("silver").items():
        assert database.execute(query).fetchone()[0] == 0, name
    # Reprocessing historical records cannot undo newer state or inflate totals.
    ingest(database, base + late)
    assert metrics(database, ["2026-10-01", "2026-10-02"]) == corrected


def test_newer_revision_wins_even_when_old_event_arrives_later(database):
    # Accept a correction at revision 2, then replay revision 1. Arrival order
    # must never replace a newer business revision with stale data.
    base, late = generate(date(2026, 10, 1), 1, 30)
    ingest(database, base + late)
    original = next(
        row
        for row in base + late
        if row["entity_type"] == "events"
        and json.loads(row["payload"])["event_type"] == "delivered"
    )
    changed = json.loads(original["payload"])
    changed.update(
        source_version=2, updated_at="2026-10-05 12:00:00", event_time="2026-10-03 12:00:00"
    )
    replacement = dict(original, payload=json.dumps(changed))
    ingest(database, [replacement])
    ingest(database, [original])
    result = database.execute(
        "SELECT source_version,event_time FROM silver.events WHERE event_id=?",
        [changed["event_id"]],
    ).fetchone()
    assert result == (2, datetime(2026, 10, 3, 12, 0))


def test_invalid_money_is_rejected_without_dropping_valid_orders(database):
    # Keep the deliberately invalid order in raw input so the rejection predicate
    # can count it, while the merge still retains all valid neighboring orders.
    base, _ = generate(date(2026, 10, 1), 1, 200)
    ingest(database, base)
    source = typed_source("orders", "raw", ["s3://source/input"])
    invalid = database.execute(
        f"SELECT count(*) FROM ({source}) WHERE NOT coalesce(({valid_record('orders')}),false)"
    ).fetchone()[0]
    assert invalid == 2
    assert database.execute("SELECT count(*) FROM silver.orders").fetchone()[0] == 200


def test_reusing_a_revision_number_cannot_silently_change_accepted_state(database):
    # A valid schema is insufficient when the same key/revision changes content.
    # The conflict check must detect the disagreement before another merge.
    base, _ = generate(date(2026, 10, 1), 1, 5)
    ingest(database, base)
    source = typed_source("orders", "raw", ["s3://source/input"])
    query = conflicting_revisions("orders", source, "silver.orders")
    assert database.execute(query).fetchone()[0] == 0
    # The incoming JSON remains valid; only its already accepted revision disagrees.
    database.execute("UPDATE silver.orders SET customer_key='changed-without-a-new-version'")
    assert database.execute(query).fetchone()[0] == 5


def test_quality_checks_detect_orphans_and_basket_corruption(database):
    # Start from valid Silver data, then inject independent monetary and reference
    # defects to prove that both quality gates identify the affected order/event.
    base, late = generate(date(2026, 10, 1), 1, 10)
    ingest(database, base + late)
    database.execute(
        "UPDATE silver.order_items SET line_total_paise=line_total_paise+100 "
        "WHERE order_item_id=(SELECT min(order_item_id) FROM silver.order_items)"
    )
    assert database.execute(integrity_checks("silver")["basket_reconciliation"]).fetchone()[0] == 1
    database.execute(
        "UPDATE silver.events SET order_id='missing-order' "
        "WHERE event_id=(SELECT min(event_id) FROM silver.events)"
    )
    assert database.execute(integrity_checks("silver")["orphan_event"]).fetchone()[0] == 1


def test_indian_business_date_and_on_time_definition(database):
    # Use one controlled order so timezone conversion and the on-time denominator
    # can be asserted without other generated deliveries influencing the result.
    base, late = generate(date(2026, 10, 1), 1, 1)
    ingest(database, base + late)
    # 20:00 UTC belongs to the following Indian business day. A 10-minute delivery
    # is on time even if its transport arrival was delayed until much later.
    database.execute(
        "UPDATE silver.orders SET created_at=TIMESTAMP '2026-10-01 20:00:00', "
        "promised_at=TIMESTAMP '2026-10-01 20:15:00'"
    )
    database.execute("DELETE FROM silver.events")
    order_id = database.execute("SELECT order_id FROM silver.orders").fetchone()[0]
    database.execute(
        "INSERT INTO silver.events VALUES "
        "('delivery',?,'delivered','2026-10-01 20:10:00',1,'2026-10-01 20:10:00')",
        [order_id],
    )
    rows = [
        dict(zip([name for name, _ in GOLD_SCHEMA], row, strict=True))
        for row in metrics(database, ["2026-10-01", "2026-10-02"])
    ]
    assert sum(row["order_count"] for row in rows if str(row["business_date"]) == "2026-10-01") == 0
    actual = next(row for row in rows if row["order_count"] == 1)
    assert str(actual["business_date"]) == "2026-10-02"
    assert actual["on_time_count"] == 1 and actual["on_time_pct"] == 100


def test_dataset_files_are_reproducible(tmp_path):
    # Matching manifests prove that paths, record counts, and file-byte checksums
    # remain stable for the same seed and generation period.
    first = write_dataset(tmp_path / "one", date(2026, 10, 1), 1, 5)
    second = write_dataset(tmp_path / "two", date(2026, 10, 1), 1, 5)
    assert first == second


def test_backfill_dates_are_inclusive_and_bounded():
    # Check both ends of a normal range, then reject reversed and excessive
    # requests before a caller can submit historical processing to Airflow.
    assert day_range("2026-10-20", "2026-10-22") == ["2026-10-20", "2026-10-21", "2026-10-22"]
    with pytest.raises(ValueError):
        day_range("2026-10-22", "2026-10-20")
    with pytest.raises(ValueError):
        day_range("2024-01-01", "2026-01-01")
