"""Build Athena SQL for validation, revision-aware Silver merges, and Gold metrics."""

from freshroute.model import EVENT_TYPES, KEYS, SCHEMAS


def literal(value: str) -> str:
    """Return a quoted SQL value; callers supply identifiers from trusted contracts."""
    # SQL escapes a quote inside a string by doubling it. Values such as S3 paths
    # must pass through this function before they are inserted into a query.
    return "'" + str(value).replace("'", "''") + "'"


def typed_source(entity: str, raw_table: str, paths: list[str] | str) -> str:
    """Read one entity from the frozen input files and cast it to its Silver schema."""
    fields = []
    for name, dtype in SCHEMAS[entity]:
        # Glue calls text columns 'string'; Athena expressions use 'varchar'.
        # TRY handles malformed JSON and TRY_CAST handles invalid field values,
        # allowing the quality rules to reject a row without aborting the query.
        sql_type = "varchar" if dtype == "string" else dtype
        fields.append(
            f"try_cast(json_extract_scalar(try(json_parse(payload)), '$.{name}') "
            f"AS {sql_type}) AS {name}"
        )

    # Production passes a manifest subquery so the SQL does not grow with the
    # number of input files. Explicit paths support smaller, isolated SQL checks.
    selected = paths if isinstance(paths, str) else ", ".join(literal(path) for path in paths)
    return f"""
        SELECT
            {", ".join(fields)},
            record_id,
            payload,
            "$path" AS source_path
        FROM {raw_table}
        WHERE entity_type = {literal(entity)}
            AND "$path" IN ({selected})
    """.strip()


def valid_record(entity: str) -> str:
    """Return the shared predicate used to accept Silver rows and identify rejects."""
    # Every declared field is required. The envelope ID must agree with the
    # business key, and revisions start at one so stale updates can be ordered.
    rules = [f"{name} IS NOT NULL" for name, _ in SCHEMAS[entity]]
    rules += [f"trim({KEYS[entity]}) <> ''", f"record_id = {KEYS[entity]}", "source_version > 0"]

    if entity == "orders":
        # Reconcile money in integer paise before any revenue aggregation. A
        # negative amount or an impossible promise is quarantined, not corrected.
        rules += [
            "promised_at > created_at",
            "distance_km > 0",
            "item_count > 0",
            "subtotal_paise >= 0",
            "delivery_fee_paise >= 0",
            "discount_paise >= 0",
            "discount_paise <= subtotal_paise",
            "total_paise >= 0",
            "total_paise = subtotal_paise + delivery_fee_paise - discount_paise",
        ]
    elif entity == "order_items":
        # Basket-level reconciliation runs later; this checks each line first.
        rules += [
            "quantity > 0",
            "unit_price_paise > 0",
            "line_total_paise = quantity * unit_price_paise",
        ]
    elif entity == "events":
        # Reject unknown stages instead of silently excluding them from metrics.
        rules += ["event_type IN (" + ",".join(literal(kind) for kind in EVENT_TYPES) + ")"]
    elif entity == "stores":
        rules += [
            "capacity_per_hour > 0",
            "latitude BETWEEN -90 AND 90",
            "longitude BETWEEN -180 AND 180",
        ]
    elif entity == "products":
        rules += ["unit_price_paise > 0"]
    return " AND ".join(rules)


def conflicting_revisions(entity: str, source: str, target: str) -> str:
    """Count reused revisions whose business fields disagree within input or with Silver."""
    key = KEYS[entity]
    columns = [name for name, _ in SCHEMAS[entity]]
    differences = " OR ".join(f"s.{name} IS DISTINCT FROM t.{name}" for name in columns)

    # Compare typed fields rather than raw JSON: formatting and field ordering
    # do not constitute a business revision. DISTINCT removes exact retries.
    valid = f"SELECT DISTINCT {','.join(columns)} FROM ({source}) WHERE {valid_record(entity)}"
    return f"""
        SELECT count(*)
        FROM (
            -- Two different incoming rows cannot share the same key and revision.
            SELECT {key}, source_version
            FROM ({valid})
            GROUP BY {key}, source_version
            HAVING count(*) > 1

            UNION ALL

            -- An accepted Silver revision is also immutable when it is replayed.
            SELECT s.{key}, s.source_version
            FROM ({valid}) s
            JOIN {target} t
                ON s.{key} = t.{key}
                AND s.source_version = t.source_version
            WHERE {differences}
        )
    """.strip()


def merge_entity(entity: str, source: str, target: str) -> str:
    """Upsert the newest valid source revision without double-counting transport retries."""
    key = KEYS[entity]
    columns = [name for name, _ in SCHEMAS[entity]]

    # MERGE requires at most one source row per key. Revision takes priority over
    # arrival time; the preceding conflict check rejects inconsistent ties.
    dedup = f"""
        SELECT {", ".join(columns)}
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {key}
                ORDER BY source_version DESC, updated_at DESC, payload DESC
            ) AS rank
            FROM ({source})
            WHERE {valid_record(entity)}
        )
        WHERE rank = 1
    """.strip()
    assignments = ", ".join(f"{name} = s.{name}" for name in columns if name != key)
    values = ", ".join("s." + name for name in columns)

    # Equal and older revisions deliberately have no matched action. Replaying
    # files therefore cannot undo a correction that Silver already accepted.
    return f"""
        MERGE INTO {target} t
        USING ({dedup}) s
            ON t.{key} = s.{key}
        WHEN MATCHED AND s.source_version > t.source_version
            THEN UPDATE SET {assignments}
        WHEN NOT MATCHED
            THEN INSERT ({", ".join(columns)}) VALUES ({values})
    """.strip()


def candidate_metrics(database: str, dates: list[str], release: str, as_of: str) -> str:
    """Rebuild complete store/date totals from current Silver state for safe replay."""
    # The caller freezes the affected dates and publication timestamp in run state.
    # Retries use those same values rather than moving the reporting boundary.
    date_rows = ", ".join(f"(DATE {literal(day)})" for day in dates)
    return f"""
        WITH requested_dates(business_date) AS (
            VALUES {date_rows}
        ),
        lifecycle AS (
            -- Pivot lifecycle events into one row per order before joining orders,
            -- so multiple delivery stages cannot multiply order counts or value.
            SELECT
                order_id,
                max(CASE WHEN event_type = 'picking_started' THEN event_time END) picking_at,
                max(CASE WHEN event_type = 'packed' THEN event_time END) packed_at,
                max(CASE WHEN event_type = 'dispatched' THEN event_time END) dispatched_at,
                max(CASE WHEN event_type = 'delivered' THEN event_time END) delivered_at,
                max(CASE WHEN event_type = 'cancelled' THEN event_time END) cancelled_at
            FROM {database}.events
            GROUP BY order_id
        ),
        orders_with_dates AS (
            -- UTC is the storage convention; reporting follows the store's Indian
            -- business date rather than the UTC date or the event's arrival date.
            SELECT *,
                CAST(
                    at_timezone(with_timezone(created_at, 'UTC'), 'Asia/Kolkata') AS date
                ) business_date
            FROM {database}.orders
        ),
        aggregates AS (
            SELECT
                o.business_date,
                o.store_id,
                count(*) order_count,
                count_if(e.delivered_at IS NOT NULL) delivered_count,
                count_if(e.cancelled_at IS NOT NULL) cancelled_count,
                count_if(e.delivered_at IS NULL AND e.cancelled_at IS NULL) pending_count,
                count_if(e.delivered_at <= o.promised_at) on_time_count,
                count_if(e.delivered_at > o.promised_at) late_count,
                avg(date_diff('second', e.picking_at, e.packed_at) / 60.0) avg_picking_minutes,
                avg(date_diff('second', e.packed_at, e.dispatched_at) / 60.0) avg_dispatch_minutes,
                avg(date_diff('second', e.dispatched_at, e.delivered_at) / 60.0) avg_travel_minutes,
                sum(
                    CASE WHEN e.delivered_at IS NOT NULL THEN o.total_paise ELSE 0 END
                ) delivered_revenue_paise
            FROM orders_with_dates o
            LEFT JOIN lifecycle e ON o.order_id = e.order_id
            JOIN requested_dates d ON o.business_date = d.business_date
            GROUP BY o.business_date, o.store_id
        )
        SELECT
            d.business_date,
            s.store_id,
            s.store_name,
            s.city,
            coalesce(a.order_count, 0) order_count,
            coalesce(a.delivered_count, 0) delivered_count,
            coalesce(a.cancelled_count, 0) cancelled_count,
            coalesce(a.pending_count, 0) pending_count,
            coalesce(a.on_time_count, 0) on_time_count,
            coalesce(a.late_count, 0) late_count,
            CAST(100.0 * a.on_time_count / nullif(a.delivered_count, 0) AS double) on_time_pct,
            CAST(a.avg_picking_minutes AS double) avg_picking_minutes,
            CAST(a.avg_dispatch_minutes AS double) avg_dispatch_minutes,
            CAST(a.avg_travel_minutes AS double) avg_travel_minutes,
            coalesce(a.delivered_revenue_paise, 0) delivered_revenue_paise,
            {literal(release)} metric_version,
            -- Saved run times may contain only whole seconds, which Athena infers
            -- as timestamp(0). The candidate Parquet writer requires milliseconds;
            -- cast explicitly so new runs and retries share the same output type.
            CAST(TIMESTAMP {literal(as_of)} AS TIMESTAMP(3)) published_at
        -- Include stores with no orders so recomputing a date can clear an old
        -- nonzero total. A missing delivered denominator remains NULL, not 0%.
        FROM requested_dates d
        CROSS JOIN {database}.stores s
        LEFT JOIN aggregates a
            ON a.store_id = s.store_id
            AND a.business_date = d.business_date
    """.strip()


def integrity_checks(database: str) -> dict[str, str]:
    """Return named violation-count queries; every count must be zero before publish."""
    # Row validation cannot detect relationships broken across otherwise valid
    # records. These checks read the accepted Silver tables together.
    return {
        # Every order must point to a known fulfillment store.
        "unknown_store": f"""
            SELECT count(*)
            FROM {database}.orders o
            LEFT JOIN {database}.stores s ON o.store_id = s.store_id
            WHERE s.store_id IS NULL
        """,
        # Events without an order cannot contribute to a meaningful lifecycle.
        "orphan_event": f"""
            SELECT count(*)
            FROM {database}.events e
            LEFT JOIN {database}.orders o ON e.order_id = o.order_id
            WHERE o.order_id IS NULL
        """,
        # Each basket line needs both its owning order and a known product.
        "orphan_item": f"""
            SELECT count(*)
            FROM {database}.order_items i
            LEFT JOIN {database}.orders o ON i.order_id = o.order_id
            LEFT JOIN {database}.products p ON i.product_id = p.product_id
            WHERE o.order_id IS NULL OR p.product_id IS NULL
        """,
        # Check the entire basket: independently valid lines may still omit an
        # item or disagree with the order's quantity and subtotal.
        "basket_reconciliation": f"""
            SELECT count(*)
            FROM {database}.orders o
            LEFT JOIN (
                SELECT order_id, sum(quantity) qty, sum(line_total_paise) amount
                FROM {database}.order_items
                GROUP BY order_id
            ) i ON o.order_id = i.order_id
            WHERE coalesce(i.qty, 0) <> o.item_count
                OR coalesce(i.amount, 0) <> o.subtotal_paise
        """,
        "event_before_order": f"""
            SELECT count(*)
            FROM {database}.events e
            JOIN {database}.orders o ON e.order_id = o.order_id
            WHERE e.event_time < o.created_at
        """,
        # Delivered and cancelled are mutually exclusive terminal states.
        "conflicting_terminal_state": f"""
            SELECT count(*)
            FROM (
                SELECT order_id
                FROM {database}.events
                GROUP BY order_id
                HAVING count_if(event_type = 'delivered') > 0
                    AND count_if(event_type = 'cancelled') > 0
            )
        """,
        # Different event IDs must not disguise a duplicate business stage.
        "duplicate_lifecycle_stage": f"""
            SELECT count(*)
            FROM (
                SELECT order_id, event_type
                FROM {database}.events
                GROUP BY order_id, event_type
                HAVING count(*) > 1
            )
        """,
        # A stage may be missing while an order is still in progress. Compare
        # observed timestamps; NULL comparisons do not fabricate a violation.
        "negative_duration": f"""
            SELECT count(*)
            FROM (
                SELECT
                    order_id,
                    max(CASE WHEN event_type = 'picking_started' THEN event_time END) picking,
                    max(CASE WHEN event_type = 'packed' THEN event_time END) packed,
                    max(CASE WHEN event_type = 'dispatched' THEN event_time END) dispatched,
                    max(CASE WHEN event_type = 'delivered' THEN event_time END) delivered
                FROM {database}.events
                GROUP BY order_id
            )
            WHERE packed < picking OR dispatched < packed OR delivered < dispatched
        """,
    }
