"""Shared schema definitions used by infrastructure, SQL and validation."""

# These defaults match the CloudFormation naming convention and CodeBuild region.
PROJECT = "freshroute"
REGION = "us-east-1"
AIRFLOW_VERSION = "2.11.0"
# Only these stages are accepted into Silver; unknown source stages are rejected.
EVENT_TYPES = ("accepted", "picking_started", "packed", "dispatched", "delivered", "cancelled")

# Currency is stored as integer paise. This avoids floating-point reconciliation errors.
# Timestamps are UTC; business_date is derived separately in Asia/Kolkata.
SCHEMAS = {
    # Store metadata supplies reporting labels and fulfillment-capacity assumptions.
    "stores": [
        ("store_id", "string"),
        ("store_name", "string"),
        ("city", "string"),
        ("locality", "string"),
        ("latitude", "double"),
        ("longitude", "double"),
        ("capacity_per_hour", "int"),
        ("source_version", "bigint"),
        ("updated_at", "timestamp"),
    ],
    # Product prices describe the catalog; order items retain their purchase-time price.
    "products": [
        ("product_id", "string"),
        ("product_name", "string"),
        ("category", "string"),
        ("pack_size", "string"),
        ("unit_price_paise", "bigint"),
        ("source_version", "bigint"),
        ("updated_at", "timestamp"),
    ],
    # The order header owns the delivery promise, basket totals, and business date.
    "orders": [
        ("order_id", "string"),
        ("store_id", "string"),
        ("customer_key", "string"),
        ("created_at", "timestamp"),
        ("promised_at", "timestamp"),
        ("distance_km", "double"),
        ("item_count", "int"),
        ("subtotal_paise", "bigint"),
        ("delivery_fee_paise", "bigint"),
        ("discount_paise", "bigint"),
        ("total_paise", "bigint"),
        ("source_version", "bigint"),
        ("updated_at", "timestamp"),
    ],
    # Line-level facts allow quantity and subtotal reconciliation against the header.
    "order_items": [
        ("order_item_id", "string"),
        ("order_id", "string"),
        ("product_id", "string"),
        ("quantity", "int"),
        ("unit_price_paise", "bigint"),
        ("line_total_paise", "bigint"),
        ("source_version", "bigint"),
        ("updated_at", "timestamp"),
    ],
    # Event time records when a stage happened, independently of transport arrival.
    "events": [
        ("event_id", "string"),
        ("order_id", "string"),
        ("event_type", "string"),
        ("event_time", "timestamp"),
        ("source_version", "bigint"),
        ("updated_at", "timestamp"),
    ],
}
# The first field in each contract is its immutable merge key. Column order is
# also used for Iceberg inserts, so changes must match infra/storage.yaml.
KEYS = {name: fields[0][0] for name, fields in SCHEMAS.items()}
# Gold has one row per business_date/store_id. Versions identify the code that
# published a row; all counts are recomputed from Silver rather than incremented.
GOLD_SCHEMA = [
    ("business_date", "date"),
    ("store_id", "string"),
    ("store_name", "string"),
    ("city", "string"),
    ("order_count", "bigint"),
    ("delivered_count", "bigint"),
    ("cancelled_count", "bigint"),
    ("pending_count", "bigint"),
    ("on_time_count", "bigint"),
    ("late_count", "bigint"),
    ("on_time_pct", "double"),
    # Stage averages stay NULL when the relevant event pair is unavailable.
    ("avg_picking_minutes", "double"),
    ("avg_dispatch_minutes", "double"),
    ("avg_travel_minutes", "double"),
    ("delivered_revenue_paise", "bigint"),
    # Publication provenance distinguishes a current deployment from an older result.
    ("metric_version", "string"),
    ("published_at", "timestamp"),
]
