"""Deterministic grocery operations data with a coherent business lifecycle."""

import hashlib
import json
import random
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from freshroute.logging import log
from freshroute.model import SCHEMAS

# Generate operating hours in local business time, then serialize timestamps in UTC.
IST = ZoneInfo("Asia/Kolkata")
# Each tuple defines a store's stable ID, location, coordinates, and hourly capacity.
STORE_SEEDS = [
    ("BLR-IND", "Indiranagar", "Bengaluru", 12.9784, 77.6408, 90),
    ("BLR-KOR", "Koramangala", "Bengaluru", 12.9352, 77.6245, 75),
    ("BLR-WHF", "Whitefield", "Bengaluru", 12.9698, 77.7500, 65),
    ("HYD-MAD", "Madhapur", "Hyderabad", 17.4483, 78.3915, 80),
    ("HYD-GAC", "Gachibowli", "Hyderabad", 17.4401, 78.3489, 70),
    ("PUN-BAN", "Baner", "Pune", 18.5590, 73.7868, 65),
]
# Catalog prices are integer paise; the pack size explains what one purchased unit means.
PRODUCT_SEEDS = [
    ("Toned milk", "Dairy", "1 L", 6000),
    ("Curd", "Dairy", "400 g", 4200),
    ("Paneer", "Dairy", "200 g", 9500),
    ("Eggs", "Protein", "6 pcs", 5400),
    ("Whole wheat bread", "Bakery", "400 g", 4500),
    ("Bananas", "Fruit", "6 pcs", 4800),
    ("Apples", "Fruit", "1 kg", 18000),
    ("Tomatoes", "Vegetables", "1 kg", 4200),
    ("Onions", "Vegetables", "1 kg", 3500),
    ("Potatoes", "Vegetables", "1 kg", 3200),
    ("Coriander", "Vegetables", "100 g", 1500),
    ("Basmati rice", "Staples", "1 kg", 13500),
    ("Whole wheat flour", "Staples", "5 kg", 24500),
    ("Toor dal", "Staples", "1 kg", 15500),
    ("Sunflower oil", "Staples", "1 L", 14500),
    ("Sugar", "Staples", "1 kg", 4800),
    ("Tea", "Beverages", "250 g", 14500),
    ("Coffee", "Beverages", "100 g", 19000),
    ("Biscuits", "Snacks", "200 g", 3500),
    ("Potato chips", "Snacks", "90 g", 5000),
    ("Dishwash liquid", "Household", "500 ml", 11500),
    ("Laundry detergent", "Household", "1 kg", 21000),
    ("Handwash", "Personal care", "250 ml", 8500),
    ("Toothpaste", "Personal care", "150 g", 9500),
]


def stamp(value: datetime) -> str:
    """Store every source timestamp in one canonical UTC representation."""
    # Athena reads these as timezone-free timestamps under the shared UTC contract.
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def envelope(entity: str, record: dict, arrival: datetime) -> dict:
    """Keep transport metadata separate from the source record's business fields."""
    # Route different business keys into the common record_id field used by Bronze.
    key = {
        "stores": "store_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "order_item_id",
        "events": "event_id",
    }[entity]
    # JSON inside a string gives all entities one stable Firehose/bronze schema.
    return {
        "entity_type": entity,
        "record_id": record[key],
        "ingested_at": stamp(arrival),
        "payload": json.dumps(record, sort_keys=True, separators=(",", ":")),
    }


def generate(start: date, days: int = 7, orders_per_day: int = 1000, seed: int = 20261024):
    """Return base and delayed batches with stable IDs and repeatable failure scenarios."""
    # Bound accidental generation requests before allocating records in memory.
    if days < 1 or orders_per_day < 1 or days > 365:
        raise ValueError("days must be 1..365 and orders_per_day must be positive")
    # A local random generator makes this dataset reproducible without changing
    # random state used by another caller or by the test process.
    rng = random.Random(seed)
    # Stable dimension revisions must not change when generating a different date range.
    origin = datetime(2026, 1, 1, tzinfo=IST)
    base, late = [], []
    stores, products = [], []
    for code, locality, city, lat, lon, capacity in STORE_SEEDS:
        # Localities and coordinates are public geographic context, not customer addresses.
        row = dict(
            store_id=code,
            store_name=f"FreshRoute {locality}",
            city=city,
            locality=locality,
            latitude=lat,
            longitude=lon,
            capacity_per_hour=capacity,
            source_version=1,
            updated_at=stamp(origin),
        )
        stores.append(row)
        base.append(envelope("stores", row, origin))
    for index, (name, category, pack, price) in enumerate(PRODUCT_SEEDS):
        # Prices are illustrative assumptions; the repository does not claim live retail pricing.
        row = dict(
            product_id=f"SKU-{index + 1:04d}",
            product_name=name,
            category=category,
            pack_size=pack,
            unit_price_paise=price,
            source_version=1,
            updated_at=stamp(origin),
        )
        products.append(row)
        base.append(envelope("products", row, origin))
    for day_index in range(days):
        business_day = start + timedelta(days=day_index)
        for sequence in range(orders_per_day):
            # Uneven store demand and evening peaks create operational variation
            # that can be explained by capacity and timing in the final report.
            store = rng.choices(stores, weights=[22, 24, 18, 15, 12, 9])[0]
            hour = rng.choices(
                range(7, 24), weights=[6, 9, 8, 5, 5, 7, 6, 4, 4, 5, 8, 13, 15, 14, 9, 5, 2]
            )[0]
            created = datetime.combine(
                business_day, time(hour, rng.randrange(60), rng.randrange(60)), IST
            )
            # Date, seed, and sequence keep IDs stable across regeneration while
            # separating orders from independently generated scenarios.
            identifier = f"FR-{business_day:%Y%m%d}-{seed}-{sequence:06d}"
            # Morning baskets favour dairy and bakery; evenings favour snacks and vegetables.
            weights = [
                7
                if p["category"] in ({"Dairy", "Bakery"} if hour < 11 else {"Snacks", "Vegetables"})
                else 2
                for p in products
            ]
            # Collapse repeated product selections into one basket line; quantity
            # captures multiple units instead of creating ambiguous duplicate lines.
            chosen = {
                p["product_id"]: p
                for p in rng.choices(products, weights=weights, k=rng.randint(2, 8))
            }
            items = []
            for item_index, product in enumerate(chosen.values()):
                quantity = rng.choices([1, 2, 3], [80, 17, 3])[0]
                items.append(
                    dict(
                        order_item_id=f"{identifier}-{item_index + 1:02d}",
                        order_id=identifier,
                        product_id=product["product_id"],
                        quantity=quantity,
                        unit_price_paise=product["unit_price_paise"],
                        line_total_paise=quantity * product["unit_price_paise"],
                        source_version=1,
                        updated_at=stamp(created),
                    )
                )
            # Derive the header from its actual lines. Delivery becomes free at
            # INR 499; eligible promotions are capped at INR 75 and applied in paise.
            subtotal = sum(item["line_total_paise"] for item in items)
            fee = 0 if subtotal >= 49900 else 2500
            discount = min(subtotal // 10, 7500) if rng.random() < 0.18 else 0
            # Most trips are short, with a smaller tail of longer distances. The
            # promise changes at 2 km and is measured against actual delivery time.
            distance = round(rng.triangular(0.3, 4.8, 1.5), 2)
            promise_minutes = 15 if distance < 2 else 25
            order = dict(
                order_id=identifier,
                store_id=store["store_id"],
                customer_key=hashlib.sha256(f"customer-{rng.randrange(2000)}".encode()).hexdigest()[
                    :16
                ],
                created_at=stamp(created),
                promised_at=stamp(created + timedelta(minutes=promise_minutes)),
                distance_km=distance,
                item_count=sum(i["quantity"] for i in items),
                subtotal_paise=subtotal,
                delivery_fee_paise=fee,
                discount_paise=discount,
                total_paise=subtotal + fee - discount,
                source_version=1,
                updated_at=stamp(created),
            )
            # Model separate order/header and basket messages arriving a few seconds
            # apart. Both retain their original business creation timestamp.
            base.append(envelope("orders", order, created + timedelta(seconds=2)))
            base.extend(
                envelope("order_items", item, created + timedelta(seconds=3)) for item in items
            )
            # Congestion is correlated with demand and store capacity, rather than independent
            # noise.
            peak = hour in (18, 19, 20, 21)
            queue = rng.uniform(0.2, 2) + (3 if peak and store["capacity_per_hour"] < 80 else 0)
            picking = 0.6 * order["item_count"] + rng.uniform(1, 3)
            dispatch = rng.uniform(0.5, 2.5) + (2 if peak else 0)
            travel = distance / (12 if peak else 18) * 60 + rng.uniform(1, 3)
            accepted = created + timedelta(seconds=rng.randrange(5, 25))
            picking_at = accepted + timedelta(minutes=queue)
            packed_at = picking_at + timedelta(minutes=picking)
            dispatched_at = packed_at + timedelta(minutes=dispatch)
            delivered_at = dispatched_at + timedelta(minutes=travel)
            lifecycle = [
                ("accepted", accepted),
                ("picking_started", picking_at),
                ("packed", packed_at),
                ("dispatched", dispatched_at),
                ("delivered", delivered_at),
            ]
            if rng.random() < 0.035:
                # Cancellation terminates the lifecycle; no impossible post-cancellation delivery is
                # emitted.
                lifecycle = [("accepted", accepted), ("cancelled", accepted + timedelta(minutes=2))]
            for event_type, happened in lifecycle:
                event = dict(
                    event_id=f"{identifier}-{event_type}",
                    order_id=identifier,
                    event_type=event_type,
                    event_time=stamp(happened),
                    source_version=1,
                    updated_at=stamp(happened),
                )
                # Delay transport by a day for selected delivery confirmations.
                # event_time remains unchanged so backfill corrects the original day.
                is_late = event_type == "delivered" and sequence % 29 == 0
                arrival = happened + timedelta(minutes=24 * 60 if is_late else rng.uniform(0.05, 2))
                target = late if is_late else base
                target.append(envelope("events", event, arrival))
                if sequence % 41 == 0:
                    # Re-delivery changes arrival metadata, not event ID or payload;
                    # downstream deduplication must retain one business event.
                    target.append(envelope("events", event, arrival + timedelta(seconds=20)))
            if sequence % 173 == 0:
                # A domain-invalid record is intentional and separately identified for quarantine
                # checks.
                invalid = dict(order, order_id=f"INVALID-{identifier}", total_paise=-100)
                base.append(envelope("orders", invalid, created))
    # Stable transport ordering gives regenerated files identical bytes/checksums.
    base.sort(key=lambda record: (record["ingested_at"], record["record_id"]))
    late.sort(key=lambda record: (record["ingested_at"], record["record_id"]))
    return base, late


def write_dataset(
    destination: Path, start: date, days=7, orders_per_day=1000, seed=20261024
) -> dict:
    """Write ready-to-upload JSON Lines files grouped by scenario and source entity."""
    # Keep generation parameters beside the file checksums so the prepared inputs
    # can be independently checked and regenerated with the same assumptions.
    base, late = generate(start, days, orders_per_day, seed)
    manifest = {
        "provenance": "Generated operational data; no BigBasket proprietary data",
        "seed": seed,
        "start": start.isoformat(),
        "days": days,
        "orders_per_day": orders_per_day,
        "batches": [],
    }
    destination.mkdir(parents=True, exist_ok=True)
    for scenario, rows in (("base", base), ("late", late)):
        # Keep each source entity readable on its own while preserving the transport
        # envelope consumed by Athena. Late deliveries remain separate for replay.
        entities = SCHEMAS if scenario == "base" else ("events",)
        grouped = {entity: [] for entity in entities}
        for row in rows:
            grouped[row["entity_type"]].append(row)
        (destination / scenario).mkdir(parents=True, exist_ok=True)
        for entity, records in grouped.items():
            content = "".join(
                json.dumps(row, separators=(",", ":")) + "\n" for row in records
            ).encode()
            filename = f"{scenario}/{entity}.jsonl"
            # Hash the exact bytes that will be uploaded; no extraction or format
            # conversion is required between the repository and the Bronze layer.
            (destination / filename).write_bytes(content)
            manifest["batches"].append(
                {
                    "file": filename,
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "records": len(records),
                    "scenario": scenario,
                }
            )
    # Publish the manifest after all files exist. Uploaders trust only listed
    # files and verify these checksums before sending bytes to AWS.
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    log(
        "dataset.generated",
        base_records=len(base),
        late_records=len(late),
        destination=str(destination),
    )
    return manifest
