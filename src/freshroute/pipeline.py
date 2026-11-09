"""Airflow task implementations; Athena executes data work and S3 records run state."""

import hashlib
import json
import uuid
from datetime import date, datetime, timedelta, timezone

from freshroute.aws import Athena, ObjectStore, Settings, client
from freshroute.logging import log
from freshroute.model import GOLD_SCHEMA, SCHEMAS
from freshroute.sql import (
    candidate_metrics,
    conflicting_revisions,
    integrity_checks,
    literal,
    merge_entity,
    typed_source,
    valid_record,
)


def day_range(start: str, end: str) -> list[str]:
    """Use inclusive business dates; reject accidental unbounded history requests."""
    # Parse before constructing SQL dates; invalid ISO dates fail immediately.
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first or (last - first).days > 365:
        raise ValueError("Backfill range must be ordered and no longer than 366 days")
    # Include both endpoints, including a one-day range where start equals end.
    return [(first + timedelta(days=index)).isoformat() for index in range((last - first).days + 1)]


class Pipeline:
    """Coordinate one run through S3 state while Athena handles the actual rows."""

    def __init__(self, settings: Settings, run_id: str):
        self.settings = settings
        self.run_id = run_id
        # Airflow run IDs contain punctuation; a digest is safe in Glue and S3 identifiers.
        self.token = hashlib.sha256(run_id.encode()).hexdigest()[:24]
        self.store = ObjectStore(settings)
        self.athena = Athena(settings)
        # Persist stage outputs outside the worker so task retries and separate
        # Airflow processes can resume the same run without passing data in XCom.
        self.state_key = f"control/runs/{self.token}/state.json"
        self.silver = f"{settings.database}_silver"
        self.gold = f"{settings.database}_gold"
        self.control = f"{settings.database}_control"

    def state(self):
        """Load required run state; downstream stages cannot invent a missing plan."""
        state = self.store.read(self.state_key)
        if state is None:
            raise RuntimeError("Run planning must succeed before downstream tasks execute")
        return state

    def save(self, state):
        """Replace this run's state after a stage has completed its durable work."""
        self.store.write(self.state_key, state)

    def source(self, entity):
        # A table-backed manifest avoids Athena's SQL-length limit for long backfill ranges.
        inputs = (
            f"SELECT source_path FROM {self.control}.input_manifest "
            f"WHERE run_token={literal(self.token)}"
        )
        return typed_source(entity, f"{self.settings.database}_raw.records", inputs)

    def plan(self, conf: dict | None = None) -> bool:
        """Freeze source files and affected dates before any mutable silver tables change."""
        conf = conf or {}
        previous = self.store.read(self.state_key)
        if previous:
            # Retrying the planning task must not silently change its input snapshot.
            return bool(previous["objects"])
        # Scheduled runs have no mode override. Manual backfills specify a date
        # range but still enter the same planning and publication sequence.
        mode = conf.get("mode", "incremental")
        if mode not in ("incremental", "backfill"):
            raise ValueError("mode must be incremental or backfill")
        # The checkpoint tracks object identity, not event time: a delivery event
        # from yesterday is new input when its source object arrives today.
        checkpoint = self.store.read("control/checkpoint.json", {})
        available = self.store.bronze_objects()
        # Missing or replaced historical inputs would make replay inconsistent.
        # Fail before Silver changes rather than accepting a silently altered history.
        if set(checkpoint) - {obj["key"] for obj in available}:
            raise RuntimeError(
                "Previously processed bronze objects are missing; restore them before processing"
            )
        for obj in available:
            old = checkpoint.get(obj["key"])
            if old and old != obj["etag"]:
                raise RuntimeError(f"Immutable bronze object was overwritten: {obj['key']}")
        # Backfills examine all retained inputs. Incremental runs merge only files
        # whose keys have not yet been committed to the shared checkpoint.
        selected = (
            available
            if mode == "backfill"
            else [obj for obj in available if obj["key"] not in checkpoint]
        )
        state = {
            "run_id": self.run_id,
            "mode": mode,
            "objects": selected,
            "release": self.settings.release,
            "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "scratch_tables": [],
        }
        # All tasks use the same manifest. Later arrivals wait for a subsequent scheduled run.
        body = "".join(
            json.dumps(
                {
                    "run_token": self.token,
                    "source_path": f"s3://{self.settings.bucket}/{obj['key']}",
                }
            )
            + "\n"
            for obj in selected
        )
        self.store.s3.put_object(
            Bucket=self.settings.bucket,
            Key=f"control/manifests/{self.token}.jsonl",
            Body=body.encode(),
            ServerSideEncryption="AES256",
        )
        # Returning False lets the DAG short-circuit the data stages. The empty
        # plan is still saved so cleanup and retries can inspect what happened.
        if not selected:
            state["dates"] = []
            self.save(state)
            log("pipeline.no_new_inputs", run_id=self.run_id)
            return False
        if mode == "backfill":
            state["dates"] = day_range(conf["start_date"], conf["end_date"])
        else:
            # Capture both old dates and corrected dates. A later order correction can move
            # an order to another date/store; both aggregates then need reconstruction.
            ids = " UNION ".join(
                f"SELECT order_id FROM ({self.source(entity)}) WHERE {valid_record(entity)}"
                for entity in ("orders", "events", "order_items")
            )
            orders = self.source("orders")
            stores = self.source("stores")
            date_sql = f"""
                SELECT DISTINCT
                    CAST(
                        at_timezone(with_timezone(created_at, 'UTC'), 'Asia/Kolkata') AS date
                    ) business_date
                FROM (
                    SELECT created_at
                    FROM {self.silver}.orders
                    WHERE order_id IN ({ids})
                        OR store_id IN (
                            SELECT store_id FROM ({stores}) WHERE {valid_record("stores")}
                        )
                    UNION ALL
                    SELECT created_at FROM ({orders}) WHERE {valid_record("orders")}
                )
            """
            state["dates"] = sorted(
                row["business_date"] for row in self.athena.rows(date_sql, "affected_dates")
            )
        self.save(state)
        log(
            "pipeline.planned",
            run_id=self.run_id,
            mode=mode,
            files=len(selected),
            dates=state["dates"],
        )
        return True

    def merge(self):
        """Quarantine invalid records, reject ambiguous revisions, then upsert silver."""
        state = self.state()
        # Verify the planning snapshot before reading it for conflict checks or
        # merges. Later-arriving files are intentionally absent from this run.
        self.store.verify_snapshot(state["objects"])
        rejected = 0
        for entity in SCHEMAS:
            source = self.source(entity)
            condition = valid_record(entity)
            # A source version is an immutable business revision. Different payloads at
            # the same key/version indicate a producer error, not a deduplication choice.
            conflicts = self.athena.scalar(
                conflicting_revisions(entity, source, f"{self.silver}.{entity}"),
                f"{entity}.conflicts",
            )
            if conflicts:
                raise ValueError(f"{entity}: {conflicts} conflicting source revisions")
            invalid = self.athena.scalar(
                f"SELECT count(*) FROM ({source}) WHERE NOT coalesce(({condition}),false)",
                f"{entity}.invalid",
            )
            if invalid:
                # Each retry has its own export prefix because Athena UNLOAD requires an empty
                # destination.
                destination = f"s3://{self.settings.bucket}/quarantine/{self.token}/{entity}/{uuid.uuid4().hex}/"
                self.athena.execute(
                    f"""
                        UNLOAD (
                            SELECT record_id, payload, source_path,
                                'schema_or_domain_violation' AS reason
                            FROM ({source})
                            WHERE NOT coalesce(({condition}), false)
                        ) TO {literal(destination)}
                        WITH (format='JSON', compression='GZIP')
                    """,
                    f"{entity}.quarantine",
                )
            # Exporting invalid rows does not discard valid neighbors. The merge
            # applies the same acceptance predicate and inserts only valid revisions.
            rejected += invalid
            self.athena.execute(
                merge_entity(entity, source, f"{self.silver}.{entity}"), f"{entity}.merge"
            )
        # Unknown envelope types must fail visibly instead of disappearing from the pipeline.
        known = ",".join(literal(entity) for entity in SCHEMAS)
        selected = (
            f"SELECT source_path FROM {self.control}.input_manifest "
            f"WHERE run_token={literal(self.token)}"
        )
        unknown = self.athena.scalar(
            f"SELECT count(*) FROM {self.settings.database}_raw.records "
            f'WHERE "$path" IN ({selected}) '
            f"AND (entity_type IS NULL OR entity_type NOT IN ({known}))",
            "unknown_envelope",
        )
        if unknown:
            raise ValueError(
                f"Found {unknown} unknown or malformed envelopes; inspect the raw manifest"
            )
        state["rejected_records"] = rejected
        self.save(state)
        log("pipeline.silver_updated", run_id=self.run_id, rejected_records=rejected)

    def build(self):
        """Materialize candidate results in a private area before analyst publication."""
        state = self.state()
        if not state["dates"]:
            # A reference-only update can change Silver without creating report
            # dates. Validation and checkpoint handling must still run afterward.
            log("pipeline.reference_only", run_id=self.run_id)
            return
        # Give each retry a fresh CTAS table, since a previous attempt may have
        # created a table before the worker could persist its result in run state.
        suffix = uuid.uuid4().hex[:12]
        table = f"{self.control}.candidate_{self.token}_{suffix}"
        query = candidate_metrics(self.silver, state["dates"], state["release"], state["as_of"])
        # The enforced workgroup chooses a unique location under its private results
        # prefix. Athena forbids external_location when that setting is enforced.
        self.athena.execute(
            f"CREATE TABLE {table} WITH (format='PARQUET') AS {query}", "candidate.build"
        )
        state["candidate"] = table
        state["scratch_tables"].append(table)
        self.save(state)

    def validate(self):
        """A failed check leaves the last published gold snapshot available to readers."""
        state = self.state()
        # Cross-table checks run even without candidate dates: reference-only
        # changes must not leave orphaned orders, invalid baskets, or broken lifecycles.
        for name, query in integrity_checks(self.silver).items():
            failures = self.athena.scalar(query, f"quality.{name}")
            log("quality.checked", check=name, failures=failures, run_id=self.run_id)
            if failures:
                raise ValueError(f"Quality check {name} failed for {failures} records")
        if state.get("candidate"):
            candidate = state["candidate"]
            # Compare aggregate identities separately from row-level constraints;
            # valid source rows do not by themselves prove that metrics reconcile.
            invalid = self.athena.scalar(
                f"""
                    SELECT count(*)
                    FROM {candidate}
                    WHERE order_count <> delivered_count + cancelled_count + pending_count
                        OR delivered_count <> on_time_count + late_count
                        OR on_time_pct < 0 OR on_time_pct > 100
                """,
                "quality.metric_balance",
            )
            if invalid:
                raise ValueError("Candidate metrics do not reconcile")
            # The candidate contains every store/date combination, including zero
            # order rows. A count mismatch indicates a missing or duplicated group.
            expected = self.athena.scalar(
                f"SELECT count(*) FROM {self.silver}.stores", "store_count"
            ) * len(state["dates"])
            actual = self.athena.scalar(f"SELECT count(*) FROM {candidate}", "candidate_count")
            if actual != expected:
                raise ValueError(f"Expected {expected} store/date rows, received {actual}")
        # This flag is the publication gate; no failed check can set it for the run.
        state["validated"] = True
        self.save(state)

    def publish(self):
        """Commit one complete metrics table atomically, then record durable progress."""
        state = self.state()
        if not state.get("validated"):
            raise RuntimeError("Publication requires successful quality validation")
        # Re-check immutable inputs before exposing a snapshot or advancing progress.
        self.store.verify_snapshot(state["objects"])
        if state.get("candidate"):
            # Replace full aggregates on the two-column reporting key. One Iceberg
            # MERGE publishes the table atomically instead of exposing partial dates.
            columns = [name for name, _ in GOLD_SCHEMA]
            assignment = ",".join(
                f"{name}=s.{name}" for name in columns if name not in ("business_date", "store_id")
            )
            self.athena.execute(
                f"""
                    MERGE INTO {self.gold}.store_daily t
                    USING {state["candidate"]} s
                        ON t.business_date = s.business_date AND t.store_id = s.store_id
                    WHEN MATCHED THEN UPDATE SET {assignment}
                    WHEN NOT MATCHED THEN
                        INSERT ({",".join(columns)})
                        VALUES ({",".join("s." + name for name in columns)})
                """,
                "gold.publish",
            )
        if state["mode"] == "incremental":
            # A historical rerun must never advance the normal ingestion checkpoint.
            # If this write fails after MERGE, a retry safely republishes identical aggregates.
            checkpoint = self.store.read("control/checkpoint.json", {})
            checkpoint.update({obj["key"]: obj["etag"] for obj in state["objects"]})
            self.store.write("control/checkpoint.json", checkpoint)
        # Record publication before emitting observability signals. If metrics
        # delivery fails, a task retry can safely repeat the idempotent merge.
        state["published"] = True
        self.save(state)
        client("cloudwatch", self.settings.region).put_metric_data(
            Namespace="FreshRoute",
            MetricData=[
                {
                    "MetricName": "SuccessfulPublication",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Environment", "Value": self.settings.environment}],
                },
                {
                    "MetricName": "RejectedRecords",
                    "Value": state.get("rejected_records", 0),
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Environment", "Value": self.settings.environment}],
                },
            ],
        )
        log("pipeline.published", run_id=self.run_id, mode=state["mode"], dates=state["dates"])

    def cleanup(self):
        """Remove scratch catalog entries; lifecycle rules expire their temporary objects."""
        # Cleanup also follows failed/skipped runs, so a missing plan is harmless.
        # Only known scratch tables are dropped; S3 lifecycle handles their files.
        state = self.store.read(self.state_key, {})
        for table in state.get("scratch_tables", []):
            self.athena.execute(f"DROP TABLE IF EXISTS {table}", "scratch.cleanup")
        log("pipeline.cleanup", run_id=self.run_id, published=state.get("published", False))


def execute_task(stage: str, environment: str, account: str, region: str, release: str, **context):
    """Airflow entry point. One DAG serializes all incremental and historical runs."""
    # Reconstruct the task-local clients from deployed configuration. Airflow
    # supplies run_id and dag_run at execution time, not while importing the DAG.
    settings = Settings(environment, account, region, release)
    pipeline = Pipeline(settings, context["run_id"])
    if stage == "plan":
        # Only planning consumes user-supplied mode/date parameters; later stages
        # use the frozen S3 state so configuration cannot drift between tasks.
        return pipeline.plan(context["dag_run"].conf)
    return getattr(pipeline, stage)()
