"""Small AWS adapters with bounded retries, query timeouts and useful identifiers."""

import json
import time
from dataclasses import dataclass

import boto3
from botocore.config import Config

from freshroute.logging import log
from freshroute.model import PROJECT, REGION


def client(service: str, region: str = REGION):
    """Use ambient short-lived credentials, never hard-coded secrets."""
    # CodeBuild and MWAA supply role credentials through boto3's provider chain.
    # Apply the same retry policy to every service for transient throttling/errors.
    return boto3.client(
        service, region_name=region, config=Config(retries={"mode": "standard", "max_attempts": 8})
    )


@dataclass
class Settings:
    """Carry environment identity and derive names shared with CloudFormation."""

    environment: str
    account: str
    region: str = REGION
    release: str = "local"

    def __post_init__(self):
        # Restrict identifiers before they reach SQL, resource names or S3 prefixes.
        if (
            self.environment not in ("dev", "prod")
            or not self.account.isdigit()
            or len(self.account) != 12
        ):
            raise ValueError("Expected dev/prod and a 12-digit AWS account ID")

    @property
    def prefix(self):
        # Stack, role, workgroup, and stream names all share this environment prefix.
        return f"{PROJECT}-{self.environment}"

    @property
    def bucket(self):
        # Account and region make the S3 name unique without user-edited placeholders.
        return f"{self.prefix}-data-{self.account}-{self.region}"

    @property
    def artifacts(self):
        # Keep deployed DAGs and release copies separate from operational data.
        return f"{self.prefix}-artifacts-{self.account}-{self.region}"

    @property
    def database(self):
        # Glue database identifiers use underscores; layer suffixes are added by callers.
        return f"{PROJECT}_{self.environment}"


class ObjectStore:
    """Persist small control documents and inspect immutable Bronze input objects."""

    def __init__(self, settings: Settings):
        # Reuse one client within the task; data access stays scoped to its environment.
        self.settings = settings
        self.s3 = client("s3", settings.region)

    def read(self, key: str, default=None):
        """Treat a missing initial checkpoint differently from an access failure."""
        try:
            response = self.s3.get_object(Bucket=self.settings.bucket, Key=key)
            return json.loads(response["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            # An absent checkpoint is normal on the first run. Access-denied and
            # service errors still propagate rather than appearing as empty state.
            return default

    def write(self, key: str, value):
        """S3 object replacement is atomic; bucket versioning retains prior checkpoints."""
        # Store JSON with explicit content type and encryption so state files are
        # inspectable in S3 and do not depend on the caller's default upload settings.
        self.s3.put_object(
            Bucket=self.settings.bucket,
            Key=key,
            Body=json.dumps(value, default=str, sort_keys=True).encode(),
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )

    def bronze_objects(self):
        """List the full retained prefix so an old event arriving today is still discovered."""
        paginator = self.s3.get_paginator("list_objects_v2")
        objects = []
        # Pagination includes every retained input, even once the bucket exceeds
        # one response page. Empty console-created folder markers are not inputs.
        for page in paginator.paginate(Bucket=self.settings.bucket, Prefix="bronze/"):
            for obj in page.get("Contents", []):
                if obj["Size"]:
                    objects.append({"key": obj["Key"], "etag": obj["ETag"], "size": obj["Size"]})
        return sorted(objects, key=lambda obj: obj["key"])

    def verify_snapshot(self, objects):
        """Fail if a frozen input disappeared or changed before its results were committed."""
        for obj in objects:
            # Compare both ETag and size with planning-time metadata. A missing key
            # raises from HeadObject; a replacement fails the explicit comparison.
            head = self.s3.head_object(Bucket=self.settings.bucket, Key=obj["key"])
            if head["ETag"] != obj["etag"] or head["ContentLength"] != obj["size"]:
                raise RuntimeError(f"Frozen bronze input changed: {obj['key']}")


class Athena:
    """Submit transformations and read small control/report results with bounded waits."""

    def __init__(self, settings: Settings, workgroup: str | None = None):
        self.settings = settings
        self.api = client("athena", settings.region)
        # Transformations use the private pipeline results prefix. Verification
        # explicitly selects the analyst workgroup and its separate output location.
        self.workgroup = workgroup or f"{settings.prefix}-pipeline"

    def execute(self, sql: str, label: str, timeout: int = 900) -> str:
        """Poll a submitted query; cancel timed-out work instead of leaving it running."""
        # The workgroup enforces encryption and the result location. The returned
        # query ID connects task logs to the exact statement in the Athena console.
        query_id = self.api.start_query_execution(QueryString=sql, WorkGroup=self.workgroup)[
            "QueryExecutionId"
        ]
        log("athena.started", label=label, query_id=query_id)
        # A monotonic deadline is unaffected by wall-clock corrections on the worker.
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            query = self.api.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            state = query["Status"]["State"]
            if state == "SUCCEEDED":
                log(
                    "athena.completed",
                    label=label,
                    query_id=query_id,
                    scanned_bytes=query.get("Statistics", {}).get("DataScannedInBytes", 0),
                )
                return query_id
            if state in ("FAILED", "CANCELLED"):
                # Surface Athena's reason immediately; retry policy belongs to Airflow.
                raise RuntimeError(
                    f"{label}: {query_id}: {query['Status'].get('StateChangeReason', state)}"
                )
            time.sleep(2)
        # Stop work at the service as well as locally so a timed-out task does not
        # leave a query consuming resources after its caller has given up.
        self.api.stop_query_execution(QueryExecutionId=query_id)
        raise TimeoutError(f"{label}: cancelled query {query_id} after {timeout}s")

    def rows(self, sql: str, label: str) -> list[dict]:
        """Read all result pages; small control queries never silently truncate their results."""
        query_id = self.execute(sql, label)
        paginator = self.api.get_paginator("get_query_results")
        output, header = [], None
        for page in paginator.paginate(QueryExecutionId=query_id):
            for row in page["ResultSet"]["Rows"]:
                values = [cell.get("VarCharValue") for cell in row["Data"]]
                # Athena includes the column-name row only at the start of the
                # result set. Missing VarCharValue represents SQL NULL, not "".
                if header is None:
                    header = values
                else:
                    output.append(dict(zip(header, values, strict=True)))
        return output

    def scalar(self, sql: str, label: str) -> int:
        """Extract the single count returned by a quality assertion."""
        # Quality queries return COUNT(*) with service-chosen column labels; read
        # the first cell by position rather than depending on that generated label.
        rows = self.rows(sql, label)
        return int(next(iter(rows[0].values()))) if rows else 0
