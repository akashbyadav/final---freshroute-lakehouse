"""Test failure boundaries rather than AWS implementation internals."""

from copy import deepcopy

import pytest

from freshroute.aws import Settings
from freshroute.pipeline import Pipeline


class MemoryStore:
    """Model S3 JSON document copies without allowing shared Python object mutation."""

    def __init__(self, state):
        # Seed an existing checkpoint so the tests can detect an unintended advance.
        self.objects = {
            "state": deepcopy(state),
            "control/checkpoint.json": {"already-seen": "etag-old"},
        }

    def read(self, key, default=None):
        # Real S3 reads deserialize a new object; match that isolation in memory.
        return deepcopy(self.objects.get(key, default))

    def write(self, key, value):
        # Later changes to the caller's dictionary must not alter a completed write.
        self.objects[key] = deepcopy(value)

    def verify_snapshot(self, objects):
        # Input immutability is assumed here to isolate the publication failure boundary.
        pass


class FailingAthena:
    def execute(self, *args):
        # Every attempted SQL write fails at the point where publication would commit.
        raise RuntimeError("Injected query failure")


def pipeline_for(state):
    # Construct the coordinator without its AWS-initializing constructor. In-memory
    # state and an intentionally failing query adapter isolate publication behavior.
    pipeline = Pipeline.__new__(Pipeline)
    pipeline.settings = Settings("dev", "123456789012")
    pipeline.run_id = "failure-test"
    pipeline.state_key = "state"
    pipeline.gold = "gold"
    pipeline.store = MemoryStore(state)
    pipeline.athena = FailingAthena()
    return pipeline


def test_publish_failure_never_advances_checkpoint():
    # Inject the failure at Gold MERGE, after validation has passed. Neither the
    # shared checkpoint nor the successful-publication flag may advance.
    pipeline = pipeline_for(
        {
            "validated": True,
            "candidate": "candidate",
            "mode": "incremental",
            "objects": [{"key": "new", "etag": "new-etag"}],
        }
    )
    with pytest.raises(RuntimeError, match="Injected query failure"):
        pipeline.publish()
    assert pipeline.store.read("control/checkpoint.json") == {"already-seen": "etag-old"}
    assert not pipeline.store.read("state").get("published")


def test_unvalidated_results_cannot_be_published():
    # The quality gate must reject this state before reaching any query or write.
    pipeline = pipeline_for({"validated": False})
    with pytest.raises(RuntimeError, match="requires successful quality"):
        pipeline.publish()


def test_historical_run_does_not_advance_ingestion_checkpoint(monkeypatch):
    # A successful backfill can mark its own run published, but its input files
    # must not be marked consumed by the separate incremental checkpoint.
    pipeline = pipeline_for(
        {
            "validated": True,
            "mode": "backfill",
            "dates": [],
            "objects": [{"key": "new", "etag": "new-etag"}],
        }
    )

    class Metrics:
        def put_metric_data(self, **kwargs):
            # Keep this test focused on checkpoint behavior without calling CloudWatch.
            pass

    monkeypatch.setattr("freshroute.pipeline.client", lambda *args: Metrics())
    pipeline.publish()
    assert pipeline.store.read("control/checkpoint.json") == {"already-seen": "etag-old"}
    assert pipeline.store.read("state")["published"]


def test_environment_and_account_validation():
    # Reject unexpected environment names and malformed account IDs before either
    # can be interpolated into resource names or SQL database identifiers.
    with pytest.raises(ValueError):
        Settings("production; drop table", "123456789012")
    with pytest.raises(ValueError):
        Settings("dev", "not-an-account")
