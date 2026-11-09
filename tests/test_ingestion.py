"""Check the optional manual sender without contacting AWS or preparing data."""

import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("send_events", ROOT / "scripts/send_events.py")
sender = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sender)


def test_firehose_receives_original_late_envelopes(monkeypatch):
    # Read the committed source file directly. AWS clients are replaced, so this
    # checks byte preservation without generating another dataset or sending it.
    received = []
    api = Mock()

    def accept(**request):
        assert request["DeliveryStreamName"] == "freshroute-dev-events"
        received.extend(record["Data"] for record in request["Records"])
        return {"RequestResponses": [{"RecordId": "accepted"} for _ in request["Records"]]}

    api.put_record_batch.side_effect = accept
    monkeypatch.setattr(sender.boto3, "client", lambda *args, **kwargs: api)
    source = ROOT / "data/late/events.jsonl"
    count = sender.send_events(source, "dev")
    assert b"".join(received) == source.read_bytes()
    assert count == len(received) == 246
    assert all(json.loads(row)["entity_type"] == "events" for row in received)


def test_only_failed_firehose_entries_are_retried(monkeypatch):
    # A partially successful API response must not duplicate its accepted entry.
    api = Mock()
    api.put_record_batch.side_effect = [
        {"RequestResponses": [{"RecordId": "ok"}, {"ErrorCode": "ServiceUnavailableException"}]},
        {"RequestResponses": [{"RecordId": "ok"}]},
    ]
    monkeypatch.setattr(sender.time, "sleep", lambda seconds: None)
    sender.send_batch(api, "freshroute-dev-events", [b"first\n", b"second\n"])
    assert api.put_record_batch.call_args_list[1].kwargs["Records"] == [{"Data": b"second\n"}]


def test_sender_rejects_non_event_input_before_aws(monkeypatch):
    # Accidentally selecting the order file must fail before any AWS call.
    client = Mock()
    monkeypatch.setattr(sender.boto3, "client", client)
    with pytest.raises(ValueError, match="events JSONL"):
        sender.send_events(ROOT / "data/base/orders.jsonl", "dev")
    client.assert_not_called()
