"""Verify deployments publish code and leave data execution under operator control."""

import json
from unittest.mock import Mock

import pytest

from freshroute import deploy
from freshroute.aws import Settings


@pytest.mark.parametrize("existing", [False, True])
def test_rollout_uploads_only_code_and_leaves_dag_paused(monkeypatch, existing):
    settings = Settings("dev", "123456789012")
    s3 = Mock()
    requests = []
    stacks = []
    monkeypatch.setattr(deploy, "identity", lambda *args: settings)
    monkeypatch.setattr(deploy, "client", lambda *args: s3)
    # A truthy stack exercises the pause/drain path as well as fresh creation.
    monkeypatch.setattr(
        deploy, "stack_exists", lambda *args: {"exists": True} if existing else None
    )
    monkeypatch.setattr(deploy, "active_runs", lambda *args: [])
    monkeypatch.setattr(deploy, "wait_for_dag", Mock())
    monkeypatch.setattr(deploy, "deploy_stack", lambda *args: stacks.append(args[1]))
    monkeypatch.setattr(
        deploy, "mwaa_request", lambda *args, **kwargs: requests.append((args, kwargs))
    )
    deploy.deploy("dev", "a" * 40)
    assert stacks == ["storage.yaml", "platform.yaml"]
    assert requests and all(
        args[1] == "PATCH" and args[3] == {"is_paused": True} for args, _ in requests
    )
    writes = [call.kwargs for call in s3.put_object.call_args_list]
    assert all(request["Bucket"] == settings.artifacts for request in writes)
    assert all(request["Key"].startswith(("dags/", "releases/")) for request in writes)
    config = next(request for request in writes if request["Key"] == "dags/deployment.json")
    assert json.loads(config["Body"])["release"] == "a" * 40


def test_queued_data_run_never_gets_started_by_deployment(monkeypatch):
    # Unpausing queued work would make a code deployment start a data operation.
    # Stop instead, leaving the operator to resolve that run in the Airflow UI.
    monkeypatch.setattr(deploy, "identity", lambda *args: Settings("dev", "123456789012"))
    monkeypatch.setattr(deploy, "client", Mock())
    monkeypatch.setattr(deploy, "stack_exists", lambda *args: {"exists": True})
    monkeypatch.setattr(deploy, "active_runs", lambda *args: [{"state": "queued"}])
    request = Mock()
    monkeypatch.setattr(deploy, "mwaa_request", request)
    stack = Mock()
    monkeypatch.setattr(deploy, "deploy_stack", stack)
    with pytest.raises(RuntimeError, match="Queued Airflow runs"):
        deploy.deploy("dev", "a" * 40)
    stack.assert_not_called()
    request.assert_called_once_with(
        Settings("dev", "123456789012", release="a" * 40),
        "PATCH",
        "/dags/freshroute_delivery",
        {"is_paused": True},
    )
