"""Exercise deployment routing without reading, preparing, or uploading data."""

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import pytest

spec = importlib.util.spec_from_file_location(
    "build", Path(__file__).resolve().parents[1] / "scripts/build.py"
)
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


@pytest.fixture
def context(monkeypatch):
    # Reproduce only deployment identity. No source files or data-operation
    # parameters are required for a CodeBuild project to deploy successfully.
    monkeypatch.setenv("TARGET_ENV", "dev")
    monkeypatch.setenv("CODEBUILD_BUILD_ID", "freshroute-dev-build:unique-build-id")
    monkeypatch.setenv("CODEBUILD_RESOLVED_SOURCE_VERSION", "a" * 40)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.delenv("OPERATION", raising=False)
    monkeypatch.setenv("CODEBUILD_WEBHOOK_HEAD_REF", "refs/heads/dev")
    monkeypatch.setattr(build.sys, "argv", ["scripts/build.py"])


def test_webhook_and_manual_builds_resolve_same_environment(context, monkeypatch):
    assert build.build_context() == ("dev", "a" * 40)
    # The console's Start build action also deploys; it cannot select a data operation.
    monkeypatch.delenv("CODEBUILD_WEBHOOK_HEAD_REF")
    assert build.build_context() == ("dev", "a" * 40)


@pytest.mark.parametrize(
    "name,value",
    [
        ("CODEBUILD_WEBHOOK_HEAD_REF", "refs/heads/main"),
        ("TARGET_ENV", "prod"),
        ("AWS_REGION", "eu-west-1"),
        ("CODEBUILD_RESOLVED_SOURCE_VERSION", "dev"),
        ("OPERATION", "backfill"),
        ("OPERATION", "deliver-late-events"),
        ("OPERATION", "report"),
    ],
)
def test_wrong_context_or_old_data_override_is_rejected(context, monkeypatch, name, value):
    # Reject misrouting and obsolete data overrides before infrastructure changes.
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        build.build_context()


def test_build_succeeds_without_source_data_or_report_queries(context, monkeypatch):
    deploy = Mock()
    monkeypatch.setattr(build, "deploy", deploy)
    # Any unexpected data-file read fails the test. Deployment has its own tests
    # for artifact reads; the build entry point must have no data dependency.
    monkeypatch.setattr(
        Path, "read_bytes", Mock(side_effect=AssertionError("Unexpected file read"))
    )
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=AssertionError("Unexpected file read")))
    build.main()
    deploy.assert_called_once_with("dev", "a" * 40)


def test_context_check_does_not_deploy(context, monkeypatch):
    monkeypatch.setattr(build.sys, "argv", ["scripts/build.py", "--check-context"])
    deploy = Mock()
    monkeypatch.setattr(build, "deploy", deploy)
    build.main()
    deploy.assert_not_called()
