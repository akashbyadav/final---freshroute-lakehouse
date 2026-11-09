"""Validate the branch identity and deploy infrastructure and Airflow code."""

import os
import re
import sys

from freshroute.deploy import deploy
from freshroute.logging import configure, log
from freshroute.model import REGION


def build_context():
    """Reject a misrouted webhook before deployment changes AWS resources."""
    # Environment comes from the branch-specific CodeBuild project created by
    # CloudFormation. Resolve its branch once and validate the surrounding context.
    environment = os.environ["TARGET_ENV"]
    branch = {"dev": "dev", "prod": "main"}[environment]
    expected_project = f"freshroute-{environment}-build"
    # The project fixes the environment and IAM role. A console override must not
    # accidentally direct the dev build at production resources, or vice versa.
    if os.environ["CODEBUILD_BUILD_ID"].split(":", 1)[0] != expected_project:
        raise ValueError("TARGET_ENV does not match this CodeBuild project")
    if os.environ["AWS_REGION"] != REGION:
        raise ValueError(f"FreshRoute must run in {REGION}")
    # Console-started builds have no webhook ref. Push-triggered builds must match
    # the expected full ref so similarly named branches cannot route incorrectly.
    head = os.environ.get("CODEBUILD_WEBHOOK_HEAD_REF")
    if head and head != f"refs/heads/{branch}":
        raise ValueError(f"This project accepts pushes to {branch} only")
    # Reject stale console overrides from the earlier combined deployment/data flow.
    # A manual data operation must never silently become an infrastructure deployment.
    if os.environ.get("OPERATION", "deploy") != "deploy":
        raise ValueError("CodeBuild only deploys; upload data in S3 and start runs in Airflow")
    # CodeBuild resolves the branch/webhook to an immutable commit during checkout.
    # Reuse that SHA in Airflow and Gold instead of tagging results with a branch name.
    release = os.environ["CODEBUILD_RESOLVED_SOURCE_VERSION"]
    if not re.fullmatch(r"[0-9a-f]{40}", release):
        raise ValueError("Expected a resolved GitHub commit SHA")
    log(
        "build.context",
        environment=environment,
        branch=branch,
        release=release,
    )
    return environment, release


def main():
    """Finish when the environment and expected DAG revision are available."""
    configure()
    environment, release = build_context()
    if sys.argv[1:] == ["--check-context"]:
        # pre_build uses this validation-only path before creating any AWS resources.
        return
    # Deployment publishes application artifacts only. Source uploads, DAG runs,
    # backfills, and report queries are separate operator actions in SETUP.md.
    deploy(environment, release)
    log("build.completed", environment=environment, release=release, dag_paused=True)


if __name__ == "__main__":
    main()
