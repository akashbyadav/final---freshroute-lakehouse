"""Airflow graph for incremental ingestion and explicit historical reprocessing."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from freshroute.pipeline import execute_task

# Deployment writes this file automatically from AWS identity and the Git revision.
# Keeping configuration beside the DAG avoids untracked console-created Airflow variables.
CONFIG = json.loads((Path(__file__).parent / "deployment.json").read_text())


def report_failure(context):
    """Emit a metric with no event payload or credentials in the callback."""
    # Match the Environment dimension used by the CloudFormation alarm. Only a
    # count is sent; task details remain in the corresponding CloudWatch log.
    boto3.client("cloudwatch", region_name=CONFIG["region"]).put_metric_data(
        Namespace="FreshRoute",
        MetricData=[
            {
                "MetricName": "PipelineFailure",
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [{"Name": "Environment", "Value": CONFIG["environment"]}],
            }
        ],
    )


def fail_run():
    """Prevent an all-done cleanup leaf from hiding an upstream processing failure."""
    # Airflow derives the run status from leaf tasks. This failing leaf preserves
    # an upstream failure even when the resource-cleanup task itself succeeds.
    raise RuntimeError("An upstream task failed; the run is unsuccessful even if cleanup completed")


with DAG(
    dag_id="freshroute_delivery",
    description=(
        "Incremental delivery analytics with governed publication and historical reprocessing"
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    # Historical ranges are explicit DAG-run parameters, not thousands of
    # automatically created five-minute intervals since start_date.
    catchup=False,
    # Both manual backfills and scheduled work enter this DAG. This setting serializes
    # the entire read/merge/validate/publish transaction sequence across all run modes.
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=3),
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": report_failure,
    },
    tags=["freshroute", CONFIG["environment"], "lakehouse", f"release:{CONFIG['release']}"],
) as dag:
    # Planning returns False when no input is new. Skip transformations in that
    # case while still allowing the ALL_DONE cleanup task to evaluate normally.
    plan = ShortCircuitOperator(
        task_id="plan_inputs",
        python_callable=execute_task,
        op_kwargs={"stage": "plan", **CONFIG},
        ignore_downstream_trigger_rules=False,
    )
    tasks = [plan]
    for stage in ("merge", "build", "validate", "publish"):
        # Small task arguments carry control metadata; rows remain in S3 and Athena.
        task = PythonOperator(
            task_id=stage, python_callable=execute_task, op_kwargs={"stage": stage, **CONFIG}
        )
        tasks[-1] >> task
        tasks.append(task)
    # Attach every processing stage so cleanup runs after both successful and
    # failed paths. It removes temporary catalog entries, never source files.
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=execute_task,
        op_kwargs={"stage": "cleanup", **CONFIG},
        trigger_rule=TriggerRule.ALL_DONE,
    )
    tasks >> cleanup
    # Watch cleanup as well as processing so a cleanup failure also marks the run
    # unsuccessful. No retry is needed for a task whose only job is to fail visibly.
    watcher = PythonOperator(
        task_id="mark_failed_run",
        python_callable=fail_run,
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
    )
    tasks + [cleanup] >> watcher
