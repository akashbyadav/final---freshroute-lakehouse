"""CloudFormation and Airflow rollout invoked by AWS CodeBuild."""

import json
import time
from pathlib import Path

from botocore.exceptions import ClientError

from freshroute.aws import Settings, client
from freshroute.logging import log
from freshroute.model import REGION

# Resolve repository assets from this module, independent of the shell's directory.
ROOT = Path(__file__).resolve().parents[2]


def identity(environment: str, region=REGION):
    """Derive the account from CodeBuild's temporary service-role credentials."""
    # The active role determines the account; users never substitute an account ID
    # into templates, source code, or bucket names before a build can deploy.
    account = client("sts", region).get_caller_identity()["Account"]
    return Settings(environment, account, region)


def stack_exists(api, name):
    """Distinguish absent stacks from permissions and transient service failures."""
    try:
        return api.describe_stacks(StackName=name)["Stacks"][0]
    except ClientError as error:
        # CloudFormation reports absence as a ValidationError. Do not treat all
        # failures as absence: that could hide an IAM problem and attempt creation.
        if error.response["Error"]["Code"] == "ValidationError" and "does not exist" in str(error):
            return None
        raise


def deploy_stack(name: str, filename: str, parameters: dict, region: str, execution_role=None):
    """Create or update a stack and surface concrete failure events before exiting."""
    api = client("cloudformation", region)
    # Reuse one request shape for creation and updates. Named IAM resources are
    # intentional, and the separate CloudFormation role owns provisioning rights.
    request = dict(
        StackName=name,
        TemplateBody=(ROOT / "infra" / filename).read_text(),
        Parameters=[
            {"ParameterKey": key, "ParameterValue": str(value)} for key, value in parameters.items()
        ],
        Capabilities=["CAPABILITY_NAMED_IAM"],
        Tags=[{"Key": "Project", "Value": "freshroute"}],
    )
    if execution_role:
        request["RoleARN"] = execution_role
    exists = stack_exists(api, name)
    try:
        # An existing environment updates in place; the first build creates it.
        if exists:
            api.update_stack(**request)
        else:
            api.create_stack(**request, TimeoutInMinutes=90)
    except ClientError as error:
        # A code-only change may leave infrastructure identical. Treat the service's
        # explicit no-update response as success, while surfacing other errors.
        if "No updates are to be performed" in str(error):
            log("stack.unchanged", stack=name)
            return
        raise
    log("stack.started", stack=name)
    deadline = time.monotonic() + 7200
    while time.monotonic() < deadline:
        stack = api.describe_stacks(StackName=name)["Stacks"][0]
        status = stack["StackStatus"]
        log("stack.status", stack=name, status=status)
        if status in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
            return
        if "IN_PROGRESS" not in status:
            # A rollback is terminal failure even when rollback itself completed.
            # Include resource-level causes so CodeBuild logs point to the blocker.
            failures = [
                event
                for event in api.describe_stack_events(StackName=name)["StackEvents"]
                if "FAILED" in event["ResourceStatus"]
            ]
            for event in failures[:8]:
                log(
                    "stack.failure",
                    resource=event["LogicalResourceId"],
                    reason=event.get("ResourceStatusReason"),
                )
            raise RuntimeError(
                f"Stack {name} ended with {status}; inspect the logged resource failures"
            )
        time.sleep(15)
    raise TimeoutError(f"Stack {name} is still running; inspect it before retrying")


def mwaa_request(settings: Settings, method: str, route: str, body=None, query=None):
    """Use AWS-signed Airflow REST access; no web login tokens are stored."""
    # MWAA signs and forwards this request to Airflow. Omitting optional fields
    # avoids sending null bodies or query objects for endpoints that do not use them.
    request = {"Name": f"{settings.prefix}-airflow", "Method": method, "Path": route}
    if body is not None:
        request["Body"] = body
    if query is not None:
        request["QueryParameters"] = query
    response = client("mwaa", settings.region).invoke_rest_api(**request)
    # The outer AWS request can succeed while Airflow returns a 4xx/5xx response.
    # Check the nested status before the caller relies on the response document.
    if response["RestApiStatusCode"] >= 400:
        raise RuntimeError(
            f"Airflow returned {response['RestApiStatusCode']}: {response['RestApiResponse']}"
        )
    return response.get("RestApiResponse", {})


def active_runs(settings: Settings):
    """Ask Airflow for active states so old completed runs cannot hide queued work."""
    # Server-side state filters avoid a page of old successful runs concealing
    # queued work that must drain before the deployment replaces Python modules.
    return mwaa_request(
        settings,
        "GET",
        "/dags/freshroute_delivery/dagRuns",
        query={"state": ["running", "queued"], "limit": 100},
    )["dag_runs"]


def wait_for_dag(settings: Settings, expected_release=None):
    """Allow MWAA's S3 synchronization and DAG parsing to finish after deployment."""
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        try:
            dag = mwaa_request(settings, "GET", "/dags/freshroute_delivery")
            # A reachable old DAG is insufficient after an artifact update. Wait for
            # the scheduler to expose the exact release that was just uploaded.
            tags = {item["name"] for item in dag.get("tags", [])}
            if expected_release and f"release:{expected_release}" not in tags:
                raise RuntimeError("Waiting for the deployed release to be parsed")
            return dag
        except (ClientError, RuntimeError) as error:
            # The environment may be available before its DAG is parsed. Retry
            # within the fixed window and retain the last symptom in build logs.
            log("airflow.waiting_for_dag", detail=str(error)[:250])
            time.sleep(15)
    raise TimeoutError("DAG was not available after 15 minutes; inspect MWAA DAG processing logs")


def deploy(environment: str, release: str, region: str = REGION):
    """Provision storage, publish runnable code, then create or update MWAA."""
    settings = identity(environment, region)
    # The caller supplies the checked-out commit, so DAG tags and published metrics
    # identify the same source revision that CodeBuild validated.
    settings.release = release
    execution_role = f"arn:aws:iam::{settings.account}:role/{settings.prefix}-cfn"
    # Drain work before changing code; all tasks in an active run keep the old files.
    api = client("cloudformation", region)
    platform_exists = stack_exists(api, f"{settings.prefix}-platform")
    if platform_exists:
        mwaa_request(settings, "PATCH", "/dags/freshroute_delivery", {"is_paused": True})
        deadline = time.monotonic() + 10800
        while True:
            active = active_runs(settings)
            if not active:
                mwaa_request(settings, "PATCH", "/dags/freshroute_delivery", {"is_paused": True})
                # Close the scheduler race: a run queued just before pausing must
                # finish before libraries are replaced, including manual backfills.
                if not active_runs(settings):
                    break
            # Deployment must not unpause a DAG to execute queued data work. An
            # operator can finish or cancel those runs in Airflow and retry the build.
            if any(run["state"] == "queued" for run in active):
                raise RuntimeError(
                    "Queued Airflow runs must finish or be cancelled in the Airflow UI "
                    "before deployment; the DAG remains paused"
                )
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "Running Airflow tasks did not finish; code is unchanged and the DAG is paused"
                )
            log("deploy.waiting_for_runs", active=len(active))
            time.sleep(15)
    # Storage precedes code upload because it creates the versioned artifact
    # bucket, pipeline role, catalog, and permissions required by a new MWAA environment.
    deploy_stack(
        f"{settings.prefix}-storage",
        "storage.yaml",
        {"Environment": environment},
        region,
        execution_role,
    )
    s3 = client("s3", region)
    config = {
        "environment": environment,
        "account": settings.account,
        "region": region,
        "release": settings.release,
    }
    # Upload the library first and the DAG last, preventing imports from a partially uploaded
    # package.
    for source in sorted((ROOT / "src/freshroute").glob("*.py")):
        if source.name in ("deploy.py", "data.py"):
            # Build-time helpers are not needed by workers and stay out of the DAG package.
            continue
        s3.put_object(
            Bucket=settings.artifacts,
            Key=f"dags/freshroute/{source.name}",
            Body=source.read_bytes(),
        )
        # Preserve a commit-addressed copy alongside the active worker package for
        # inspection; this archive does not change which code MWAA currently loads.
        s3.put_object(
            Bucket=settings.artifacts,
            Key=f"releases/{settings.release}/freshroute/{source.name}",
            Body=source.read_bytes(),
        )
    # Configuration is generated from the running build. Ignore the helper package
    # during DAG discovery, then upload the graph only after all imports are present.
    s3.put_object(
        Bucket=settings.artifacts, Key="dags/deployment.json", Body=json.dumps(config).encode()
    )
    s3.put_object(Bucket=settings.artifacts, Key="dags/.airflowignore", Body=b"freshroute/\n")
    s3.put_object(
        Bucket=settings.artifacts,
        Key="dags/delivery_pipeline.py",
        Body=(ROOT / "dags/delivery_pipeline.py").read_bytes(),
    )
    # MWAA creation/update follows the storage and artifact steps. Finally verify
    # that the scheduler sees this exact release. Loading data and starting runs
    # remain manual actions after deployment, including the very first load.
    deploy_stack(
        f"{settings.prefix}-platform",
        "platform.yaml",
        {"Environment": environment},
        region,
        execution_role,
    )
    wait_for_dag(settings, settings.release)
    # Keep every deployment paused until the operator confirms inputs are ready.
    # This also prevents a code change from immediately reprocessing existing data.
    mwaa_request(settings, "PATCH", "/dags/freshroute_delivery", {"is_paused": True})
    log(
        "deploy.ready",
        environment=environment,
        release=settings.release,
        airflow=f"{settings.prefix}-airflow",
        dag_paused=True,
    )
    return settings
