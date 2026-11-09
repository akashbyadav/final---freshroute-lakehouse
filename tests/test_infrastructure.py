"""Check branch routing, deployment identities, and governed publication access."""

import json
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import yaml
from botocore.exceptions import ClientError

from freshroute.model import GOLD_SCHEMA, REGION, SCHEMAS

ROOT = Path(__file__).resolve().parents[1]


def test_static_tables_match_the_processing_contracts():
    # Templates are edited directly. A schema change must keep catalog definitions
    # aligned with the code before deployment rather than relying on a generator.
    resources = yaml.safe_load((ROOT / "infra/storage.yaml").read_text())["Resources"]
    for name, fields in {**SCHEMAS, "store_daily": GOLD_SCHEMA}.items():
        # Preserve both types and column order, since INSERT uses the shared contracts.
        logical_id = "".join(part.title() for part in name.split("_")) + "Table"
        columns = resources[logical_id]["Properties"]["TableInput"]["StorageDescriptor"]["Columns"]
        assert [(column["Name"], column["Type"]) for column in columns] == fields


def test_codebuild_routes_pushes_to_the_matching_environment():
    # Inspect the deployed project definitions rather than assuming the build
    # script alone can compensate for a broadly configured webhook.
    resources = yaml.safe_load((ROOT / "infra/cicd.yaml").read_text())["Resources"]
    assert REGION == "us-east-1"
    for title, environment, branch in (("Dev", "dev", "dev"), ("Prod", "prod", "main")):
        project = resources[title + "BuildProject"]["Properties"]
        assert project["SourceVersion"] == branch
        # One AND-group prevents a broad PUSH filter from bypassing branch matching.
        assert project["Triggers"] == {
            "Webhook": True,
            "FilterGroups": [
                [
                    {"Type": "EVENT", "Pattern": "PUSH"},
                    {"Type": "HEAD_REF", "Pattern": f"^refs/heads/{branch}$"},
                ]
            ],
        }
        assert project["Source"]["Auth"]["Type"] == "CODECONNECTIONS"
        assert project["Source"]["BuildSpec"] == "buildspec.yml"
        assert project["ConcurrentBuildLimit"] == 1
        variables = {
            item["Name"]: item["Value"] for item in project["Environment"]["EnvironmentVariables"]
        }
        assert variables["TARGET_ENV"] == environment
        assert set(variables) == {"TARGET_ENV"}
        assert project["ServiceRole"]["Fn::GetAtt"] == [title + "BuildRole", "Arn"]


def test_validation_failures_abort_before_cloud_mutation():
    # Pre-build failures must stop the deployment phase rather than allowing an
    # invalid branch revision to create or update environment resources.
    buildspec = yaml.safe_load((ROOT / "buildspec.yml").read_text())
    assert buildspec["phases"]["install"]["on-failure"] == "ABORT"
    assert buildspec["phases"]["pre_build"]["on-failure"] == "ABORT"
    assert buildspec["phases"]["build"]["commands"] == ["python scripts/build.py"]
    platform = yaml.safe_load((ROOT / "infra/platform.yaml").read_text())
    options = platform["Resources"]["Airflow"]["Properties"]["AirflowConfigurationOptions"]
    assert options["core.dags_are_paused_at_creation"] == "True"
    assert options["webserver.show_trigger_form_if_no_params"] == "True"


def test_build_has_no_source_data_or_event_sender_permissions():
    # IAM reinforces the code boundary: the build can publish application
    # artifacts but cannot upload source data, send events, or assume the analyst.
    template = yaml.safe_load((ROOT / "infra/cicd.yaml").read_text())
    for title in ("Dev", "Prod"):
        role = template["Resources"][title + "BuildRole"]["Properties"]
        statements = role["Policies"][0]["PolicyDocument"]["Statement"]
        actions = {action for item in statements for action in item["Action"]}
        assert "firehose:PutRecordBatch" not in actions
        assert "sts:AssumeRole" not in actions
        for item in statements:
            if "s3:PutObject" in item["Action"]:
                assert all("-artifacts-" in resource["Fn::Sub"] for resource in item["Resource"])
    buildspec = (ROOT / "buildspec.yml").read_text()
    assert "validate_dataset.py" not in buildspec
    assert "test_data_and_sql.py" not in buildspec


def test_build_roles_are_separate_and_project_bound():
    # Both role trusts must bind the service to this account and its specific
    # CodeBuild project, while preserving the existing Lake Formation administrators.
    template = yaml.safe_load((ROOT / "infra/cicd.yaml").read_text())
    for title, environment in (("Dev", "dev"), ("Prod", "prod")):
        role = template["Resources"][title + "BuildRole"]["Properties"]
        trust = role["AssumeRolePolicyDocument"]["Statement"][0]
        assert trust["Principal"] == {"Service": "codebuild.amazonaws.com"}
        assert trust["Condition"]["StringEquals"]["aws:SourceAccount"] == {"Ref": "AWS::AccountId"}
        assert trust["Condition"]["ArnEquals"]["aws:SourceArn"]["Fn::Sub"].endswith(
            f":project/freshroute-{environment}-build"
        )
    assert (
        template["Resources"]["LakeFormationAdministrators"]["Properties"]["MutationType"]
        == "APPEND"
    )


def test_analyst_has_no_direct_lake_data_permissions():
    # The analyst needs query-result access, but warehouse reads must go through
    # the governed path rather than a direct S3 object permission.
    template = yaml.safe_load((ROOT / "infra/storage.yaml").read_text())
    role = template["Resources"]["AnalystRole"]["Properties"]
    statements = role["Policies"][0]["PolicyDocument"]["Statement"]
    object_grants = [item for item in statements if "s3:GetObject" in item["Action"]]
    assert len(object_grants) == 1
    assert object_grants[0]["Resource"]["Fn::Sub"].endswith("/query-results/analyst/*")
    assert template["Resources"]["GoldRegistration"]["Properties"]["HybridAccessEnabled"]
    assert "AnalystOptIn" in template["Resources"]


@pytest.mark.parametrize("title,environment", [("Dev", "dev"), ("Prod", "prod")])
def test_governance_has_glue_metadata_access_for_its_opt_in_scope(title, environment):
    # Being an LF administrator is insufficient: opt-in must resolve Glue metadata
    # with the caller's IAM permissions, including the catalog/database ancestors.
    template = yaml.safe_load((ROOT / "infra/cicd.yaml").read_text())
    role = template["Resources"][title + "GovernanceRole"]["Properties"]
    statements = role["Policies"][0]["PolicyDocument"]["Statement"]
    glue_grants = [
        statement
        for statement in statements
        if statement["Effect"] == "Allow"
        and any(action.startswith("glue:") for action in statement["Action"])
    ]
    arn = "arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:"
    required_resources = {
        arn + "catalog",
        arn + f"database/freshroute_{environment}_gold",
        arn + f"table/freshroute_{environment}_gold/*",
    }
    required_actions = {"glue:GetDatabase", "glue:GetTable", "glue:GetTables"}
    # A wildcard action or other environment's database would widen governance
    # access; omitting a required ancestor reproduces the deployment failure.
    assert {action for grant in glue_grants for action in grant["Action"]} == required_actions
    for action in required_actions:
        resources = {
            resource["Fn::Sub"]
            for grant in glue_grants
            if action in grant["Action"]
            for resource in grant["Resource"]
        }
        assert resources == required_resources


def test_data_survives_stack_deletion_and_public_access_is_blocked():
    # Retention and public-access controls are data-protection contracts that
    # must survive routine infrastructure edits.
    template = yaml.safe_load((ROOT / "infra/storage.yaml").read_text())
    bucket = template["Resources"]["DataBucket"]
    assert bucket["DeletionPolicy"] == "Retain"
    assert all(bucket["Properties"]["PublicAccessBlockConfiguration"].values())
    assert template["Resources"]["StoreDailyTable"]["DeletionPolicy"] == "Retain"


@pytest.fixture
def governance_handler(monkeypatch):
    # Execute the actual inline Lambda source so these tests cover the code that
    # CloudFormation deploys, without maintaining a second implementation.
    resources = yaml.safe_load((ROOT / "infra/storage.yaml").read_text())["Resources"]
    source = resources["GovernanceFunction"]["Properties"]["Code"]["ZipFile"]
    namespace = {}
    exec(compile(source, "storage.yaml:GovernanceFunction", "exec"), namespace)
    api = Mock()
    monkeypatch.setattr(namespace["boto3"], "client", Mock(return_value=api))
    responses = []

    def respond(request, timeout):
        # Capture the CloudFormation callback locally; no AWS call or HTTP
        # request should escape a lifecycle regression test.
        assert request.get_method() == "PUT"
        responses.append(json.loads(request.data))
        return MagicMock()

    monkeypatch.setattr(namespace["urllib"].request, "urlopen", respond)
    event = {
        "RequestType": "Delete",
        "ResourceProperties": {
            "PrincipalArn": "arn:aws:iam::123456789012:role/freshroute-dev-analyst",
            "Database": "freshroute_dev_gold",
            "Account": "123456789012",
        },
        "PhysicalResourceId": "existing-analyst-opt-in",
        "StackId": "test-stack",
        "RequestId": "test-request",
        "LogicalResourceId": "AnalystOptIn",
        "ResponseURL": "https://cloudformation-response.invalid/response",
    }
    return namespace["handler"], api, event, responses


@pytest.mark.parametrize("missing_scope", [0, 1])
@pytest.mark.parametrize("code", ["InvalidInputException", "EntityNotFoundException"])
def test_governance_delete_finishes_when_one_opt_in_is_absent(
    governance_handler, missing_scope, code
):
    handler, api, event, responses = governance_handler
    # A partial creation or earlier cleanup can leave either the database opt-in
    # or the table opt-in absent. Cleanup must still attempt the other scope.
    outcomes = [{}, {}]
    outcomes[missing_scope] = ClientError(
        {"Error": {"Code": code, "Message": "LakeFormation OptIn does not exist"}},
        "DeleteLakeFormationOptIn",
    )
    api.delete_lake_formation_opt_in.side_effect = outcomes
    handler(event, None)
    assert len(responses) == 1
    assert responses[0]["Status"] == "SUCCESS"
    assert responses[0]["PhysicalResourceId"] == event["PhysicalResourceId"]
    calls = api.delete_lake_formation_opt_in.call_args_list
    assert len(calls) == 2
    assert "Database" in calls[0].kwargs["Resource"]
    assert "Table" in calls[1].kwargs["Resource"]
    api.create_lake_formation_opt_in.assert_not_called()


def test_governance_delete_can_be_retried_after_both_opt_ins_are_removed(governance_handler):
    handler, api, event, responses = governance_handler
    missing = ClientError(
        {
            "Error": {
                "Code": "InvalidInputException",
                "Message": "LakeFormation OptIn does not exist.",
            }
        },
        "DeleteLakeFormationOptIn",
    )
    # The first request deletes both scopes; a redelivered request finds neither.
    # Both requests must acknowledge success with the same physical resource ID.
    api.delete_lake_formation_opt_in.side_effect = [{}, {}, missing, missing]
    handler(event, None)
    handler({**event, "RequestId": "retry-request"}, None)
    assert [response["Status"] for response in responses] == ["SUCCESS", "SUCCESS"]
    assert {response["PhysicalResourceId"] for response in responses} == {
        event["PhysicalResourceId"]
    }
    assert api.delete_lake_formation_opt_in.call_count == 4


@pytest.mark.parametrize(
    "code,message",
    [
        ("InvalidInputException", "Resource contains an invalid database identifier"),
        ("InvalidInputException", "Database does not exist"),
        ("AccessDeniedException", "Caller cannot remove this opt-in"),
        ("InternalServiceException", "Service temporarily unavailable"),
    ],
)
def test_governance_delete_reports_real_failures(governance_handler, code, message):
    handler, api, event, responses = governance_handler
    # Only the known absence outcome is harmless. Broadly swallowing InvalidInput
    # or service errors would falsely mark incomplete cleanup as successful.
    api.delete_lake_formation_opt_in.side_effect = ClientError(
        {"Error": {"Code": code, "Message": message}}, "DeleteLakeFormationOptIn"
    )
    handler(event, None)
    assert len(responses) == 1
    assert responses[0]["Status"] == "FAILED"
    assert message in responses[0]["Reason"]


@pytest.mark.parametrize("request_type", ["Create", "Update"])
@pytest.mark.parametrize("code", ["AlreadyExistsException", "InvalidInputException"])
def test_governance_create_and_update_do_not_use_delete_exceptions(
    governance_handler, request_type, code
):
    handler, api, event, responses = governance_handler
    # Already-created opt-ins remain safe to retry. A missing opt-in during
    # creation is a failure, even if its text matches the deletion exception.
    api.create_lake_formation_opt_in.side_effect = ClientError(
        {"Error": {"Code": code, "Message": "LakeFormation OptIn does not exist"}},
        "CreateLakeFormationOptIn",
    )
    handler({**event, "RequestType": request_type}, None)
    expected_status = "SUCCESS" if code == "AlreadyExistsException" else "FAILED"
    assert len(responses) == 1
    assert responses[0]["Status"] == expected_status
    api.delete_lake_formation_opt_in.assert_not_called()
