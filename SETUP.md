# Setup

**Deploy from GitHub. Upload source files in S3. Start processing in Airflow. Read the results in Athena.**

Use one AWS account and **US East (N. Virginia), `us-east-1`**, throughout. Use an administrative IAM or federated identity that can create resources and switch roles. MWAA and NAT gateways incur charges while provisioned; the first deployment takes tens of minutes.

If an earlier version is already deployed, update **freshroute-cicd** with the revised `infra/cicd.yaml`, keeping its existing parameter values, before pushing this version. This applies changes to CodeBuild and its deployment/governance roles. Branch builds update only the storage and platform stacks. Existing source data and tables remain available; do not upload the same inputs again.

## 1. Put the project in GitHub

1. Create a private repository named **freshroute-lakehouse**. Leave the initialization options unchecked.
2. Choose **uploading an existing file**, upload this folder's contents with their directories intact, and commit to **main**. An existing repository can use its usual Git workflow.
3. Confirm that `buildspec.yml`, `infra/`, `src/`, `dags/`, `scripts/`, `tests/`, both requirements files, and `pyproject.toml` are at the repository root.
4. Create **dev** from **main** using the branch selector.

Keep the supplied `data/base/` and `data/late/` files on your computer for the manual uploads below. They can also remain in GitHub for versioning. CodeBuild does not prepare, upload, or process them.

## 2. Connect AWS to GitHub

1. In the AWS console, select **N. Virginia**.
2. Open **Developer Tools → Settings → Connections → Create connection**.
3. Choose **GitHub**, name the connection **freshroute-github**, and choose **Connect to GitHub**.
4. Install or select the **AWS Connector for GitHub** app and authorize **freshroute-lakehouse**.
5. Wait for status **Available**, then copy the **connection ARN**.

## 3. Create the deployment setup once

1. Open **CloudFormation → Create stack → With new resources (standard)**.
2. Choose **Upload a template file** and select `infra/cicd.yaml`.
3. Enter:

| Field | Value |
| --- | --- |
| Stack name | `freshroute-cicd` |
| GitHubRepository | The actual `owner/freshroute-lakehouse` from your repository URL. |
| GitHubConnectionArn | The ARN copied in step 2. |

4. Keep the other defaults, acknowledge named IAM resources, and submit.
5. Wait for **CREATE_COMPLETE**. Confirm that CodeBuild contains **freshroute-dev-build** and **freshroute-prod-build**.

Account details and bucket names are resolved automatically. No AWS keys or GitHub secrets are required.

## 4. Deploy dev

1. Push a change to **dev**, or choose **Start build** on **freshroute-dev-build** for the existing commit.
2. CodeBuild checks the code, templates, deployment tests, and Airflow imports; creates or updates the storage/platform stacks; and uploads the Airflow code.
3. Wait for **Succeeded** and **`build.completed`** in the build log.
4. Open **Amazon MWAA → freshroute-dev-airflow → Open Airflow UI**. Confirm **freshroute_delivery** is present and **paused**.

A successful build means the application is deployed. It does not mean data has been loaded or a report produced. **Every deployment leaves the DAG paused**; resume it after the source files are ready. Existing running tasks finish before their code is replaced.

## 5. Upload the initial data manually

1. Open **CloudFormation → freshroute-dev-storage → Outputs** and copy the **DataBucket** value.
2. Open that bucket in **S3**. Create and open `bronze/files/base/`: create `bronze`, then `files`, then `base`.
3. Choose **Upload → Add files** and select these five files from your computer's `data/base/` folder:

   - `stores.jsonl`
   - `products.jsonl`
   - `orders.jsonl`
   - `order_items.jsonl`
   - `events.jsonl`

4. Choose **Upload** and wait until **all five files succeed**. Keep the DAG paused until then.

Upload the plain files as supplied. Keep `manifest.json`, `expected-default.json`, and the late-event file out of this initial upload. Do not overwrite or delete processed Bronze objects; place new inputs in a new folder or filename.

## 6. Start the first processing run

1. In Airflow, open **freshroute_delivery** and turn its pause toggle **on/unpaused**.
2. Choose the **Trigger DAG** play button. If a configuration field appears, leave it as `{}` and submit.
3. Open the run in **Grid** and wait for **success**. The processing tasks are `plan_inputs → merge → build → validate → publish`; cleanup runs afterward.
4. Keep the DAG enabled for its five-minute schedule. A scheduled run with no new files skips the data stages.

The first load should produce **7,000 valid orders: 6,526 delivered, 235 cancelled, and 239 pending**. The 42 deliberately invalid source orders are excluded and written under `quarantine/` in the data bucket.

## 7. Read the report with governed access

1. In the AWS account menu, choose **Switch role**. Enter your current account ID and role **freshroute-dev-analyst**. Your administrative identity supplies permission to assume this role.
2. Open **Athena → Query editor** in **N. Virginia** and select workgroup **freshroute-dev-analyst**.
3. Select data source **AwsDataCatalog** and database **freshroute_dev_gold**. Run:

```sql
SELECT business_date, store_name, city,
       order_count, delivered_count, cancelled_count, pending_count,
       on_time_count, late_count, round(on_time_pct, 2) AS on_time_pct,
       avg_picking_minutes, avg_dispatch_minutes, avg_travel_minutes,
       delivered_revenue_paise / 100.0 AS delivered_order_value_inr
FROM freshroute_dev_gold.store_daily
ORDER BY business_date, store_id;
```

4. Expect **42 rows**: six stores across seven dates. Use **Download results** for a CSV copy. The workgroup already supplies its S3 query-results location.
5. To check the access boundary, run `SELECT * FROM freshroute_dev_silver.orders LIMIT 1;`. Access to Silver must fail for this role.
6. Switch back to your administrative identity before uploads, deployments, or Airflow operations.

To compare overall counts after any operation, run this in the analyst workgroup:

```sql
SELECT sum(order_count) AS orders,
       sum(delivered_count) AS delivered,
       sum(cancelled_count) AS cancelled,
       sum(pending_count) AS pending,
       sum(delivered_revenue_paise) / 100.0 AS delivered_order_value_inr
FROM freshroute_dev_gold.store_daily;
```

## 8. Upload the late events manually

1. In the same data bucket, create and open **bronze/files/late/**.
2. Upload **only `data/late/events.jsonl`** from your computer. Keep it separate from the base `events.jsonl`.
3. Wait for the next scheduled Airflow run, or trigger **freshroute_delivery** with `{}` after the upload completes.
4. Query the report again. Expect **7,000 orders, 6,765 delivered, 235 cancelled, zero pending**, and delivered order value **₹2,863,493.80**.

This corrects the original order dates. No deployment or CodeBuild run is required. To use the optional Firehose route instead of this direct upload, follow the section below.

## 9. Run a historical backfill

1. Open **freshroute_delivery** in Airflow and ensure it is unpaused.
2. Choose **Trigger DAG** and open its JSON configuration field. Paste:

```json
{
  "mode": "backfill",
  "start_date": "2026-10-01",
  "end_date": "2026-10-07"
}
```

3. Submit, wait for **success**, and query Gold again. With unchanged inputs and logic, business totals remain the same.

Both dates are inclusive. Backfills use the deployed code and retained inputs; deploy a calculation change before rebuilding its dates. The DAG serializes scheduled and manual runs. After a code deployment, existing files require an explicit backfill to recalculate their results with the new logic.

## 10. Promote to production

1. Open a GitHub pull request from **dev** into **main** and merge it.
2. Wait for **freshroute-prod-build** to succeed. It deploys the independent prod resources and leaves its DAG paused.
3. Repeat steps 5–9 using **freshroute-prod-storage**, **freshroute-prod-airflow**, **freshroute-prod-analyst**, and **freshroute_prod_gold** in the SQL.

Dev data is not copied to prod. Future pushes deploy code and infrastructure; upload and processing remain separate. Changes to `infra/cicd.yaml` require updating **freshroute-cicd** in CloudFormation.

## Optional: send late events through Firehose

Use this route after the base load if you want events to pass through Firehose. The direct S3 upload in step 8 is sufficient for the main workflow.

1. Switch back to the administrative identity and open **AWS CloudShell** in **N. Virginia**.
2. Choose **Actions → Upload file** and upload `scripts/send_events.py`. Repeat for `data/late/events.jsonl`. Both remain plain files.
3. Run this single command:

```bash
python3 send_events.py events.jsonl --environment dev
```

4. The sender reports **firehose.accepted**. In S3, wait for objects under **bronze/stream/**, then observe the next Airflow run. Acceptance alone does not mean all buffered records have reached S3; keep scheduling enabled until the final report totals match step 8.

The sender uses CloudShell's AWS credentials and the existing **freshroute-dev-events** stream. It does not generate data, deploy resources, or trigger Airflow. For prod, use `--environment prod`. Replaying accepted events preserves the business totals through deduplication.

## Troubleshooting and cleanup

For deployment failures, read **CodeBuild → Build logs**, then the failing stack's **CloudFormation Events**. For processing failures, read the failed **Airflow task log**. If deployment reports queued Airflow runs, finish or cancel those runs in the UI before retrying; deployment will not start them for you.

### Gold build: incorrect timestamp precision

If `candidate.build` fails with `Incorrect timestamp precision for timestamp(0)` on `published_at`, deploy the corrected `src/freshroute/sql.py`. The candidate Parquet table requires millisecond precision; the SQL explicitly casts the saved publication time to `TIMESTAMP(3)`, including for existing run state.

1. Pause **freshroute_delivery**. Let running attempts finish and mark unwanted queued DAG runs failed before deploying.
2. Commit and push the fix to **dev**, then wait for **freshroute-dev-build** to succeed.
3. Unpause the DAG and trigger a new run with `{}`. The failed build did not advance the input checkpoint, so the existing Bronze files remain eligible. No re-upload or backfill is required for this failure.

Each build attempt uses a new candidate table and query-specific S3 location. The failed query's temporary output does not need deletion before retrying; the bucket lifecycle expires objects under `query-results/` after seven days.

### Opt-in creation: insufficient Glue permissions

If **AnalystOptIn** fails with `CreateLakeFormationOptIn: Insufficient Glue permissions to access database`, update the governance Lambda's IAM role through the CI/CD stack. Lake Formation administrator status and the opt-in API permissions do not supply Glue metadata access. The template grants `glue:GetDatabase`, `glue:GetTable`, and `glue:GetTables` on the catalog and the environment's Gold database/tables.

1. In **CloudFormation → freshroute-cicd**, choose **Update stack → Replace current template → Upload a template file** and select the current `infra/cicd.yaml`.
2. Keep the existing GitHub repository and connection parameters, acknowledge named IAM resources, submit, and wait for **UPDATE_COMPLETE**. This updates both `freshroute-dev-governance` and `freshroute-prod-governance`.
3. If the initial storage stack is stuck in **ROLLBACK_FAILED** because cleanup also reports a missing opt-in, apply the Lambda cleanup patch below before deleting that failed storage stack. Follow the retained-resource cleanup steps only for unused resources from the failed first deployment.
4. After recovery, commit and push the corrected files to **dev** to recreate storage and continue deployment. A push alone does not update `freshroute-cicd`.

### Initial storage deployment: an IAM grant already exists

Lake Formation can automatically grant `ALL` to `IAM_ALLOWED_PRINCIPALS` when a database is created. Older templates declared the same grant as `AWS::LakeFormation::PrincipalPermissions`, which can fail with `AlreadyExists` on `RawIamAccess`, `SilverIamAccess`, `GoldIamAccess`, or `ControlIamAccess`. The current template uses `AWS::LakeFormation::Permissions` for these four grants and retains them alongside their databases. Analyst permissions and hybrid opt-in remain in place.

If **freshroute-dev-storage** is **ROLLBACK_COMPLETE**, it cannot be updated. For a failed first deployment **before any source uploads or processing**, recover in this order:

1. Wait for the failed CodeBuild run to finish. In **CloudFormation → freshroute-dev-storage → Resources**, record the physical IDs of retained buckets, databases, and tables marked **DELETE_SKIPPED**.
2. Delete **freshroute-dev-storage**. Keep **freshroute-cicd** and the GitHub connection.
3. In **Glue → Data Catalog → Tables**, delete any retained tables from the recorded project databases. Then delete those databases: `freshroute_dev_raw`, `freshroute_dev_silver`, `freshroute_dev_gold`, and `freshroute_dev_control`, if present.
4. In **S3**, inspect the two recorded project buckets (`DataBucket` and `ArtifactsBucket`). Only if they are empty or contain solely metadata from this failed initial deployment, use **Empty**, then **Delete** for each. Emptying also removes object versions. These retained names would otherwise collide with the next deployment.
5. Commit and push the corrected files to **dev**, then wait for **freshroute-dev-build** to succeed before continuing with step 5 of this guide.

If any retained resource contains existing business data or results, preserve it and recover through CloudFormation resource import rather than following the deletion steps. This cleanup sequence applies only to a failed first deployment.

### Rollback or deletion: LakeFormation OptIn does not exist

This error comes from the governance Lambda's cleanup request. A previous request or partial creation can leave an opt-in absent. The handler accepts both `EntityNotFoundException` and the specific `InvalidInputException` message `LakeFormation OptIn does not exist` during deletion. Other errors still fail the operation.

If an older deployed Lambda is blocking cleanup, a GitHub push cannot update a stack in a failed rollback/deletion state. Patch that Lambda first:

1. In **Lambda → freshroute-dev-governance → Code → index.py**, find `except ClientError as error:`. Inside that block, replace the final `if code != expected: raise` check with the following, preserving its indentation:

```python
message = error.response["Error"].get("Message", "").strip().rstrip(".").casefold()
missing_opt_in = (
    event["RequestType"] == "Delete"
    and code == "InvalidInputException"
    and message == "lakeformation optin does not exist"
)
if code != expected and not missing_opt_in:
    raise
```

2. Choose **Deploy** in the Lambda editor. Do not invoke a saved CloudFormation event manually; the next stack operation supplies a fresh response URL.
3. Return to **CloudFormation → freshroute-dev-storage**. For **DELETE_FAILED**, retry deletion. For **ROLLBACK_FAILED** after a failed initial creation, delete the failed stack. For **UPDATE_ROLLBACK_FAILED**, choose **Stack actions → Continue update rollback**.
4. Keep the same fix in `infra/storage.yaml` when committing the next deployment. For a failed first deployment, review retained resources as described above before recreating storage.

A deletion error can be secondary to an earlier creation error. Read the first **CREATE_FAILED** event and its CloudWatch Lambda log before attempting another deployment; fixing cleanup alone does not resolve a separate creation failure.

### Operational checks and cleanup

Only one build per environment may run at a time. If another build is throttled, start it after the active build finishes. Re-enable the DAG after each successful deployment. Previously exported files are snapshots; query Athena for current results.

To stop running resources, wait for active work to finish, delete each **platform** stack, then its **storage** stack, and delete **freshroute-cicd** last. S3 buckets and catalog tables are retained intentionally; remove them separately only when no longer needed. Pausing Airflow alone does not stop infrastructure charges.
