"""Parse the actual DAG in an isolated Airflow installation without AWS access."""

import json
import shutil
import tempfile
from pathlib import Path

from airflow.models import DagBag

root = Path(__file__).resolve().parents[1]
# Recreate the deployed DAG/config layout in an automatically removed directory.
# No generated deployment.json or Airflow metadata belongs in the source tree.
with tempfile.TemporaryDirectory() as folder:
    directory = Path(folder)
    shutil.copy(root / "dags/delivery_pipeline.py", directory)
    # Only import-time configuration is needed. The fixed test account never
    # obtains credentials or causes AWS requests while the graph is being parsed.
    (directory / "deployment.json").write_text(
        json.dumps(
            {
                "environment": "dev",
                "account": "123456789012",
                "region": "us-east-1",
                "release": "validation",
            }
        )
    )
    # No connections are initialized during DAG import; runtime tasks obtain credentials later.
    bag = DagBag(str(directory), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    # Read the parsed graph directly; get_dag can consult Airflow's metadata database.
    dag = bag.dags.get("freshroute_delivery")
    assert dag is not None and dag.max_active_runs == 1
    # Verify the full graph includes publication, cleanup, and the failure watcher,
    # not merely that Python can import a module containing a partial DAG.
    assert set(dag.task_ids) == {
        "plan_inputs",
        "merge",
        "build",
        "validate",
        "publish",
        "cleanup",
        "mark_failed_run",
    }
    print("DAG import and single-writer scheduling checks passed")
