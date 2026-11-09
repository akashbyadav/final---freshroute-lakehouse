"""FreshRoute's data contracts, processing, and deployment tools.

Package import has no AWS side effects. CodeBuild enters through scripts/build.py;
Airflow tasks enter through pipeline.execute_task when a DAG run executes.
"""
