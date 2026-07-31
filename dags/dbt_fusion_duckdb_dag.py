"""
ecommerce_dbt_fusion_duckdb_dag
-------------------------------
Runs the ecommerce dbt project with the dbt Fusion binary against DuckDB.

Raw data is no longer loaded inside this DAG. The DAG is scheduled by
ECOMMERCE_RAW_READY_ASSET, emitted by ecommerce_ingestion_dag, and can also
be triggered manually when the raw schemas are already populated.
"""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag
from cosmos import DbtTaskGroup, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode

from include.ingestion.assets import ECOMMERCE_RAW_READY_ASSET

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
DBT_FUSION_EXECUTABLE = "/usr/local/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR,
)

_profile_config = ProfileConfig(
    profile_name = "ecommerce",
    target_name = "dev_fusion",
    profiles_yml_filepath = f"{DBT_PROJECT_DIR}/profiles.yml",
)

_execution_config = ExecutionConfig(
    execution_mode = ExecutionMode.WATCHER,
    dbt_executable_path = DBT_FUSION_EXECUTABLE,
    invocation_mode = InvocationMode.SUBPROCESS,
)

_render_config = RenderConfig(
    select = ["path:models"],
    test_behavior = "after_each",
    load_method = LoadMode.DBT_LS,
    invocation_mode = InvocationMode.SUBPROCESS,
    dbt_executable_path = DBT_FUSION_EXECUTABLE,
)


@dag(
    dag_id = "ecommerce_dbt_fusion_duckdb_dag",
    start_date = datetime(2026, 1, 1),
    schedule = [ECOMMERCE_RAW_READY_ASSET], # Consumidor
    catchup = False,
    max_active_tasks = 1,
    tags = ["dbt", "cosmos", "ecommerce", "fusion", "duckdb"],
    doc_md = __doc__,
)
def ecommerce_dbt_fusion_duckdb_dag():
    start = EmptyOperator(task_id = "start")

    transform_and_test = DbtTaskGroup(
        group_id = "transform_and_test",
        project_config = _project_config,
        profile_config = _profile_config,
        execution_config = _execution_config,
        render_config = _render_config,
    )

    end = EmptyOperator(task_id = "end")

    start >> transform_and_test >> end


ecommerce_dbt_fusion_duckdb_dag()
