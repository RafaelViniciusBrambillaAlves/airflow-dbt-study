import os
from datetime import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.operators.local import DbtSeedLocalOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

# Força o dbt-fusion a aceitar adaptadores experimentais (como o postgres)
os.environ["DBT_ALLOW_EXPERIMENTAL_ADAPTERS"] = "true"

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
DBT_FUSION_EXECUTABLE = "/usr/local/bin/dbt"
DBT_PROFILES_PATH = "/usr/local/airflow/include/dbt/ecommerce/profiles.yml"

project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR
)

profile_config = ProfileConfig(
    profile_name = "ecommerce",
    target_name = "dev_fusion_pg",
    profile_mapping = PostgresUserPasswordProfileMapping(
        conn_id = "ecommerce_warehouse",
        profile_args = {"schema": "analytics_fusion"}
    )
)

execution_config = ExecutionConfig(
    execution_mode = ExecutionMode.LOCAL,
    dbt_executable_path = DBT_FUSION_EXECUTABLE,
    invocation_mode = InvocationMode.SUBPROCESS, 
)

render_config = RenderConfig(
    select = ["path:models"],
    test_behavior = "after_each",
    load_method = LoadMode.DBT_LS,
    invocation_mode = InvocationMode.SUBPROCESS,
    dbt_executable_path = DBT_FUSION_EXECUTABLE,
)

@dag(
    dag_id = "ecommerce_dbt_fusion_postgres_dag",
    start_date = datetime(2026, 1, 1),
    schedule = "@daily",
    catchup = False,
    max_active_tasks = 4,
    tags = ["dbt", "cosmos", "ecommerce", "postgres"]
)

def ecommerce_dbt_fusion_postgres_dag():

    start = EmptyOperator(task_id = "start")

    load_raw_data = DbtSeedLocalOperator(
        task_id = "load_raw_data",
        project_dir = DBT_PROJECT_DIR,
        profile_config = profile_config,
        dbt_executable_path = DBT_FUSION_EXECUTABLE,
        invocation_mode = InvocationMode.SUBPROCESS,
    )

    transform_and_test = DbtTaskGroup(
        group_id = "transform_and_test",
        project_config = project_config,
        profile_config = profile_config,
        execution_config = execution_config,
        render_config = render_config
    )

    end = EmptyOperator(task_id = "end")

    start >> load_raw_data >> transform_and_test >> end 

ecommerce_dbt_fusion_postgres_dag()