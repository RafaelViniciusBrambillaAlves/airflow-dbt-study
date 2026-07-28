"""
ecommerce_dbt_fusion_postgres_dag
------------------
Roda o mesmo projeto dbt via dbt Fusion, target Postgres (analytics_fusion).
 
INGESTAO PARAMETRIZAVEL (small/large)
Mesma logica das demais 3 DAGs (ver include/ingestion/airflow_tasks.py):
a Airflow Variable `ecommerce_dataset_size` decide, em runtime, entre dbt
seed (small, default) e geracao/carga fake via Faker (large, ~10.000
linhas), escrevendo no schema raw correspondente a este target
(analytics_fusion_raw) via a Connection `ecommerce_warehouse`.
"""

import os
from datetime import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.operators.local import DbtSeedLocalOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from include.ingestion.airflow_tasks import build_ingestion_branch

# Força o dbt-fusion a aceitar adaptadores experimentais (como o postgres)
os.environ["DBT_ALLOW_EXPERIMENTAL_ADAPTERS"] = "true"

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
DBT_FUSION_EXECUTABLE = "/usr/local/bin/dbt"

POSTGRES_CONN_ID = "ecommerce_warehouse"

RAW_SCHEMA = "analytics_warehouse"


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
    tags = ["dbt", "cosmos", "ecommerce", "postgres"],
    doc_md = __doc__,
)

def ecommerce_dbt_fusion_postgres_dag():

    start = EmptyOperator(task_id = "start")

    load_raw_seed = DbtSeedLocalOperator(
        task_id = "load_raw_seed",
        project_dir = DBT_PROJECT_DIR,
        profile_config = profile_config,
        dbt_executable_path = DBT_FUSION_EXECUTABLE,
        invocation_mode = InvocationMode.SUBPROCESS,
    )

    choose_ingestion_path, raw_data_ready = build_ingestion_branch(
        seed_operator = load_raw_seed,
        schema = RAW_SCHEMA,
        warehouse = "postgres",
        postgres_conn_id = POSTGRES_CONN_ID
    )

    transform_and_test = DbtTaskGroup(
        group_id = "transform_and_test",
        project_config = project_config,
        profile_config = profile_config,
        execution_config = execution_config,
        render_config = render_config
    )

    end = EmptyOperator(task_id = "end")

    start >> choose_ingestion_path
    raw_data_ready >> transform_and_test >> end

ecommerce_dbt_fusion_postgres_dag()