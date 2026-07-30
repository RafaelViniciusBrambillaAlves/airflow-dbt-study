"""
ecommerce_dbt_core_postgres_dag
------------------
Orchestrates the `ecommerce` dbt project (Postgres target) through Astronomer Cosmos.
Follows the exact same isolated venv pattern as the DuckDB DAG.
 
INGESTAO PARAMETRIZAVEL (small/large)
A carga dos dados brutos bifurca em runtime via a Airflow Variable
`ecommerce_dataset_size` (ver include/ingestion/airflow_tasks.py):
  - "small" (default): dbt seed, comportamento original.
  - "large": ~10.000 linhas de raw_orders geradas com Faker e carregadas
    direto no schema raw do Postgres (via a mesma Connection
    `ecommerce_warehouse` usada pelo Cosmos), sem passar pelo dbt seed.
"""

from datetime import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.operators.local import DbtSeedLocalOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from include.ingestion.airflow_tasks import build_ingestion_branch

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
DBT_CORE_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

POSTGRES_CONN_ID = "ecommerce_warehouse"

RAW_SCHEMA = "analytics_core_raw"

_project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR
)

_profile_config = ProfileConfig(
    profile_name = "ecommerce",
    target_name = "dev_core_pg",
    profile_mapping = PostgresUserPasswordProfileMapping(
        conn_id="ecommerce_warehouse",
        profile_args={"schema": "analytics_core"},
    ),
)

_execution_config = ExecutionConfig(
    execution_mode = ExecutionMode.LOCAL,
    dbt_executable_path = DBT_CORE_EXECUTABLE,
    invocation_mode = InvocationMode.SUBPROCESS, 
)

_render_config = RenderConfig(
    select = ["path:models"],
    test_behavior = "after_each",
    load_method = LoadMode.DBT_LS,
    invocation_mode = InvocationMode.SUBPROCESS, 
    dbt_executable_path = DBT_CORE_EXECUTABLE,
)

@dag(
    dag_id = "ecommerce_dbt_core_postgres_dag",
    start_date = datetime(2026, 1, 1),
    schedule = "@daily",
    catchup = False,
    max_active_tasks = 4,
    tags = ["dbt", "cosmos", "ecommerce", "postgres"],
    doc_md = __doc__,
)

def ecommerce_dbt_core_postgres_dag():

    start = EmptyOperator(task_id = "start")

    load_raw_seed = DbtSeedLocalOperator(
        task_id = "load_raw_seed",
        project_dir = DBT_PROJECT_DIR,
        profile_config = _profile_config,
        dbt_executable_path = DBT_CORE_EXECUTABLE,
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
        project_config = _project_config,
        profile_config = _profile_config,
        execution_config = _execution_config,
        render_config = _render_config
    )

    end = EmptyOperator(task_id = "end")

    start >> choose_ingestion_path
    raw_data_ready >> transform_and_test >> end

ecommerce_dbt_core_postgres_dag()