"""
ecommerce_dbt_core_duckdb_dag
------------------
Orchestrates the `ecommerce` dbt project through Astronomer Cosmos instead
of a raw BashOperator calling `dbt run`.
 
WHY COSMOS INSTEAD OF `BashOperator(bash_command="dbt run")`?
A single BashOperator treats the whole dbt project as one opaque step: if
model #12 out of 15 fails, Airflow just shows "task failed" with no signal
about which model, no per-model retries, and no lineage in the Airflow UI.
Cosmos parses the dbt project's manifest and turns EVERY dbt model and test
into its own Airflow task, wired together with the exact same dependency
edges dbt already computed. That gives you: per-model retries, per-model
logs, a lineage graph that mirrors dbt's DAG inside Airflow's Graph view,
and a pipeline that fails at the exact model/test that broke instead of a
single black box. The cost is a small amount of extra parsing at DAG-parse
time, which is negligible for a project this size.
 
INGESTAO PARAMETRIZAVEL (small/large)
A carga dos dados brutos agora bifurca em runtime, via a Airflow Variable
`ecommerce_dataset_size` (ver include/ingestion/airflow_tasks.py):
  - "small" (default): dbt seed, comportamento original, sem alteracao.
  - "large": ~10.000.000 linhas de raw_orders (configuravel via
    `ecommerce_large_dataset_n_orders`), geradas com Faker e carregadas
    direto no schema raw do DuckDB - nao usa dbt seed, pois seeds nao sao
    recomendados pela documentacao oficial do dbt para volumes desse
    tamanho (ver docstring de generate_fake_data.py).
Isso evita duplicar esta DAG em duas (pequena/grande): a mesma DAG roda
com um volume ou outro dependendo do valor da Variable no momento do
trigger.
"""

import os
from datetime import datetime

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.operators.local import DbtSeedLocalOperator

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from include.ingestion.airflow_tasks import build_ingestion_branch

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
DBT_CORE_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

DUCKDB_PATH = os.environ.get(
    "DUCKDB_PATH", "/usr/local/airflow/duckdb_data/ecommerce.duckdb"
)

RAW_SCHEMA = "analytics_core_raw"

project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR
)


profile_config = ProfileConfig(
    profile_name = "ecommerce",
    target_name = "dev_core",
    profiles_yml_filepath = f"{DBT_PROJECT_DIR}/profiles.yml"
)


execution_config = ExecutionConfig(
    execution_mode = ExecutionMode.LOCAL,
    dbt_executable_path = DBT_CORE_EXECUTABLE,
    invocation_mode = InvocationMode.SUBPROCESS,
)


render_config = RenderConfig(
    select = ["path:models"],
    test_behavior = "after_each",  
    load_method = LoadMode.DBT_LS,
)

@dag(
    dag_id = "ecommerce_dbt_core_duckdb_dag",
    start_date = datetime(2026, 1, 1),
    schedule = "@daily",
    catchup = False,
    max_active_tasks = 1, # No paralelism
    tags = ["dbt", "cosmos", "ecommerce", "duckdb"],
    doc_md = __doc__,
)

def ecommerce_dbt_core_duck_db_dag():

    start = EmptyOperator(task_id = "start")

    load_raw_seed = DbtSeedLocalOperator(
        task_id = "load_raw_seed",
        project_dir = DBT_PROJECT_DIR,
        profile_config = profile_config,
        dbt_executable_path = DBT_CORE_EXECUTABLE,
        invocation_mode = InvocationMode.SUBPROCESS
    )

    choose_ingestion_path, raw_data_ready = build_ingestion_branch(
        seed_operator = load_raw_seed,
        schema = RAW_SCHEMA,
        warehouse = "duckdb",
        duckdb_path = DUCKDB_PATH 
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
    

ecommerce_dbt_core_duck_db_dag()
