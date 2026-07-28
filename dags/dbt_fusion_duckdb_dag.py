"""
ecommerce_dbt_fusion_duckdb_dag
-------------------------
Roda o MESMO projeto dbt (`include/dbt/ecommerce`) usado pela
`ecommerce_dbt_core_dag`, mas através do motor dbt Fusion (Rust) em vez do
dbt Core (Python), escrevendo num schema separado (`analytics_fusion_*`)
para permitir comparação lado a lado sem conflito de dados.
 
POR QUE NÃO DUPLICAR OS MODELOS SQL?
Fusion e Core compilam o MESMO dbt_project.yml/models/*.sql — a diferença
está inteiramente em QUAL BINÁRIO o Cosmos invoca (dbt_executable_path),
não em qual código SQL é executado. Duplicar os .sql criaria dois projetos
para manter sincronizados sem necessidade nenhuma.
 
LIMITAÇÃO CONHECIDA (documentada, não workaround):
Cosmos só suporta dbt Fusion com ExecutionMode.LOCAL (não há VIRTUALENV
nem AIRFLOW_ASYNC para Fusion). InvocationMode.DBT_RUNNER também não se
aplica a Fusion, pois ele não é uma lib Python instalável no venv do
Airflow — por isso usamos SUBPROCESS explicitamente, igual à DAG do Core.
Fonte: https://astronomer.github.io/astronomer-cosmos/configuration/dbt-fusion.html
 
INGESTAO PARAMETRIZAVEL (small/large)
Mesma logica da DAG do Core (ver include/ingestion/airflow_tasks.py):
a Airflow Variable `ecommerce_dataset_size` decide, em runtime, entre
dbt seed (small, default) e geracao/carga fake via Faker (large, ~10.000
linhas), escrevendo no schema raw correspondente a este target
(analytics_fusion_raw).
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
DBT_FUSION_EXECUTABLE = "/usr/local/bin/dbt"

DUCKDB_PATH = os.environ.get(
    "DUCKDB_PATH", "/usr/local/airflow/duckdb_data/ecommerce.duckdb"
)

RAW_SCHEMA = "analytics_fusion_raw"

project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR
)

profile_config = ProfileConfig(
    profile_name = "ecommerce",
    target_name = "dev_fusion",
    profiles_yml_filepath = f"{DBT_PROJECT_DIR}/profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode = ExecutionMode.LOCAL, # único modo suportado pelo Cosmos para Fusion
    dbt_executable_path = DBT_FUSION_EXECUTABLE,
    invocation_mode = InvocationMode.SUBPROCESS, # DBT_RUNNER não se aplica a Fusion (não é lib Python)
)

render_config = RenderConfig(
    select = ["path:models"],
    test_behavior = "after_each",
    load_method = LoadMode.DBT_LS,
    dbt_executable_path = DBT_FUSION_EXECUTABLE,
)

@dag(
    dag_id = "ecommerce_dbt_fusion_duckdb_dag",
    start_date = datetime(2026, 1, 1),
    schedule = "@daily",
    catchup = False,
    max_active_tasks = 1, # DuckDB não paraleliza escrita - mesma restrição da DAG do Core
    tags = ["dbt", "cosmos", "ecommerce", "fusion", "duckdb"],
    doc_md = __doc__,
)

def ecommerce_dbt_fusion_duckdb_dag():

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
        warehouse = "duckdb",
        duckdb_path = DUCKDB_PATH,
    )

    transform_and_test = DbtTaskGroup(
        group_id = "transform_and_test",
        project_config = project_config,
        profile_config = profile_config,
        execution_config = execution_config,
        render_config = render_config,
    )

    end = EmptyOperator(task_id = "end")

    start >> choose_ingestion_path
    raw_data_ready >> transform_and_test >> end

ecommerce_dbt_fusion_duckdb_dag()
