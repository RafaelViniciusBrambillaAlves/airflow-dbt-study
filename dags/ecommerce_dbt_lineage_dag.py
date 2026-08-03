"""
ecommerce_dbt_lineage_dag
--------------------------
DAG dedicada exclusivamente a demonstrar lineage via OpenLineage + Marquez.

NAO faz parte do benchmark Core/Fusion x DuckDB/Postgres. A Cosmos só emite
eventos OpenLineage nos modos LOCAL e VIRTUALENV (nao em WATCHER, usado pelas
4 DAGs de benchmark) - ver https://astronomer.github.io/astronomer-cosmos/configuration/lineage.html.
Por isso esta DAG existe separada, em ExecutionMode.LOCAL, para nao introduzir
uma segunda variavel (WATCHER vs LOCAL) no benchmark controlado.

NAO COMPARE o tempo de execucao desta DAG com as 4 DAGs de benchmark - ela
roda em modo de execucao diferente por definicao.

Usa Postgres (nao DuckDB) para poder rodar em paralelo com as DAGs de
benchmark sem disputar o lock de escritor unico do arquivo .duckdb.
Schema proprio (analytics_lineage) para nao colidir com os schemas do
benchmark.
"""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag
from cosmos import DbtTaskGroup, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

from include.ingestion.assets import ECOMMERCE_RAW_READY_ASSET

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
DBT_CORE_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR,
)

_profile_config = ProfileConfig(
    profile_name = "ecommerce",
    target_name = "dev_lineage",
    profile_mapping = PostgresUserPasswordProfileMapping(
        conn_id = "ecommerce_warehouse",
        profile_args = {"schema": "analytics_lineage", "threads": 4},
        # profile_args = {"schema": "analytics_core", "threads": 8},
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
    dag_id = "ecommerce_dbt_lineage_dag",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    max_active_tasks = 4,
    tags = ["dbt", "cosmos", "ecommerce", "lineage", "observavility"],
    doc_md = __doc__,
)

def ecommerce_dbt_lineage_dag():

    start = EmptyOperator(task_id = "start")

    transform_and_test = DbtTaskGroup(
        group_id = "transform_and_test",
        project_config = _project_config,
        profile_config = _profile_config,
        execution_config = _execution_config,
        render_config = _render_config
    )

    end = EmptyOperator(task_id = "end")

    start >> transform_and_test >> end

ecommerce_dbt_lineage_dag()



