"""
ecommerce_dbt_fusion_dag
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
"""

from datetime import datetime

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.operators.local import DbtSeedLocalOperator

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"

# ---------------------------------------------------------------------------
# Binário do dbt Fusion instalado via install.sh no Dockerfile — NÃO é o
# mesmo executável da DAG do Core (que usa /usr/local/airflow/dbt_venv/bin/dbt).
# CONFIRME esse caminho após o build: `astro dev bash` -> `which dbt` (com
# o venv do Core desativado) ou `find / -iname dbt -type f 2>/dev/null`.
# ---------------------------------------------------------------------------
# DBT_FUSION_EXECUTABLE = "/home/astro/.local/bin/dbt"
DBT_FUSION_EXECUTABLE = "/usr/local/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR
)

# Mesmo profiles.yml do Core, mas target_name diferente -> schema isolado
# (analytics_fusion_*), evitando qualquer conflito com a DAG do Core.
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
    # dbt_executable_path = DBT_FUSION_EXECUTABLE, # também precisa ser explícito aqui, senão o parsing usa outro dbt
    load_method = LoadMode.DBT_LS,
)

@dag(
    dag_id = "ecommerce_dbt_fusion_dag",
    start_date = datetime(2026, 1, 1),
    schedule = "@daily",
    catchup = False,
    max_active_tasks = 1, # DuckDB não paraleliza escrita - mesma restrição da DAG do Core
    tags = ["dbt", "cosmos", "ecommerce", "fusion"],
    doc_md = __doc__,
)

def ecommerce_dbt_fusion_dag():

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
        render_config = render_config,
    )

    end = EmptyOperator(task_id = "end")

    start >> load_raw_data >> transform_and_test >> end

ecommerce_dbt_fusion_dag()
