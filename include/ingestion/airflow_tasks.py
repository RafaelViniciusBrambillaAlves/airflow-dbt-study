"""
include/ingestion/airflow_tasks.py
-------------------------------------
Tasks de ingestao reutilizadas pelas 4 DAGs de benchmark
(core/fusion x duckdb/postgres), para evitar duplicar 4 DAGs em 8 so para
suportar dois volumes de dado.
 
Decide, EM RUNTIME, atraves da Airflow Variable `ecommerce_dataset_size`
(valores aceitos: "small" | "large", default "small") qual caminho de
carga usar:
 
  - small -> dbt seed (comportamento ORIGINAL, sem nenhuma alteracao:
    carrega os CSVs versionados em seeds/, uteis para iterar rapido e
    para CI).
  - large -> gera dados fake via Faker (generate_fake_data.py) e carrega
    DIRETO nas tabelas raw_* do warehouse (load_raw_data.py), sem passar
    pelo dbt seed - ver a justificativa de por que seeds nao sao
    adequados para ~10.000.000 linhas na docstring de generate_fake_data.py.
 
Configuracao (Airflow Variables, ver airflow_settings.yaml):
  - ecommerce_dataset_size: "small" (default) ou "large"
  - ecommerce_large_dataset_n_orders: quantidade de linhas de raw_orders
    a gerar quando dataset_size="large" (default 10000000)
 
IMPORTANTE PARA A COMPARACAO DE BENCHMARK: o volume usado em cada
execucao fica registrado nos logs da task `choose_ingestion_path` e no
nome da task efetivamente executada (load_raw_seed vs load_raw_large) -
sempre confira qual das duas rodou antes de comparar tempos entre runs.
"""

# from airflow.models import Variable
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from include.ingestion.generate_fake_data import generate_dataset
from include.ingestion.load_raw_data import load_to_duckdb, load_to_postgres

DATASET_SIZE_VAR = "ecommerce_dataset_size"
LARGE_DATASET_N_ORDERS_VAR = "ecommerce_large_dataset_n_orders"
DEFAULT_LARGE_N_ORDERS = 10_000_000

SEED_TASK_ID = "load_raw_seed"
LARGE_TASK_ID = "load_raw_large"


def _chosse_branch(**context) -> str:
    size = Variable.get(DATASET_SIZE_VAR, default_var = "small").strip().lower()

    if size not in ("small", "large"):
        raise ValueError(
            f"Variable {DATASET_SIZE_VAR} = '{size}' invalida"
            "Valores aceitos: 'small' ou 'large'"
        )
    
    print(f"[ingestao] ecommerce_dataset_size='{size}' -> "
          f"branch escolhido: {SEED_TASK_ID if size == 'small' else LARGE_TASK_ID}")
    return SEED_TASK_ID if size == "small" else LARGE_TASK_ID


def _load_large_dataset(
    schema: str,
    warehouse: str,
    duckdb_path: str | None = None,
    postgres_conn_id: str | None = None,
    **context,
) -> None:
    n_orders = int(
        Variable.get(LARGE_DATASET_N_ORDERS_VAR, default_var = DEFAULT_LARGE_N_ORDERS)
    )

    print(f"[ingestao] gerando dataset fake com n_orders = {n_orders}"
          f"-> schema = {schema} (warehouse = {warehouse})")
    dataframes = generate_dataset(n_orders = n_orders)

    if warehouse == "duckdb":
        if not duckdb_path:
            raise ValueError("duckdb_path e obrigatorio quando warehouse == 'duckdb'")
        load_to_duckdb(\
            dataframes = dataframes, 
            duckdb_path = duckdb_path, 
            schema = schema
        )

    elif warehouse == "postgres":
        if not postgres_conn_id:
            raise ValueError("postgres_conn_id e obrigatorio quando warehouse = 'postgres'")
        load_to_postgres(
            dataframes= dataframes,
            conn_id = postgres_conn_id,
            schema = schema 
        )
        
    else:
        raise ValueError("warehouse desconhecido: '{warehouse}' (use 'duckdb' ou 'postgres')")

    for table_name, df in dataframes.items():
        print(f"[ingestao] {schema}.{table_name}: {len(df)} linhas carregadas")


def build_ingestion_branch(
    seed_operator,
    schema: str, 
    warehouse: str,
    duckdb_path: str | None = None,
    postgres_conn_id: str | None = None
):
    """
    Monta o trecho `choose_ingestion_path -> [load_raw_seed | load_raw_large]
    -> raw_data_ready` da DAG.
 
    `seed_operator` precisa ser a task de dbt seed ja instanciada com
    task_id="load_raw_seed" (comportamento "small", sem alteracoes).
 
    Retorna (choose_task, raw_data_ready_task): o chamador conecta
    `start >> choose_task` e `raw_data_ready >> transform_and_test`.
    """

    if seed_operator.task_id != SEED_TASK_ID:
        raise ValueError(
            f"seed_operator.task_id deve ser '{SEED_TASK_ID}' para casar"
            f"com o branch (recebido: '{seed_operator.task_id}')"
        )

    choose = BranchPythonOperator(
        task_id = "choose_ingestion_path",
        python_callable = _chosse_branch,
    )

    load_large = PythonOperator(
        task_id = LARGE_TASK_ID,
        python_callable = _load_large_dataset,
        op_kwargs = {
            "schema": schema,
            "warehouse": warehouse,
            "duckdb_path": duckdb_path,
            "postgres_conn_id": postgres_conn_id,
        },
    ) 

    # none_failed_min_one_success: segue em frente assim que UM dos dois
    # ramos (o que nao foi pulado pelo branch) terminar com sucesso.
    raw_data_ready = EmptyOperator(
        task_id = "raw_data_ready",
        trigger_rule = "none_failed_min_one_success",
    )

    choose >> [seed_operator, load_large]
    [seed_operator, load_large] >> raw_data_ready

    return choose, raw_data_ready