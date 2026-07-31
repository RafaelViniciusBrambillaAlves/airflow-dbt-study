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
from pathlib import Path

import pandas as pd
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

RAW_SCHEMAS = ("analytics_core_raw", "analytics_fusion_raw")
DEFAULT_INGESTION_WAREHOUSES = ("duckdb", "postgres")


def _dataset_size() -> str:
    size = Variable.get(DATASET_SIZE_VAR, default = "small").strip().lower()

    if size not in ("small", "large"):
        raise ValueError(
            f"Variable {DATASET_SIZE_VAR} = '{size}' invalida. "
            "Valores aceitos: 'small' ou 'large'"
        )

    return size


def _large_n_orders() -> int:
    return int(
        Variable.get(LARGE_DATASET_N_ORDERS_VAR, default = DEFAULT_LARGE_N_ORDERS)
    )


def _seed_dataset(dbt_project_dir: str) -> dict[str, pd.DataFrame]:
    seed_dir = Path(dbt_project_dir) / "seeds"
    return {
        "raw_customers": pd.read_csv(seed_dir / "raw_customers.csv"),
        "raw_products": pd.read_csv(seed_dir / "raw_products.csv"),
        "raw_orders": pd.read_csv(seed_dir / "raw_orders.csv"),
    }


def _selected_warehouses() -> tuple[str, ...]:
    raw_value = Variable.get(
        "ecommerce_ingestion_warehouses",
        default = ",".join(DEFAULT_INGESTION_WAREHOUSES),
    )
    warehouses = tuple(
        warehouse.strip().lower()
        for warehouse in raw_value.split(",")
        if warehouse.strip()
    )
    invalid = sorted(set(warehouses) - set(DEFAULT_INGESTION_WAREHOUSES))
    if invalid:
        raise ValueError(
            "Variable ecommerce_ingestion_warehouses contem valores invalidos: "
            f"{invalid}. Use duckdb, postgres ou duckdb,postgres."
        )
    if not warehouses:
        raise ValueError("ecommerce_ingestion_warehouses nao pode ficar vazia")

    return warehouses


def load_benchmark_raw_data(
    dbt_project_dir: str,
    duckdb_path: str,
    postgres_conn_id: str,
    raw_schemas: tuple[str, ...] = RAW_SCHEMAS,
) -> None:
    """
    Carrega os dados brutos uma unica vez para os schemas consumidos pelas
    quatro DAGs de transformacao. Nao chama dbt: tanto o dataset pequeno
    quanto o grande entram pela camada de ingestao Python.
    """
    size = _dataset_size()
    warehouses = _selected_warehouses()

    if size == "small":
        dataframes = _seed_dataset(dbt_project_dir)
    else:
        dataframes = generate_dataset(n_orders = _large_n_orders())

    print(
        "[ingestao] dataset_size=%s warehouses=%s schemas=%s"
        % (size, ",".join(warehouses), ",".join(raw_schemas))
    )

    for warehouse in warehouses:
        for schema in raw_schemas:
            if warehouse == "duckdb":
                load_to_duckdb(
                    dataframes = dataframes,
                    duckdb_path = duckdb_path,
                    schema = schema,
                )
            elif warehouse == "postgres":
                load_to_postgres(
                    dataframes = dataframes,
                    conn_id = postgres_conn_id,
                    schema = schema,
                )

    for table_name, df in dataframes.items():
        print(f"[ingestao] {table_name}: {len(df)} linhas carregadas")


def _chosse_branch(**context) -> str:
    size = _dataset_size()
    
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
    n_orders = _large_n_orders()

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

