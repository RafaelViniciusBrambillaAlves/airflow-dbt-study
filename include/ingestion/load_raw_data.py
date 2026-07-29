"""
include/ingestion/load_raw_data.py
------------------------------------
Carrega os DataFrames gerados por generate_fake_data.py DIRETO nas tabelas
raw_customers / raw_products / raw_orders do warehouse de destino, sem
passar pelo `dbt seed`. Isso é o que torna esse caminho uma ingestao de
verdade, e nao um seed disfarcado (ver docstring de generate_fake_data.py).
 
Cada funcao recebe o schema fisico exato que o dbt vai ler via
`source('raw', ...)` - o mesmo schema calculado por
`"{{ target.schema }}_raw"` em models/staging/_staging__sources.yml.
Ex: target dev_core -> schema analytics_core -> schema_raw = analytics_core_raw.
 
Suporta os dois warehouses do projeto:
  - DuckDB: escreve direto no arquivo .duckdb via biblioteca duckdb (mesmo
    arquivo apontado por DUCKDB_PATH/profiles.yml).
  - Postgres: escreve via a mesma Airflow Connection (`ecommerce_warehouse`)
    ja usada pelo Cosmos/PostgresUserPasswordProfileMapping, atraves do
    PostgresHook.
"""

import os
import pandas as pd

import duckdb 
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_to_duckdb(dataframes: dict[str, pd.DataFrame], duckdb_path: str, schema: str) -> None:
    """
    Grava os DataFrames como tabelas no arquivo DuckDB indicado.
    CREATE OR REPLACE TABLE garante idempotencia: rodar de novo com o mesmo
    dataset (ou um dataset diferente) sempre deixa a tabela consistente com
    o DataFrame gerado nesta execucao, sem acumular linhas de runs antigos.
 
    Usa uma unica conexao para todas as tabelas, respeitando o lock de
    escritor unico do DuckDB (mesma restricao ja documentada nas DAGs de
    DuckDB, max_active_tasks=1).
    """
    conn = duckdb.connect(duckdb_path)
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table_name, df in dataframes.items():
            conn.register("df_tmp", df)
            conn.execute(
                F"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df_tmp"
            )
            conn.unregister("df_tmp")
    finally:
        conn.close()


def load_to_postgres(dataframes: dict[str, pd.DataFrame], conn_id: str, schema: str) -> None:
    """
    Grava os DataFrames como tabelas no schema indicado do Postgres, usando
    a Connection do Airflow ja configurada (mesmo conn_id usado pelo
    PostgresUserPasswordProfileMapping do Cosmos).
    """
    hook = PostgresHook(postgres_conn_id = conn_id)

    pg_conn = hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
        for table_name, df in dataframes.items():

            engine = hook.get_sqlalchemy_engine()
            df.head(0).to_sql(table_name, con = engine, schema = schema, if_exists = "replace", index = False)

            tmp_csv_path = f"/tmp/{table_name}_bulk.csv"
            df.to_csv(tmp_csv_path, index = False, header = False)

            with open(tmp_csv_path, 'r') as f:
                cursor.copy_expert(f'COPY "{schema}"."{table_name}" FROM STDIN WITH CSV', f)

            os.remove(tmp_csv_path)

            print(f"[ingestao] {schema}.{table_name}: {len(df)} linhas carregadas via COPY.")

        pg_conn.commit()

    except Exception as e:
        pg_conn.rollback()
        raise e

    finally:
        cursor.close()
        pg_conn.close()