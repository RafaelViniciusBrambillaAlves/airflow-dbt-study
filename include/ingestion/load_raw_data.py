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

import io

import duckdb
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


# Mapeando pandas dtype -> tipo Postgres
def _pandas_dtype_to_pg(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"


def _build_create_table_sql(schema: str, table: str, df: pd.DataFrame) -> str:
    cols = ", ".join(f'"{col}" {_pandas_dtype_to_pg(dtype)}' for col, dtype in df.dtypes.items())
    return f'CREATE TABLE "{schema}"."{table}" ({cols})'


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
            conn.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df_tmp")
            conn.unregister("df_tmp")
    finally:
        conn.close()


def load_to_postgres(dataframes: dict[str, pd.DataFrame], conn_id: str, schema: str) -> None:
    """
    Carrega DataFrames para PostgreSQL de forma idempotente e eficiente.

    Etapa 1 - via SQLAlchemy, em uma única transação:
        - CREATE SCHEMA IF NOT EXISTS
        - DROP TABLE IF EXISTS ... CASCADE
        - CREATE TABLE com tipos mapeados dos dtypes do DataFrame

    Etapa 2 - via psycopg2 + COPY FROM STDIN:
        - copy_expert() com StringIO como buffer
        - transação única cobrindo todas as tabelas (commit só no final)
        - rollback automático em caso de erro
    """
    # Toda a logica de conexao do Postgres usando o Connection do Airflow
    hook = PostgresHook(postgres_conn_id=conn_id)
    # Objeto SQLAlchemy Engine
    engine = hook.get_sqlalchemy_engine()

    # Etapa 1: DDL (CREATE SCHEMA / DROP / CREATE TABLE)
    with engine.begin() as conn:  # commit automatico / rollback
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        for table_name, df in dataframes.items():
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE'))

            conn.execute(text(_build_create_table_sql(schema, table_name, df)))

    # Etapa 2: Carga via COPY FROM STDIN
    # Usa a conexão DBAPI2 bruta, que é o caminho oficial do psycopg2 para
    # COPY. Abrimos UMA conexão e mantemos a transação explícita.
    pg_conn = hook.get_conn()
    try:
        with pg_conn.cursor() as cursor:
            for table_name, df in dataframes.items():
                buffer = io.StringIO()

                df.to_csv(buffer, index=False, header=False, na_rep="")

                buffer.seek(0)

                cursor.copy_expert(
                    f'COPY "{schema}"."{table_name}" ' f"FROM STDIN WITH CSV NULL AS ''", buffer
                )
                print(
                    f"[ingestao] {schema}.{table_name}: " f"{len(df)} linhas carregadas via COPY."
                )
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise

    finally:
        pg_conn.close()
