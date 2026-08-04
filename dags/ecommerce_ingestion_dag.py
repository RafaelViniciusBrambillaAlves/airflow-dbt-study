"""
ecommerce_ingestion_dag
-----------------------
Loads the raw benchmark dataset independently from dbt Core/Fusion.

The DAG reads Airflow Variables to choose the dataset volume:
  - ecommerce_dataset_size: "small" or "large"
  - ecommerce_large_dataset_n_orders: row count for large raw_orders
  - ecommerce_ingestion_warehouses: duckdb, postgres, or duckdb,postgres

On success it emits ECOMMERCE_RAW_READY_ASSET, which schedules the four
dbt transformation DAGs. Those DAGs can still be triggered manually when
the raw data is already loaded and only the dbt benchmark should be rerun.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.ingestion.airflow_tasks import load_benchmark_raw_data
from include.ingestion.assets import ECOMMERCE_RAW_READY_ASSET

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
POSTGRES_CONN_ID = "ecommerce_warehouse"
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/usr/local/airflow/duckdb_data/ecommerce.duckdb")


with DAG(
    dag_id="ecommerce_ingestion_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "ingestion", "benchmark"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(taks_id="start")

    load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_benchmark_raw_data,
        op_kwargs={
            "dbt_project_dir": DBT_PROJECT_DIR,
            "duckdb_path": DUCKDB_PATH,
            "postgres_conn_id": POSTGRES_CONN_ID,
        },
    )

    raw_data_ready = EmptyOperator(
        task_id="raw_data_ready",
        outlets=[ECOMMERCE_RAW_READY_ASSET],  # produtor
    )

    end = EmptyOperator(task_id="end")

    start >> load_raw_data >> raw_data_ready >> end
