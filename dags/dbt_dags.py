"""
ecommerce_dbt_dag
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
"""

import os
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode
from cosmos.operators.local import DbtSeedLocalOperator

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/ecommerce"
# DBT_VENV_PYTHON = "/usr/local/airflow/dbt_venv/bin/python"
DBT_VENV_PATH = "/usr/local/airflow/dbt_venv"
DBT_EXECUTABLE_PATH = f"{DBT_VENV_PATH}/bin/dbt"

# ---------------------------------------------------------------------------
# ProjectConfig: tells Cosmos WHERE the dbt project lives and where seeds/
# models/etc. are, inside the Airflow worker's filesystem (this is the same
# `include/dbt/ecommerce` folder from the repo, mounted into the container
# by the Astro CLI).
# ---------------------------------------------------------------------------
project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR
)

# ---------------------------------------------------------------------------
# ProfileConfig: tells Cosmos HOW to connect to the warehouse.
#
# Here we point at the profiles.yml checked into the dbt project, because
# DuckDB has no username/password/host for an Airflow Connection to hold —
# see the comment in profiles.yml for why.
#
# If/when this project is pointed at Postgres (see README "how this
# scales"), the honest, credentials-in-Airflow-Connections way to do this
# is a profile_mapping instead of profiles_yml_filepath, e.g.:
#
#   from cosmos.profiles import PostgresUserPasswordProfileMapping
#   ProfileConfig(
#       profile_name="ecommerce",
#       target_name="dev",
#       profile_mapping=PostgresUserPasswordProfileMapping(
#           conn_id="ecommerce_warehouse",  # an Airflow Connection, not a secret in git
#           profile_args={"schema": "analytics"},
#       ),
#   )
#
# That's the pattern this project intentionally avoids hardcoding around,
# since it doesn't apply to a file-based database like DuckDB.
# ---------------------------------------------------------------------------
profile_config = ProfileConfig(
    profile_name = "ecommerce",
    target_name = "dev",
    profiles_yml_filepath = f"{DBT_PROJECT_DIR}/profiles.yml"
)

# ---------------------------------------------------------------------------
# ExecutionConfig: HOW each dbt command actually runs.
#
# ExecutionMode.VIRTUALENV points Cosmos at the isolated dbt venv built in
# the Dockerfile, so dbt's dependencies never touch Airflow's. The naive
# alternative, ExecutionMode.LOCAL with dbt installed straight into the
# Airflow image, is simpler to set up but risks a dependency clash the
# moment either Airflow or dbt-core bumps a shared library — exactly the
# failure mode this project is trying to model good practice around.
# ---------------------------------------------------------------------------
execution_config = ExecutionConfig(
    # execution_mode = ExecutionMode.VIRTUALENV,
    execution_mode = ExecutionMode.LOCAL,
    # virtualenv_dir = os.path.dirname(DBT_VENV_PYTHON.replace("/bin/python", ""))
    dbt_executable_path = DBT_EXECUTABLE_PATH,
)

# ---------------------------------------------------------------------------
# RenderConfig: WHICH parts of the dbt graph become Airflow tasks, and how
# they're selected. We exclude seeds here because seeds are handled by a
# single explicit `dbt seed` task upstream (see below) rather than being
# scattered as individual tasks — seeds represent "load raw data", which is
# a different concern from "transform data" and reads more clearly as one
# gate the model tasks wait on.
# ---------------------------------------------------------------------------
render_config = RenderConfig(
    select = ["path:models"],
    test_behavior = "after_each"  # a model's tests run immediately after it, not all at the end
)

@dag(
    dag_id = "ecommerce_dbt_dag",
    start_date = datetime(2026, 1, 1),
    schedule = "@daily",
    catchup = False,
    max_active_tasks = 1, # No paralelism
    tags = ["dbt", "cosmos", "ecommerce"],
    doc_md = __doc__,
)

def ecommerce_dbt_dag():
    start = EmptyOperator(
        task_id = "start"
    )

    # Explicit seed-loading step. In a real pipeline this task would instead
    # be an Airbyte/Fivetran sensor or trigger — seeding here just stands in
    # for "raw data has landed."
    load_raw_data = DbtSeedLocalOperator(
        task_id = "load_raw_data",
        project_dir = DBT_PROJECT_DIR,
        profile_config = profile_config,
        # py_system_site_packages = False,
        # py_requirements = ["dbt-core==1.9.*", "dbt-duckdb==1.9.*"]
        # invocation_mode = InvocationMode.SUBPROCESS,  
        
    )

    # -----------------------------------------------------------------
    # DbtTaskGroup vs DbtDag:
    # DbtTaskGroup embeds the dbt project as a TaskGroup INSIDE a larger,
    # hand-written DAG — the right call whenever dbt is one stage in a
    # bigger pipeline (as here: seed -> transform -> [future: export]).
    # DbtDag generates a whole standalone DAG straight from the dbt
    # project and is the right call when dbt IS the entire pipeline with
    # nothing upstream/downstream to model in Airflow. Since this project
    # has an explicit upstream seed step, DbtTaskGroup is the correct
    # choice here.
    # -----------------------------------------------------------------
    transform_and_test = DbtTaskGroup(
        group_id = "transform_and_test",
        project_config = project_config,
        profile_config = profile_config,
        execution_config = execution_config,
        render_config = render_config
    )
    
    end = EmptyOperator(
        task_id = "end"
    )

    start >> load_raw_data >> transform_and_test >> end

ecommerce_dbt_dag()
