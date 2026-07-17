FROM astrocrpublic.azurecr.io/runtime:3.0-9

# ---------------------------------------------------------------------------
# WHY A SEPARATE dbt VIRTUALENV?
# Airflow's own Python environment has a large, pinned dependency set
# (providers, Celery/Kubernetes libs, etc). dbt-core also pins its own
# versions of things like click, jinja2, protobuf. Installing dbt directly
# into the Airflow image (`pip install dbt-core dbt-duckdb`) is the naive
# approach: it works today and breaks silently the day either project
# bumps a shared dependency. Cosmos's ExecutionMode.VIRTUALENV keeps dbt in
# a fully isolated venv that Cosmos calls as a subprocess, so an Airflow
# upgrade can never break dbt and vice-versa. The trade-off is a slightly
# heavier image build and one extra RUN step here. 
# ---------------------------------------------------------------------------

# 1) Build the isolated dbt virtualenv used by Cosmos (ExecutionMode.VIRTUALENV)
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.9.* \
        dbt-duckdb==1.9.*

# ---------------------------------------------------------------------------
# 2) dbt Fusion binary (documented for completeness — NOT wired into this
#    project; see the "dbt Fusion" note in README.md for why).
#    This is how you WOULD install it if you moved to a Fusion-supported
#    warehouse (Snowflake, Databricks, BigQuery, Redshift-preview):
#
#    USER root
#    RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
#    USER astro
#
#    Cosmos only orchestrates Fusion under ExecutionMode.LOCAL, pointing
#    straight at the installed binary — there is no Fusion virtualenv mode
#    and, as of today, no DuckDB adapter for Fusion.
# ---------------------------------------------------------------------------

# 3) Airflow-side Python packages (Cosmos itself lives here, not in the dbt venv)
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt