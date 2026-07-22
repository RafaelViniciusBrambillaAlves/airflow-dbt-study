FROM astrocrpublic.azurecr.io/runtime:3.0-9

# ---------------------------------------------------------------------------
# POR QUE UM VIRTUALENV SEPARADO PARA O dbt?
# O próprio ambiente Python do Airflow tem um conjunto grande de dependências
# fixadas (providers, libs de Celery/Kubernetes etc). O dbt-core também fixa
# suas próprias versões de coisas como click, jinja2, protobuf. Instalar o dbt
# diretamente na imagem do Airflow (`pip install dbt-core dbt-duckdb`) é a
# abordagem ingênua: funciona hoje e quebra silenciosamente no dia em que
# qualquer um dos dois projetos atualizar uma dependência compartilhada. O
# ExecutionMode.VIRTUALENV do Cosmos mantém o dbt em um venv totalmente
# isolado que o Cosmos chama como subprocesso, então uma atualização do
# Airflow nunca quebra o dbt e vice-versa. O trade-off é um build de imagem
# um pouco mais pesado e um passo RUN extra aqui.
# ---------------------------------------------------------------------------

# 1) # 1) Cria o virtualenv isolado do dbt usado pelo Cosmos (ExecutionMode.VIRTUALENV)
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.9.* \
        dbt-duckdb==1.9.*

# 2) Duckdb
RUN mkdir -p /usr/local/airflow/duckdb_data

# ---------------------------------------------------------------------------
# 3) Binário do dbt Fusion (documentado apenas para fins de completude — NÃO
#    está integrado neste projeto; veja a nota sobre "dbt Fusion" no
#    README.md para entender o porquê).
#    É assim que você INSTALARIA caso migrasse para um warehouse com suporte
#    a Fusion (Snowflake, Databricks, BigQuery, Redshift-preview):
#
#    O Cosmos só orquestra o Fusion sob ExecutionMode.LOCAL, apontando
#    diretamente para o binário instalado — não existe modo virtualenv para
#    o Fusion e, até hoje, não há adapter de DuckDB para o Fusion.
# ---------------------------------------------------------------------------

# 4) Pacotes Python do lado do Airflow (o próprio Cosmos vive aqui, não no venv do dbt)
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt