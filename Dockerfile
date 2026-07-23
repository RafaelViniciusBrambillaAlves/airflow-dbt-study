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
# 3) dbt FUSION — motor em Rust, instalado via binário oficial (não é pip
# install). Coexiste com o dbt Core (venv acima) no mesmo container: são
# dois executáveis completamente separados, cada DAG aponta pro seu via
# ExecutionConfig.dbt_executable_path. Licença ELv2 (open-source com
# restrição comercial específica) — ok para uso de estudo/interno.
# Suporte confirmado pelo Cosmos desde 1.11.0a1, só com ExecutionMode.LOCAL.
# ---------------------------------------------------------------------------
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
ENV SHELL=/bin/bash
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update && \
    mv /root/.local/bin/dbt /usr/local/bin/dbt && \
    chmod +x /usr/local/bin/dbt
USER astro


# 4) Pacotes Python do lado do Airflow (o próprio Cosmos vive aqui, não no venv do dbt)
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
