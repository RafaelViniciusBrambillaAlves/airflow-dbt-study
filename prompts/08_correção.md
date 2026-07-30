# Prompt: Correção de 3 problemas — RenderConfig inconsistente, conexão Postgres e carga com pandas

Tenho 3 problemas concretos, com erro reproduzido em cada um. Quero a causa raiz de cada um
antes de qualquer correção, e depois a correção aplicada de forma consistente nas 4 DAGs —
elas devem ficar o mais parecidas possível, variando só motor dbt (Core/Fusion) e banco
(DuckDB/Postgres), nada mais.

## Problema 1 — `RenderConfig` diferente em cada DAG e erro de `InvocationMode`

Hoje cada DAG está configurada de um jeito:

```python
# dbt_core_duckdb_dag.py
render_config = RenderConfig(
    select=["path:models"],
    test_behavior="after_each",
    load_method=LoadMode.DBT_LS,
    dbt_executable_path=DBT_CORE_EXECUTABLE,
)

# dbt_core_postgres_dag.py
render_config = RenderConfig(
    select=["path:models"],
    test_behavior="after_each",
    load_method=LoadMode.DBT_LS,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_CORE_EXECUTABLE,
)

# dbt_fusion_duckdb_dag.py
render_config = RenderConfig(
    select=["path:models"],
    test_behavior="after_each",
    load_method=LoadMode.DBT_LS,
    dbt_executable_path=DBT_FUSION_EXECUTABLE,
)

# dbt_fusion_postgres_dag.py
render_config = RenderConfig(
    select=["path:models"],
    test_behavior="after_each",
    load_method=LoadMode.DBT_LS,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_FUSION_EXECUTABLE,
)
```

A `dbt_core_duckdb_dag.py` (sem `invocation_mode` explícito) quebra com:

```
cosmos.exceptions.CosmosValueError: RenderConfig.dbt_executable_path is set, but it is not
the same as the system dbt executable path. Do not set render_config.dbt_executable_path
when using InvocationMode.DBT_RUNNER.
```

- Investigar por que o Cosmos assume `InvocationMode.DBT_RUNNER` por padrão quando
  `invocation_mode` não é passado, e por que isso é incompatível com apontar
  `dbt_executable_path` para um executável diferente do dbt instalado no Python do Airflow
  (ler a validação em `cosmos/converter.py` para confirmar a regra exata).
- Definir **um único padrão de `RenderConfig`** para as 4 DAGs, explicitando o
  `invocation_mode` correto (provavelmente `SUBPROCESS`, já que usamos executáveis
  diferentes — Core, Fusion — que não são o dbt "do sistema" do Python do Airflow) e
  aplicar exatamente o mesmo bloco nas 4 DAGs, mudando apenas a constante do executável
  (`DBT_CORE_EXECUTABLE`/`DBT_FUSION_EXECUTABLE`).
- Explicar a diferença real entre `InvocationMode.DBT_RUNNER` e `InvocationMode.SUBPROCESS`
  (import direto em processo vs. chamada de processo externo) e por que `SUBPROCESS` é a
  escolha coerente para este projeto, que depende de trocar o executável do dbt entre DAGs.

## Problema 2 — Conexão com Postgres só funciona com `host.docker.internal`

Configuração que funciona:
```yaml
conn_host: host.docker.internal
conn_port: 5433
```

Configuração que deveria funcionar mas não funciona (usando o nome do serviço Docker):
```yaml
conn_host: postgres-warehouse
conn_port: 5432
```
Erro:
```
could not translate host name "postgres-warehouse" to address: Name or service not known
```

`docker ps` mostra os containers do Airflow (scheduler, dag-processor, api-server, triggerer)
e dois Postgres: `airflow-dbt-study_f4057d-postgres-warehouse-1` (porta 5433:5432, o nosso
warehouse) e `airflow-dbt-study_f4057d-postgres-1` (porta 5432, esse é o metastore do
próprio Airflow — não confundir os dois).

- Investigar se os containers do Airflow e o `postgres-warehouse` estão na **mesma rede
  Docker** — resolução de nome por nome de serviço só funciona dentro da mesma rede
  Docker Compose; se o `postgres-warehouse` foi definido em `docker-compose.override.yml`
  sem herdar a rede que o Astro CLI cria para os outros serviços, é essa a causa raiz.
- Confirmar isso rodando algo como `docker network inspect` nas redes envolvidas e/ou
  `docker inspect` do container do warehouse, verificando a rede em que ele está.
- Corrigir o `docker-compose.override.yml` para o `postgres-warehouse` entrar na mesma rede
  dos demais serviços do Astro, permitindo usar `conn_host: postgres-warehouse` e
  `conn_port: 5432` (porta interna do container, não a porta mapeada no host) — que é a
  forma correta e recomendada, em vez de depender de `host.docker.internal`
  (que funciona mas contorna a rede Docker interna e mistura porta de host com porta de
  container, sendo mais frágil e não é a prática recomendada para comunicação
  container-a-container).
- Atualizar a Connection (`ecommerce_warehouse`) e o `.env.example`/`airflow_settings.yaml.example`
  para refletir a configuração corrigida.

## Problema 3 — Falha ao carregar dataset grande no Postgres via pandas

Erro:
```
AttributeError: 'Engine' object has no attribute 'cursor'
```
com o warning antes dele:
```
UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database
string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested.
```
Ocorre em `load_raw_data.py`, linha 87, dentro de `load_to_postgres`, na chamada
`DataFrame.to_sql(...)`.

- Ler o `load_to_postgres` atual e identificar exatamente o que está sendo passado no
  parâmetro `con=` do `to_sql` — provável causa: está sendo passado algo que mistura um
  objeto SQLAlchemy `Engine` com uma chamada que espera uma conexão DBAPI2 pura (ou
  vice-versa), o que é incompatível com como o `PostgresHook` do Airflow expõe a conexão.
- Corrigir usando a forma recomendada: `PostgresHook.get_sqlalchemy_engine()` passado
  diretamente como `con=` do `to_sql` (essa é a via suportada oficialmente pelo pandas),
  não uma conexão DBAPI2 obtida via `get_conn()` nem uma mistura dos dois.
- Confirmar que a mesma correção não quebra o caminho do DuckDB (`load_to_duckdb`), já que a
  lib `duckdb` tem sua própria forma de interagir com pandas — não aplicar a mesma solução
  do Postgres ali se não for o padrão correto para DuckDB.
- Depois de corrigir, rodar a carga do dataset grande (~10.000 linhas) no Postgres de ponta a
  ponta e confirmar que os dados batem em contagem com o que foi gerado.

## Formato de entrega
1. Causa raiz de cada um dos 3 problemas, separadamente, com evidência (trecho de código,
   documentação ou comportamento observado).
2. Correção de cada um, com diff.
3. `RenderConfig` final, idêntico nas 4 DAGs a menos da constante do executável — mostrar
   as 4 versões lado a lado para eu confirmar visualmente que só isso muda.
4. Confirmação de que os 4 cenários (Core/Fusion × DuckDB/Postgres) rodam sem erro depois das
   correções, incluindo o caminho de dataset grande no Postgres.

## Restrição
Não aplicar `host.docker.internal` como solução definitiva do problema 2 — é o workaround que
já sei que funciona; preciso da correção real (rede Docker compartilhada) explicada e
aplicada, a menos que exista uma razão técnica concreta para não ser possível, e nesse caso
explicar por que antes de manter o workaround.