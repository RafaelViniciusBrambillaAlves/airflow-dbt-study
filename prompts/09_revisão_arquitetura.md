# Prompt: Redesenho de arquitetura para benchmark real — ingestão separada, agrupamento de tasks e threads

### Roteamento de Agents (Astronomer)
Para garantir que as validações e implementações sigam as melhores práticas e a documentação oficial, utilize os agents especializados da Astronomer disponíveis no seu ambiente para as respectivas etapas:

- **Validação de Cosmos/dbt:** Use os agents `cosmos-dbt-core` e `cosmos-dbt-fusion` para confirmar o comportamento do LoadMode.DBT_LS, alternativas de agrupamento (como DbtRunLocalOperator puro) e configuração de threads por adapter.
- **Validação de Airflow 3:** Use os agents `airflow` e `migrating-airflow-2-to-3` para confirmar se Datasets e DatasetAlias são a forma recomendada e não deprecada no Airflow 3 (baseado nos logs: Assigning outlets with DatasetAlias in Airflow 3).
- **Autoração de DAGs:** Use o agent `authoring-dags` para estruturar o diff das novas DAGs, garantindo uso correto de TaskFlow API e context managers.
- **Testes e Debug:** Use `testing-dags` caso precise sugerir como validar o disparo via Dataset localmente.

## Contexto e problema
Comparei as 4 combinações (Core/Fusion × DuckDB/Postgres) em dois volumes de dado:

**Dataset ínfimo (<10 linhas por seed):**
- `ecommerce_dbt_core_duckdb_dag` — 00:01:10
- `ecommerce_dbt_fusion_duckdb_dag` — 00:00:33
- `ecommerce_dbt_core_postgres_dag` — 00:00:37
- `ecommerce_dbt_fusion_postgres_dag` — 00:00:15

**Dataset grande (~10.000.000 linhas):**
- `ecommerce_dbt_core_duckdb_dag` — 00:02:13
- `ecommerce_dbt_fusion_duckdb_dag` — 00:01:21
- `ecommerce_dbt_core_postgres_dag` — 00:01:49
- `ecommerce_dbt_fusion_postgres_dag` — 00:01:47

Os números não fazem sentido para mim: com volume grande, Core e Fusion ficam quase iguais
no Postgres, e o DuckDB não aparece mais rápido que o Postgres mesmo usando Fusion nos dois.
Não sei se o problema é a forma como gero os dados, como conecto ao banco, ou se preciso de
mais volume ainda para ver diferença real.

Log detalhado de uma execução (Postgres, Core vs Fusion), para referência:

```
ecommerce_dbt_core_postgres_dag
  load_raw_large (PythonOperator)      00:00:58
  choose_ingestion_path                2.76s
  stg_customers_run                    9.92s
  stg_products_run                     9.92s
  stg_orders_run                       00:00:10
  int_orders_enriched_run              00:00:00 (Empty)
  dim_products_run                     9.61s
  dim_customers_run                    00:00:20
  fct_orders_run                       00:00:32

ecommerce_dbt_fusion_postgres_dag
  load_raw_large (PythonOperator)      00:01:06
  choose_ingestion_path                2.28s
  stg_customers_run                    8.64s
  stg_products_run                     9.02s
  stg_orders_run                       8.82s
  int_orders_enriched_run              00:00:00 (Empty)
  dim_products_run                     4.18s
  dim_customers_run                    00:00:15
  fct_orders_run                       00:00:27
```

## Hipótese recebida (de outra análise, não validada ainda)
Recebi uma análise externa sugerindo que o problema é arquitetural, não dos dados:

1. A ingestão (`load_raw_large`, gerar + inserir os dados) está dentro da própria DAG do dbt
   e consome boa parte do tempo total — isso não deveria fazer parte do benchmark de
   Core/Fusion nem de DuckDB/Postgres.
2. O `RenderConfig` atual (`load_method = LoadMode.DBT_LS`) faz o Cosmos criar **uma task do
   Airflow por modelo dbt**, e cada task sobe um subprocesso novo (parse do manifest, conexão
   ao banco, etc.) — isso seria "overhead fixo" mascarando a velocidade real do motor/banco,
   especialmente visível nos ~9s de cada task de staging para um `SELECT` que no banco deveria
   levar frações de segundo.
3. `threads` no `profiles.yml`/`ProfileConfig` talvez não estejam configurados de forma ótima
   por banco (Postgres se beneficiaria de mais threads em paralelo; DuckDB precisaria de
   `threads: 1` por causa do lock de escritor único).
4. Sugestão de solução: separar a ingestão em uma **DAG própria** (`ecommerce_ingestion_dag`),
   comunicando com a DAG do dbt via **Airflow Dataset** (orquestração orientada a dado, não a
   horário); e agrupar as transformações dbt em **menos tasks** (um único `DbtRunLocalOperator`
   rodando o projeto inteiro, ou agrupado por camada/tag) em vez de uma task por modelo.

**Não quero implementar essa hipótese às cegas.** Quero que você valide cada afirmação contra
o comportamento real do projeto, a documentação oficial do Cosmos/Airflow/dbt, e só então
decidir o que faz sentido implementar.

## O que preciso

### 1. Validar a hipótese
- Confirmar (com evidência: código do Cosmos, documentação oficial, ou teste no próprio
  projeto) se `LoadMode.DBT_LS` realmente gera uma task por modelo, e se existe alternativa
  suportada oficialmente para rodar em lote (um único operator para todo o projeto ou por
  seleção/tag), e o nome exato dessa configuração no Cosmos (não assumir sintaxe).
- Confirmar se o overhead observado por task (~9s) é mesmo dominado por subida de
  processo/parse de manifest, medindo isso separadamente (ex: rodar `dbt run` manualmente
  fora do Airflow e comparar o tempo de start-up vs. execução do SQL).
- Confirmar a recomendação de `threads: 1` para DuckDB e `threads` mais altos para Postgres
  com base na documentação oficial dos adapters, não só na explicação recebida.
- Avaliar se Airflow Datasets é de fato a forma recomendada e atual (não deprecada) de
  encadear DAGs por dependência de dado no Airflow 3, já que o projeto está nessa versão
  (os logs mostram `Assigning outlets with DatasetAlias in Airflow 3`).

### 2. Separar ingestão da transformação
- Se validado, criar `ecommerce_ingestion_dag` responsável só por gerar/carregar os dados
  brutos (reaproveitando `include/ingestion/`), sem nenhuma dependência de motor
  dbt/Core/Fusion.
- As 4 DAGs de transformação passam a ser disparadas pela conclusão da ingestão (Dataset),
  não mais rodando a ingestão internamente.
- Manter a opção de rodar cada DAG de transformação isoladamente (sem esperar a ingestão)
  para os cenários em que os dados já estão carregados e eu só quero re-rodar o benchmark do
  dbt — explicar como isso fica coerente com o modelo orientado a Dataset.

### 3. Reagrupar as tasks do dbt
- Reconfigurar o `RenderConfig` das 4 DAGs para rodar o projeto (ou cada camada:
  staging/intermediate/marts) como blocos maiores, reduzindo o número de subprocessos, e
  medir o impacto real disso no tempo total.
- Se for melhor manter granularidade por task para efeitos de observabilidade
  (falha isolada por modelo, retry granular), apresentar esse trade-off e não decidir sozinho
  — isso é uma escolha de projeto que eu preciso confirmar, já que benchmark "puro" e
  "observabilidade granular em produção" podem pedir configurações diferentes.

### 4. Ajustar `threads` por banco/engine
- Configurar `threads` adequados no target Postgres (paralelismo real) e forçar
  `threads: 1` no target DuckDB, com a justificativa documentada.

### 5. Metodologia de benchmark justa
- Definir um protocolo de medição: dataset já carregado antes de medir (ingestão não entra
  no tempo comparado), mesmo número de execuções, cache do dbt limpo/quente de forma
  consistente entre os 4 cenários, e reportar separadamente: tempo de ingestão (medido uma
  vez, fora da comparação) vs. tempo de transformação (a métrica que de fato queremos
  comparar entre Core/Fusion e DuckDB/Postgres).
- Rodar o benchmark novamente nos dois volumes de dado (pequeno e grande) com a nova
  arquitetura e reportar os números lado a lado com os anteriores.

## Formato de entrega
1. Validação de cada ponto da hipótese, com evidência — dizer explicitamente onde a análise
   recebida está correta, incompleta ou errada.
2. Diagrama (texto ou Mermaid) da arquitetura nova (`ecommerce_ingestion_dag` + as 4 DAGs de
   transformação via Dataset).
3. Diff de tudo que muda: novas DAGs, `RenderConfig`, `profiles.yml`/`ProfileConfig`
   (threads), remoção da ingestão de dentro das DAGs de transformação.
4. Novo protocolo de benchmark e os números remedidos.
5. Atualizar `README.md` com a arquitetura final e a conclusão real sobre Core vs. Fusion e
   DuckDB vs. Postgres, só depois da comparação justa.

## Restrição
Qualquer mudança estrutural (nova DAG, forma de agrupar tasks, granularidade de retry) deve
vir acompanhada do trade-off explicado — eu decido entre benchmark "mais puro" e
observabilidade "mais granular" se os dois forem tecnicamente válidos, não é para escolher
por mim sem perguntar quando isso for uma preferência de projeto e não uma correção técnica
inequívoca.