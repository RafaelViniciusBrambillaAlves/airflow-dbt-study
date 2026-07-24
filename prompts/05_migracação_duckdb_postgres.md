# Prompt: Migração de DuckDB para Postgres

## Contexto
O projeto está funcionando com DuckDB como warehouse (seeds `raw_customers`, `raw_products`,
`raw_orders` → staging → intermediate → marts, via dbt + Cosmos + Airflow). Já identificamos
antes que o DuckDB não paraleliza bem escrita concorrente, o que limita a execução das DAGs.
Quero migrar o warehouse para **Postgres**, mantendo os mesmos dados, mesmas transformações e
mesmos testes — só troca o banco por trás.

## O que preciso

### 1. Serviço Postgres no ambiente
- Adicionar um serviço Postgres ao ambiente do Astro CLI (`docker-compose` gerenciado pelo
  Astro, ou configuração equivalente), explicando a forma recomendada de fazer isso hoje sem
  quebrar o gerenciamento padrão do Astro.
- Definir volumes/persistência de dados do Postgres para não perder dados entre restarts do
  ambiente local.
- Credenciais via `.env`/Airflow Connection, nunca hardcoded — seguindo o mesmo padrão de
  segredo já usado no resto do projeto.

### 2. Adapter e dependências
- Trocar/ajustar `requirements.txt` e `Dockerfile`: sair de `dbt-duckdb` para `dbt-postgres`
  (ou manter os dois instalados, se decidirmos manter DuckDB como opção — deixar isso como
  pergunta em aberto para eu decidir).
- Ajustar o `profiles.yml` do dbt para um novo target Postgres, mantendo o target DuckDB
  existente como alternativa nomeada (ex: `target: postgres` vs `target: duckdb`), para eu
  poder comparar os dois sem perder o que já funciona.

### 3. Seeds
- Confirmar como o `dbt seed` se comporta ao carregar os `.csv` (`raw_customers`,
  `raw_products`, `raw_orders`) no Postgres — tipos de coluna inferidos, schema de destino,
  necessidade de configurar `+column_types` no `dbt_project.yml` se o Postgres inferir algo
  diferente do DuckDB.

### 4. Modelos e materializações
- Revisar se as materializações atuais (view em staging, table/incremental em marts) ainda
  fazem sentido no Postgres ou se algo deve mudar por causa das características do banco.
- Confirmar que staging → intermediate → marts roda sem alteração de SQL (o objetivo é o dbt
  abstrair a diferença de banco; se algum modelo usar sintaxe específica do DuckDB, apontar
  e corrigir).

### 5. Airflow/Cosmos
- Ajustar a `ProfileConfig` da(s) DAG(s) para apontar para o novo target/Connection do
  Postgres, usando Airflow Connections em vez de credenciais no `profiles.yml`.
- Confirmar se isso resolve de fato o problema de paralelismo identificado antes (múltiplas
  tasks/models rodando concorrentemente sem lock de arquivo), e reportar a diferença de tempo
  de execução da DAG antes/depois, usando as métricas de duração já implementadas no projeto.
- Entregar as duas novas dags, seguindo os mesmos padrões `dbt_core_dag_postgres_dag` e `dbt_fusion_dag_postgres_dag`, para rodar com core e fusion
 
### 6. Decisão sobre o DuckDB
- Não descartar o DuckDB sem eu decidir: apresentar a opção de manter os dois targets
  (Postgres como principal, DuckDB como target alternativo para comparação/estudo) vs.
  remover o DuckDB por completo, com prós e contras de cada uma, e perguntar antes de apagar
  qualquer coisa.

### 7. Documentação
- Atualizar o `README.md`: nova seção de decisão de arquitetura explicando por que migramos
  (paralelismo), o que mudou na infraestrutura (novo serviço no ambiente), e o resultado da
  comparação de performance DuckDB vs. Postgres.
- Comentários novos em português, seguindo o padrão do projeto.

## Formato de entrega
1. Resumo do que muda na infraestrutura (novo serviço, novas variáveis de ambiente).
2. Diff do `Dockerfile`/`requirements.txt`.
3. Diff do `profiles.yml` (targets duckdb e postgres lado a lado).
4. Configuração do serviço Postgres no ambiente Astro.
5. Passo a passo para rodar `dbt seed` + `dbt build` + `dbt test` no novo target e validar
   que os dados/testes batem com o DuckDB.
6. Resultado da comparação de tempo de execução da DAG nos dois bancos.
7. Trecho novo do `README.md`.

## Restrição
Não migrar silenciosamente removendo o que já funciona — qualquer decisão que apague ou
substitua configuração existente (ex: remover o target DuckDB) deve ser explicitamente
confirmada comigo antes de ser aplicada.