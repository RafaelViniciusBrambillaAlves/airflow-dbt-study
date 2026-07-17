# Prompt: Projeto de estudo profissional — Airflow + dbt + Docker

## Contexto
Quero construir um projeto de estudo de engenharia de dados, mas com padrão de qualidade
profissional (algo que eu poderia mostrar em um portfólio ou usar como base real em uma empresa).
O objetivo é aprender a stack moderna de orquestração + transformação de dados, entendendo o
"porquê" de cada decisão de arquitetura, não só copiar comandos.

## Stack obrigatória
- **Orquestração**: Apache Airflow, rodando via **Astro CLI** (Astronomer), em Docker.
- **Transformação**: dbt, começando com **dbt Core** (Python) e depois migrando para
  **dbt Fusion** (motor em Rust, mais rápido, com validação de modelos em tempo real).
- **Integração Airflow + dbt**: **Astronomer Cosmos**, renderizando cada modelo dbt como uma
  task/task group do Airflow (não usar `BashOperator` cru chamando `dbt run`).
- **Banco de dados**: sugerir a melhor opção para um projeto de estudo pequeno (ex: DuckDB
  para simplicidade local vs. Postgres para simular um ambiente mais "produção"), explicando
  o trade-off, e implementar com a escolhida.
- **Dados**: um dataset pequeno de exemplo (poucas linhas, tipo pedidos/clientes/produtos ou
  similar), só para eu conseguir rodar o pipeline fim-a-fim. Depois vou trocar por dados reais
  ou gerados.

## O que preciso que o projeto entregue

### 1. Estrutura de projeto
- Organização de pastas seguindo as convenções recomendadas pela Astronomer (`dags/`, `dbt/`,
  `include/`, `plugins/`, `Dockerfile`, `requirements.txt`, `packages.yml` do dbt, etc).
- `Dockerfile` do Astro instalando corretamente:
  - dbt Core + adapter do banco escolhido
  - depois, binário do dbt Fusion (via `install.sh` do dbt Labs), já que ele não é
    distribuído pelo pip como o Core
- Separação clara entre **ambiente de execução do Airflow** e **ambiente de execução do dbt**
  (virtualenv isolado ou `ExecutionMode` do Cosmos), explicando por que isso é best practice
  (evitar conflito de dependências entre Airflow e dbt).

### 2. Modelagem dbt
- Organização em camadas: `staging` → `intermediate` (se fizer sentido) → `marts`, seguindo
  a convenção do dbt (nomeação `stg_`, `int_`, `fct_`/`dim_`).
- Uso de `sources.yml` para declarar a origem dos dados brutos.
- Uso de `schema.yml` com **testes genéricos** (`not_null`, `unique`, `relationships`,
  `accepted_values`) em pelo menos um modelo de cada camada.
- Pelo menos um **teste customizado** (singular test) demonstrando uma regra de negócio.
- Documentação dbt (`dbt docs generate`) configurada e explicada.
- `dbt_project.yml` configurado com materializações apropriadas por camada (view para staging,
  table/incremental para marts) e explicação de por que essa escolha.

### 3. Orquestração no Airflow via Cosmos
- Uma DAG usando `DbtDag` ou `DbtTaskGroup` do Cosmos (explicar quando usar cada um).
- Configuração de `ProjectConfig`, `ProfileConfig` e `ExecutionMode` explicada linha a linha.
- Execução dos **testes do dbt como parte da DAG** (não só localmente), com a task de teste
  rodando depois do modelo correspondente e podendo falhar a pipeline se um teste falhar.
- Uso de conexões do Airflow (`Connection`) para credenciais do banco, em vez de credenciais
  hardcoded no `profiles.yml`.
- Visualização do grafo de lineage do dbt dentro da UI do Airflow (task groups refletindo o DAG
  de dependências do dbt).

### 4. Boas práticas de produção (mesmo em projeto pequeno)
- Uso de variáveis de ambiente / Airflow Variables e Connections para segredos, nunca commitados.
- `.gitignore` apropriado (target/, logs/, dbt_packages/, .env).
- Testes de qualidade de dados como "quality gate" antes de expor o dado final.
- Sugestão de como isso escalaria: incremental models, particionamento, CI/CD (ex: rodar
  `dbt build` no PR antes de mergear).

### 5. Explicações pedagógicas
Para cada decisão de arquitetura tomada (Astro vs docker-compose puro, Cosmos vs Bash puro,
dbt Fusion vs dbt Core, DuckDB vs Postgres, staging/marts, ExecutionMode escolhido), quero um
parágrafo curto explicando:
- o que o Airflow/dbt resolvem nessa parte
- por que essa é considerada a prática recomendada atualmente
- qual seria a alternativa mais simples/ingênua e por que ela é pior em produção

## Formato de entrega
1. Primeiro, me dê a árvore de diretórios completa do projeto.
2. Depois, o conteúdo de cada arquivo relevante (Dockerfile, DAG, modelos dbt, schema.yml,
   docker-compose/astro config), com comentários explicando o essencial.
3. Por fim, os comandos exatos para subir o projeto localmente (`astro dev start` etc.),
   rodar `dbt build` e verificar os testes passando, e acessar a UI do Airflow para ver a DAG.

## Restrições
- Não usar bibliotecas ou padrões deprecados.
- Priorizar o que é considerado best practice hoje (2026), não tutoriais antigos.
- Se alguma parte da stack pedida tiver uma limitação conhecida (ex: algo que o Cosmos ainda
  não suporta com dbt Fusion), avisar explicitamente em vez de simular que funciona.