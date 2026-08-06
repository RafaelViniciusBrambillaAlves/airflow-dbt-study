# Prompt: Auditoria Final Completa — Revisão end-to-end do projeto

## Contexto
Este projeto começou como um estudo de Airflow + dbt + Docker, mas evoluiu para um caso de
uso profissional de portfólio, cobrindo:

- **Infra**: Astro CLI, Astronomer Runtime, dbt Core/Fusion, DuckDB/Postgres, Docker.
- **Orquestração**: 4 DAGs (Core/Fusion × DuckDB/Postgres) via Cosmos, com ingestão
  separada, agrupamento de tasks otimizado, Airflow Datasets para orquestração orientada a
  dado.
- **Transformação dbt**: staging → intermediate → marts, com exposures, custom schema macro,
  testes (genéricos, singulares, dbt-expectations), contratos de dados, versionamento de
  modelo, observabilidade via OpenLineage/Marquez.
- **Ingestão**: script Faker reprodutível, carga idempotente em DuckDB/Postgres, branch
  automático (dataset pequeno/grande).
- **Governança**: guardrails via hooks/settings.json (Claude Code), credenciais via variáveis
  de ambiente, zero passo manual além do esperado.
- **Documentação**: README.md explicando arquitetura, decisões, procedimentos.

Quero uma **auditoria final completa**, como se fosse um code review de entrada em uma empresa
grande, validando que tudo está pronto para ser mostrado em portfólio e mantível em produção.

## Escopo da auditoria

### 1. Infraestrutura e ambiente
- `Dockerfile`: todas as dependências (Airflow, dbt Core, dbt Fusion, adapters DuckDB/Postgres,
  Faker, pandas, etc.) instaladas corretamente, sem conflito de versão, camadas otimizadas,
  nada órfão ou duplicado.
- `docker-compose.override.yml`: serviços (Airflow, Postgres warehouse, Marquez) na mesma
  rede Docker, com healthchecks, persistência de volume, nenhuma credencial hardcoded.
- `.env.example`: lista completa de todas as variáveis realmente usadas no projeto
  (conexões, paths, volumes), com defaults seguros e documentação clara.
- `requirements.txt`: versões fixadas (não `latest`), sem dependência órfã, coerente com
  compatibilidade Airflow/Cosmos/dbt investigada.
- `airflow_settings.yaml.example`: todas as Connections e Variables sincronizadas
  automaticamente no startup (Astro CLI nativo), zero passo manual.

### 2. Projeto dbt — estrutura e maturidade
- Organização de pastas (`staging`, `intermediate`, `marts`) coerente, sem arquivo em lugar
  errado.
- `dbt_project.yml`: materializações apropriadas (view/table/incremental), seed path apontando
  para ingestão separada (não mais para `dbt seed`), macro `generate_schema_name` customizado
  funcionando nos dois targets, schema/database/catalog configurado corretamente.
- `packages.yml`: `dbt-expectations` incluído com versão compatível, `dbt-utils` ou
  equivalente se usados nos modelos.
- `profiles.yml`: não deve existir no git com credenciais reais; confirmar que usa apenas
  variáveis de ambiente/Airflow Connections (ProfileConfig do Cosmos), não hardcoding.
- Modelos SQL:
  - Staging: um `SELECT` simples + `CAST` por tabela raw, sem lógica de negócio.
  - Intermediate: enriquecimento e JOINs preparatórios (se necessário).
  - Marts: fatos e dimensões, com lógica de negócio, sem sintaxe específica de banco.
- `schema.yml`:
  - Sources declarando as tabelas raw (`raw_customers`, `raw_products`, `raw_orders`),
    apontando para o banco correto (não mais para seeds).
  - Modelos documentados, com descrição e `columns`.
  - Testes: genéricos (`not_null`, `unique`, `relationships`, `accepted_values`) distribuídos
    por camada, **sem teste pulado silenciosamente** em nenhum volume de dado.
  - Testes `dbt-expectations`: pelo menos 2 aplicados em `fct_orders` (ex:
    `expect_column_values_to_be_increasing`, `expect_column_mean_to_be_between`), com
    valores justificados pelos dados reais.
  - Contratos de dados (`contracts: {enforced: true}`) em modelos de marts, especialmente
    `fct_orders`, com tipos e constraints declarados.
- Exposures: `_marts__exposures.yml` declarando um "Dashboard de Vendas" que consome
  `fct_orders`, com owner, description e dependencies corretos.
- Versionamento: modelos marts (especialmente `fct_orders`) com `versions: v1.0.0` declarados
  e documentados.

### 3. Ingestão de dados
- `include/ingestion/generate_fake_data.py`: Faker reprodutível (seed fixa), integridade
  referencial garantida (todo `customer_id` existe, todo `product_id` existe, datas coerentes),
  distribuição realista (status, quantidade, desconto), parâmetro de volume configurável.
- `include/ingestion/load_raw_data.py`: funções separadas por banco (DuckDB vs. Postgres),
  idempotência garantida (truncate + load ou upsert, sem duplicação na re-execução), tratamento
  de erro/transação claro.
- `include/ingestion/airflow_tasks.py`: branch (`choose_ingestion_path` via
  `ecommerce_dataset_size` Variable) lógico e sem race condition, junção (`raw_data_ready`)
  com `trigger_rule` correto.
- Integração: ingestão rodando em DAG **separada** (`ecommerce_ingestion_dag`), comunicando
  via Airflow Dataset (`ecommerce_raw_data`) com as 4 DAGs de transformação (não hardcoded de
  horário).

### 4. Orquestração — as 4 DAGs + DAG de ingestão
- `ecommerce_ingestion_dag`: carga de dados brutos (seed ou large dataset), emite Dataset ao
  fim.
- `ecommerce_dbt_core_duckdb_dag`, `ecommerce_dbt_fusion_duckdb_dag`,
  `ecommerce_dbt_core_postgres_dag`, `ecommerce_dbt_fusion_postgres_dag`:
  - Configuração de `ProjectConfig`, `ProfileConfig`, `RenderConfig` **idêntica** entre as 4,
    variando **apenas** motor (dbt_executable_path) e banco (target name no profile).
  - `InvocationMode.SUBPROCESS` explícito em todas as 4 (não `DBT_RUNNER` por padrão).
  - Agrupamento de tasks otimizado: um único `DbtRunLocalOperator` rodando o projeto inteiro
    (ou por seleção/tag), **não** uma task por modelo (isso elimina overhead de subprocesso
    repetido).
  - Threads configurados adequadamente: `threads: 8` ou similar para Postgres, `threads: 1`
    para DuckDB.
  - Sensor/trigger de Dataset na entrada (as 4 rodam quando `ecommerce_raw_data` é atualizado,
    não por horário fixo).
  - Task de ingestão dentro da DAG **removida** (agora em `ecommerce_ingestion_dag`).
  - Nomes de task coerentes e descritivos (não genéricos como `load_raw_seed`).

### 5. Observabilidade e qualidade
- Logs estruturados: cada fase (ingestão, staging, intermediate, marts, testes) com início/fim
  marcado em nível INFO, duração registrada.
- Métricas de duração: capturadas por task/task group, comparáveis entre as 4 DAGs (benchmark
  justo, sem a ingestão confundindo os números).
- OpenLineage integrado: Cosmos emitindo eventos de lineage (em LocalMode/VirtualEnv, suportado
  desde Cosmos 1.1), com limitações conhecidas do dbt Fusion documentadas se houver.
- Marquez rodando em Docker, recebendo eventos, com grafo de lineage visível (ponta a ponta:
  banco → dbt → Airflow).

### 6. Segurança e configuração
- Nenhuma credencial em código: DuckDB/Postgres credenciais via Airflow Connections ou
  variáveis de ambiente.
- `.env` real **nunca commitado**: `.gitignore` contém `.env`, `.env.local`, `*.local.json`.
- `.env.example` atualizado com **todos** os valores esperados.
- Airflow `webserver.SECRET_KEY`, `sql_alchemy_conn` configurados sem expô-los.
- Guardrails via `.claude/settings.json`: hooks bloqueando edição de arquivos sensíveis
  (profiles, docker-compose, .env real, dbt_project.yml) sem confirmação, com script auditável
  em `.claude/hooks/`.

### 7. Documentação
- `README.md`:
  - Visão geral: o que o projeto faz.
  - Stack e papel de cada ferramenta.
  - Arquitetura (diagrama ASCII/Mermaid): ingestão separada → transformação via Dataset →
    observabilidade.
  - Decisões de arquitetura (por que Astro, por que Cosmos, por que separar ingestão, por que
    agrupamento de tasks, threads por banco, etc.), cada uma justificada.
  - Estrutura de pastas e propósito de cada uma.
  - Como rodar localmente: `astro dev start`, como alternar dataset (small/large), como
    ver docs do dbt, como visualizar lineage no Marquez.
  - Benchmark final: tempos das 4 DAGs com dataset grande, conclusão sobre Core vs. Fusion e
    DuckDB vs. Postgres.
  - Próximos passos: CI/CD, incremental models, dados reais, etc.
- Comentários em código: todos em português, explicando a intenção (não redundante com
  README).

### 8. Testes e validação
- `dbt build` rodando sem erro nos 4 cenários (Core/Fusion × DuckDB/Postgres) com dataset
  grande (~10.000 linhas).
- Todos os testes passando (genéricos, singulares, dbt-expectations, contratos).
- Cada DAG disparável independentemente (sem dependência quebrada se `ecommerce_ingestion_dag`
  não rodar).
- Idempotência confirmada: rodar a ingestão duas vezes não duplica dados; rodar a
  transformação duas vezes com o mesmo input não quebra nada.
- Reaproveitamento de dados: se dados já estão carregados, as 4 DAGs conseguem rodar
  transformação só deles, sem estar presos a aguardar ingestão.

### 9. Reprodutibilidade
- Seed Faker fixa: dois builds geram os mesmos dados (com `ecommerce_large_dataset_n_orders
  = 10000`, por exemplo).
- Versão pinada de todas as ferramentas (sem `latest` solto).
- Passo a passo de setup novo limpo (fresh clone, `astro dev start`, um comando para
  configurar variáveis, pronto — sem "ah, você precisa também fazer X manualmente").

### 10. Pronto para portfólio
- Não há TODO/FIXME comments deixados no código.
- Não há arquivos temporários (`.tmp`, `debug/`, backup da DAG antigo) commitados.
- Logs/outputs sensíveis (.duckdb, `target/`, `logs/`) estão em `.gitignore`.
- Projeto pode ser clonado, subido e rodado por outra pessoa sem dúvida sobre o que fazer.
- Histórico de git coerente (não há commit "corrigir erro anterior" logo depois de "implementar
  X" — squash ou reescreva se necessário).

## Formato de entrega
1. **Resumo executivo**: pontos críticos (quebras, incoerências), pontos de melhoria (cosmético
   ou optimization), pontos de excelência (já está ótimo).
2. **Por categoria da auditoria** (1-10 acima):
   - Se está tudo certo: "✓ Validado".
   - Se falta algo: descrição, por que é importante, como corrigir.
   - Se há incoerência: onde está, qual a causa, como resolver.
3. **Checklist final**: sim/não + justificativa brevemente para cada item critical.
4. **Plano de ação**: lista ordenada do que precisa ser feito, de crítico para cosmético.
5. **Confirmação final**: "Pronto para portfólio: SIM/NÃO", com ressalvas se houver.

## Restrição
Não deixar de fora nada porque "é óbvio" — melhor errar para o lado de checar demais.
Tudo que eu pedir para corrigir deve vir com justificativa profissional, não "porque é mais
moderno" — qualidade, manutenibilidade, reprodutibilidade, segurança, ou conformidade com a
promessa do projeto.
