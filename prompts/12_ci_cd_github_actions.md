# Prompt: Implementar CI/CD profissional (GitHub Actions)

## Contexto
O projeto (`airflow-dbt-study/`) tem hoje: Airflow + Cosmos, projeto dbt com staging →
intermediate → marts, testes genéricos + `dbt-expectations` + data contracts em
`fct_orders`, 4 DAGs (Core/Fusion × DuckDB/Postgres), camada de ingestão própria
(`include/ingestion/`), guardrails via hooks do Claude Code, e tudo versionado no Git.

Quero implementar CI/CD real via **GitHub Actions**, no padrão que uma equipe de dados
profissional usaria, sem inventar complexidade desnecessária para um projeto de estudo, mas
também sem pular etapas que fariam esse pipeline ser levado a sério em portfólio.

## O que preciso

### 1. Estrutura de workflows
- Criar `.github/workflows/` com jobs separados por responsabilidade (não um workflow
  monolítico fazendo tudo), no mínimo:
  - `lint.yml` — lint/format de Python (DAGs, `include/`) e do SQL/YAML do dbt.
  - `dbt-ci.yml` — build e teste do projeto dbt.
  - `airflow-dag-validation.yml` — valida que as 4 DAGs importam sem erro (DAG integrity
    check), sem precisar subir o Airflow inteiro.
  - `docker-build.yml` — garante que a imagem (`Dockerfile`) builda sem erro.
- Definir os **gatilhos** certos: `pull_request` para tudo que valida código antes de
  mergear; `push` na branch principal só para o que fizer sentido rodar pós-merge (se for o
  caso). Evitar rodar suites pesadas em todo `push` de branch de feature sem necessidade.

### 2. Lint e formatação
- Definir e aplicar um formatter/linter para Python (ex: `ruff`, cobrindo lint + format) nas
  DAGs e nos scripts de `include/ingestion/`.
- Definir lint para os arquivos do dbt (`sqlfluff` ou equivalente, considerando os dialetos
  DuckDB e Postgres que o projeto usa) e para os YAMLs (`yamllint`).
- Configurar como pre-commit hook local também (`.pre-commit-config.yaml`), para eu pegar
  problemas antes mesmo de abrir o PR, não só depois no CI.

### 3. CI do projeto dbt (o core do pipeline)
- Rodar `dbt deps`, `dbt build` (que já cobre seed + run + test) contra pelo menos um target
  leve o suficiente para rodar em CI de forma rápida (ex: DuckDB, sem precisar subir um
  serviço Postgres) — decidir e justificar se vale a pena rodar contra os 4 cenários
  completos em CI ou só validar a lógica com 1-2 (rodar tudo em toda PR pode ser lento demais
  para dar feedback rápido; explicar o trade-off e propor a melhor combinação).
- Se Postgres entrar no CI, usar o `services:` do GitHub Actions para subir um container
  Postgres efêmero, sem depender de infraestrutura externa.
- Rodar `dbt build` sobre o **dataset pequeno** (seed original), nunca o de 10.000/10M linhas
  — CI deve ser rápido; deixar claro que o dataset grande é só para benchmark manual, não
  para todo PR.
- Falhar o CI se qualquer teste dbt falhar, incluindo os testes de `dbt-expectations` e a
  validação de data contracts.
- Publicar os artefatos do `dbt docs generate` como artifact do workflow (não precisa
  hospedar publicamente, só disponibilizar para download/inspeção do PR).

### 4. Validação das DAGs do Airflow
- Job que importa as 4 DAGs em um ambiente com as dependências do projeto instaladas e falha
  se houver erro de import/parse (o erro clássico de "DAG quebrada" que só aparece na UI do
  Airflow em produção se não for pego antes).
- Validar que não há ciclo, task duplicada, ou `dag_id` conflitante entre as 4 DAGs.

### 5. Build da imagem Docker
- Job que builda a imagem do `Dockerfile` do zero, garantindo que a instalação de dbt Core +
  dbt Fusion + dependências do Airflow continua funcionando (esse é o tipo de coisa que quebra
  silenciosamente quando uma versão de pacote muda upstream).
- Usar cache de camadas do Docker no Actions para não pagar o custo total do build a cada PR.

### 6. Segredos e configuração
- Nenhuma credencial real no repositório nem no workflow — usar GitHub Actions Secrets para
  qualquer coisa sensível que o CI precisar (ex: senha do Postgres efêmero pode ser fixa
  porque é descartável, mas explicar essa diferença de qualquer segredo "de verdade").
- Confirmar que `.env`/`airflow_settings.yaml` reais nunca são lidos pelo CI — o CI deve usar
  seus próprios valores de exemplo/efêmeros, não depender de arquivo local.

### 7. Branch protection e qualidade
- Recomendar (documentar no README, já que isso é configuração do GitHub, não código) quais
  desses checks devem ser obrigatórios (`required status checks`) antes de permitir merge na
  branch principal.
- Sugerir um `CODEOWNERS` simples se fizer sentido para o projeto (mesmo sendo só eu, serve
  de documentação de intenção).

### 8. Documentação
- Adicionar um badge de status do CI no `README.md`.
- Nova seção no `README.md` explicando o pipeline de CI: o que cada workflow valida, quando
  dispara, e como rodar localmente os mesmos checks antes de abrir PR (lint, dbt build no
  dataset pequeno).

## Formato de entrega
1. Lista dos workflows propostos com gatilho e responsabilidade de cada um, para eu validar
   o desenho antes da implementação completa.
2. Os arquivos `.github/workflows/*.yml` completos.
3. `.pre-commit-config.yaml` e configuração de lint (`ruff`, `sqlfluff`, `yamllint`).
4. Diff de qualquer ajuste necessário no projeto para o CI funcionar (ex: se algo hoje
   depende de estado local que não existiria no runner do GitHub).
5. Trecho novo do `README.md` (badge + seção de CI).

## Restrição
Não rodar o benchmark de performance nem o dataset grande dentro do CI — isso é validação
manual, não parte do pipeline de PR. Qualquer decisão sobre rodar Postgres no CI (custo extra
de tempo/complexidade) deve vir com o trade-off explicado antes de eu confirmar.