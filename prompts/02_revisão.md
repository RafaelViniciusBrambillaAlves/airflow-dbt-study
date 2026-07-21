# Prompt: Revisão técnica — ExecutionMode, paralelismo e observabilidade

Já tenho o projeto Airflow + dbt + Cosmos + DuckDB funcionando (estrutura em
`airflow-dbt-study/`, DAG em `dags/dbt_dag.py`, modelos em `include/dbt/ecommerce/`).

Preciso que você revise o código atual como um engenheiro de dados sênior faria em um
code review, focando nos três problemas abaixo. Para cada um: explique a causa raiz,
proponha a correção seguindo as melhores práticas atuais, e mostre o diff/trecho de código
alterado (não reescreva arquivos inteiros sem necessidade).

## Problema 1 — `ExecutionMode.VIRTUALENV` não funciona, só `ExecutionMode.LOCAL`
- Investigar por que a DAG só roda com `ExecutionMode.LOCAL` e falha (ou não executa) com
  `ExecutionMode.VIRTUALENV`.
- Verificar se falta a configuração de um `virtualenv_dbt_project_path` /
  `py_requirements` no `ProjectConfig`/`RenderConfig`, se o dbt (Core ou Fusion) está
  instalado corretamente para ser usado dentro de um venv isolado criado pelo Cosmos, e se
  há conflito de versão entre o Python do Airflow e o exigido pelo adapter do DuckDB.
- Explicar a diferença real de isolamento entre `LOCAL` e `VIRTUALENV` (o que cada um resolve),
  para eu entender se realmente preciso do `VIRTUALENV` nesse projeto ou se `LOCAL` já é
  aceitável para o meu caso de uso (estudo, um único adapter, sem conflito de dependências
  entre DAGs).
- Se `VIRTUALENV` não fizer sentido aqui, dizer isso explicitamente e justificar manter
  `LOCAL`, em vez de forçar uma solução desnecessariamente complexa.

## Problema 2 — DuckDB não paraleliza, os modelos rodam um de cada vez
- Confirmar a limitação real do DuckDB (é uma limitação do banco em si com múltiplas
  conexões de escrita concorrentes, não um bug de configuração).
- Revisar a configuração de `threads` no `profiles.yml` do dbt e o que ela realmente
  paraleliza (threads internas de execução de um único processo dbt vs. paralelismo de
  tasks do Airflow).
- Avaliar e comparar as alternativas, com prós/contras de cada uma para um projeto de estudo:
  a) manter DuckDB e aceitar a execução sequencial, já que o volume de dados é pequeno;
  b) usar DuckDB apenas em modo single-writer com fila de escrita e revisar se o Cosmos está
     gerando concorrência desnecessária entre tasks que escrevem no mesmo arquivo `.duckdb`;
  c) migrar para Postgres, que suporta conexões concorrentes de verdade, se o objetivo do
     projeto for também aprender/demonstrar paralelismo real de DAG.
- Recomendar a opção mais adequada para os objetivos do projeto (estudo, mas com padrão
  profissional) e explicar o porquê.

## Formato de entrega
1. Resumo executivo: para cada um dos 2 problemas, causa raiz em 1-2 frases.
2. Para cada problema, a solução recomendada com código (diff ou trecho).
3. Se algum problema exigir uma decisão minha (ex: trocar de banco), apresentar as opções
   com trade-offs e perguntar antes de aplicar a mudança, em vez de assumir.
4. Atualizar o README (seção de decisões de arquitetura) com qualquer mudança relevante
   feita nesta revisão, mantendo o padrão de explicação já usado nele.