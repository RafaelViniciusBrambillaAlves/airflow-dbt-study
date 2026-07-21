# Prompt: Investigação de compatibilidade — Airflow, Cosmos, dbt e DuckDB

Já tenho o projeto Airflow + dbt + Cosmos + DuckDB rodando via Astronomer Runtime em Docker
(estrutura em `airflow-dbt-study/`). A arquitetura pretendida é:

```
Airflow → Astronomer Cosmos → dbt (virtualenv isolado) → DuckDB
```

## Problema
As versões que eu configurei não são as versões que estão de fato rodando dentro do
container, e isso está causando erros. Exemplos:

- `Dockerfile` pede `dbt-core==1.9.*` e `dbt-duckdb==1.9.*`, mas `dbt --version` dentro do
  container mostra `Core: 1.12.0` / `duckdb: 1.10.1`.
- `ExecutionMode.VIRTUALENV` se comporta diferente de `ExecutionMode.LOCAL`.
- Erro `NameError: InvocationMode is not defined`, indicando que o código está usando um
  recurso do Cosmos que não existe na versão realmente instalada.
- Erro de `Conflicting lock` no DuckDB.
- Erro/permissão ao Cosmos tentar ler o arquivo `.duckdb` (relacionado ao cache dele).

Preciso entender a causa raiz de cada um antes de aplicar qualquer correção — não quero um
workaround, quero saber por que está acontecendo.

## O que preciso que você investigue

Para cada item, a resposta precisa ter como base evidência real (comando executado,
código-fonte lido, ou documentação oficial citada) — não suposição.

1. **Versões efetivas em cada camada**: Airflow, Astronomer Runtime, Python, Cosmos
   (`pip show astronomer-cosmos`), dbt do sistema, dbt do virtualenv, e qual dbt o Cosmos de
   fato invoca. Mostrar os comandos usados para cada verificação.
2. **Por que a versão instalada diverge da versão pedida no `Dockerfile`/`requirements.txt`**
   (ex: resolução de dependências puxando uma versão mais nova, imagem base do Astronomer
   Runtime já trazendo algo pré-instalado que sobrepõe, cache de build do Docker, etc).
3. **Como o Cosmos localiza e executa o dbt internamente**: se respeita
   `dbt_executable_path`/`virtualenv_dir`, se cria virtualenv temporário, como monta o
   comando, diferença real de isolamento entre `ExecutionMode.LOCAL` e
   `ExecutionMode.VIRTUALENV` (o que cada um resolve de fato).
4. **`InvocationMode`**: em qual versão do Cosmos foi introduzido, se a versão atualmente
   instalada tem esse recurso, e se a documentação que eu segui era de uma versão diferente
   da instalada.
5. **Cache do Cosmos e o lock do DuckDB**: por que o Cosmos tenta ler o arquivo `.duckdb`
   para calcular hash/cache, por que isso gera `PermissionError` ou `Conflicting lock`, e
   como o DuckDB lida com concorrência de conexões (ligar isso ao que já vimos sobre DuckDB
   não paralelizar bem).
6. **Matriz de compatibilidade oficial** entre Airflow, Astronomer Runtime, Cosmos, dbt Core
   e dbt-duckdb — só com base em changelog/release notes oficiais, indicando versões
   recomendadas e o que está perto do fim de suporte.

## Recomendação final
Depois da investigação, comparar estas opções e recomendar uma, com justificativa técnica:

- **A**: manter `ExecutionMode.VIRTUALENV` (isolamento total, porém mais complexidade)
- **B**: usar `ExecutionMode.LOCAL` (mais simples, isolamento menor, mas suficiente para um
  projeto com um único adapter/dbt)
- **C**: outra abordagem, se fizer mais sentido dado o que for encontrado

A recomendação deve levar em conta que este é um projeto de estudo com padrão profissional:
priorizar estabilidade, compatibilidade oficial e facilidade de manutenção, não
sofisticação desnecessária.

## Formato de entrega
1. Resumo executivo: causa raiz de cada erro (1-2 frases cada).
2. Tabela de versões: configurada vs. instalada vs. usada de fato, com o comando que provou
   cada valor.
3. Matriz de compatibilidade oficial (com fonte).
4. Diagrama da arquitetura atual vs. arquitetura recomendada.
5. Plano de correção com código (fixar versões no `Dockerfile`/`requirements.txt`,
   ajustar `ExecutionMode`, resolver o cache/lock do DuckDB).
6. Atualizar o README (seção de decisões de arquitetura) com o que for decidido aqui.