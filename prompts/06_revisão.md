# Prompt: Revisão completa do projeto — infraestrutura, dbt e as 4 DAGs

## Contexto
O projeto evoluiu bastante: hoje temos 4 combinações rodando (dbt Core/Fusion × DuckDB/Postgres),
cada uma em sua própria DAG:

- `ecommerce_dbt_core_duckdb_dag` — 00:01:10
- `ecommerce_dbt_fusion_duckdb_dag` — 00:00:33
- `ecommerce_dbt_core_postgres_dag` — 00:00:37
- `ecommerce_dbt_fusion_postgres_dag` — 00:00:15

(tempos medidos com dataset pequeno, menos de 10 linhas por seed). Os números já mostram o
esperado (Fusion mais rápido que Core, Postgres mais rápido que DuckDB em paralelismo), mas
antes de tirar qualquer conclusão eu quero ter certeza de que o projeto inteiro está correto,
consistente e seguindo boas práticas — não só que "funciona e dá um número".

Quero uma revisão completa, de ponta a ponta, como um code review sênior faria antes de eu
considerar isso pronto para portfólio.

## Escopo da revisão

### 1. Infraestrutura
- `Dockerfile`: instalação correta e sem conflito entre dbt Core, dbt Fusion, adapter DuckDB
  e adapter Postgres coexistindo na mesma imagem; camadas otimizadas; nada instalado que não
  é mais usado.
- `docker-compose.override.yml`: configuração do serviço Postgres (persistência, rede,
  healthcheck), e se está seguindo o padrão esperado pelo Astro CLI sem conflitar com o que
  ele já gerencia.
- `.env.example`: todas as variáveis realmente usadas pelo projeto estão documentadas ali
  (credenciais Postgres, paths do dbt Fusion, etc.), nenhuma tratada como opcional se for
  obrigatória, e nenhum segredo real vazado no exemplo.
- `requirements.txt`: versões fixadas (não `latest` solto), sem dependência órfã, coerente
  com o que investigamos antes sobre compatibilidade Airflow/Cosmos/dbt.

### 2. Projeto dbt
- `profiles.yml`: os 4 targets (core/fusion × duckdb/postgres, ou a estrutura que ficou)
  configurados corretamente, usando variáveis de ambiente/Connections — nenhuma credencial
  hardcoded.
- `dbt_project.yml`: materializações coerentes por camada e por banco (o que faz sentido em
  DuckDB pode não ser o ideal em Postgres — verificar se isso foi considerado).
- Modelos (`staging`, `intermediate`, `marts`): mesma lógica SQL reaproveitada nos 4 cenários,
  sem sintaxe específica de um banco vazando para os modelos (se tiver algo assim, apontar).
- `schema.yml`/testes: testes genéricos e o singular test rodando de forma idêntica nos 4
  targets, sem teste "pulado" silenciosamente em algum cenário.
- Seeds: confirmar que o schema de destino e os tipos de coluna são equivalentes entre DuckDB
  e Postgres (evitar comparação de performance "injusta" por causa de diferença de schema).

### 3. As 4 DAGs
Para cada uma das 4 DAGs, revisar:
- `ProjectConfig`, `ProfileConfig`, `RenderConfig` e `ExecutionMode` usados — se são
  consistentes entre si (a única diferença entre as 4 deveria ser exatamente
  motor dbt/banco, não configuração incidental que também afeta o tempo medido).
- Se a **conexão com o banco** está sendo feita da forma correta em cada caso: Airflow
  Connection vs. variável de ambiente vs. credencial no `profiles.yml`, e se esse método é
  o mesmo nas 4 DAGs (uma inconsistência aqui pode estar distorcendo a comparação de tempo).
- Se a forma de conectar o Cosmos ao dbt Fusion está de acordo com o que documentamos como
  suportado oficialmente (não um workaround que passamos batido antes).
- Duplicação desnecessária de código entre as 4 DAGs — avaliar se faz sentido extrair
  configuração comum para um único módulo compartilhado (ex: `include/cosmos_config.py`),
  mantendo apenas o que de fato varia (motor/banco) explícito em cada DAG.
- Nomeação, tags e docs de cada DAG, para ficar claro na UI do Airflow qual é qual.

### 4. Justeza da comparação de performance
- Antes de validar os números acima como conclusão, checar se a comparação é justa:
  mesma quantidade de dados, mesmas materializações, mesmo número de threads/paralelismo
  configurado, sem uma DAG rodando com cache "quente" e outra "fria", por exemplo.
- Se encontrar algo que torna a comparação enviesada, apontar antes de qualquer outra coisa.

## Formato de entrega
1. Resumo executivo: o que está correto, o que precisa de ajuste, o que é crítico vs.
   cosmético.
2. Para cada problema encontrado: causa, correção recomendada, e diff do trecho de código.
3. Validação explícita (sim/não + justificativa) se a comparação de tempos acima é confiável
   como está, ou se precisa ser refeita depois dos ajustes.
4. Sugestão de refatoração para reduzir duplicação entre as 4 DAGs, se fizer sentido.
5. Atualizar o `README.md` com o resultado final da comparação de performance e qualquer
   decisão de arquitetura nova que surgir dessa revisão.

## Restrição
Apontar apenas problemas reais, com evidência no código — não sugerir mudança "porque é mais
moderno" se não houver ganho real de correção, consistência ou manutenção para este projeto.