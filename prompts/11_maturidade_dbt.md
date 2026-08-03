# Prompt: Maturidade avançada do projeto dbt — exposures, schemas, observabilidade e contratos

## Contexto
O projeto dbt (`include/dbt/ecommerce/`) já tem staging → intermediate → marts, testes
genéricos e singular test, rodando em 2 motores (Core/Fusion) × 2 bancos (DuckDB/Postgres)
via Cosmos. Quero elevar o nível de maturidade em 4 frentes, cada uma com um objetivo
específico de portfólio profissional.

Implementar uma de cada vez, validando que nada quebra as 4 DAGs existentes antes de seguir
para a próxima.

## 1. Declarar ponto de consumo final (`exposures`)
- O termo correto no dbt para isso é **`exposures`** (não é um campo livre no
  `dbt_project.yml`) — declarar em um arquivo `models/marts/_marts__exposures.yml` (ou
  arquivo próprio, ex: `exposures.yml`) um exposure do tipo `dashboard` representando um
  "Dashboard de Vendas" que consome `fct_orders` (e `dim_customers`/`dim_products` se fizer
  sentido), com `owner`, `description` e `depends_on` apontando para os modelos corretos.
- Confirmar que `dbt docs generate` reflete esse exposure no grafo de linhagem (ele deve
  aparecer como nó final, mostrando visualmente até onde o dado é consumido).

## 2. Macro `generate_schema_name` customizado
- Confirmar se o projeto já sobrescreve esse macro (ele não vem customizado por padrão no
  `dbt_project.yml` gerado, precisa ser criado em `macros/`).
- Implementar `generate_schema_name` em `macros/generate_schema_name.sql` seguindo o padrão
  oficial recomendado pelo dbt (baseado no macro padrão do dbt-core, não reescrever do zero):
  o schema final deve seguir uma convenção fixa (ex: `analytics_<camada>` ou
  `<database_schema_base>_<custom_schema>`), **igual independente de quem estiver rodando**
  (sem sufixo de usuário/dev automático, que é o comportamento padrão do dbt em ambiente de
  desenvolvimento).
- Validar em pelo menos 2 targets (Postgres e DuckDB) que o schema gerado é o esperado.

## 3. Observabilidade e qualidade de dado além do básico

### `dbt-expectations`
- Adicionar `dbt-expectations` ao `packages.yml` (via `dbt deps`) e confirmar a versão
  compatível com a versão do dbt Core já usada no projeto.
- Aplicar pelo menos 2 testes representativos em `fct_orders`:
  - `expect_column_values_to_be_increasing` em uma coluna que faça sentido (ex: `order_date`
    dentro de uma janela, ou um ID sequencial) — explicar se esse teste faz sentido dado o
    grão da tabela antes de aplicá-lo.
  - `expect_column_mean_to_be_between` em `net_amount` (ou equivalente), com faixa de valor
    justificada com base nos dados reais atuais (não um número arbitrário) — para detectar
    variação absurda entre rodadas.
- Confirmar se `dbt-expectations` funciona igual nos dois targets (Postgres/DuckDB) e nos
  dois motores (Core/Fusion) — não assumir, testar, já que Fusion é mais novo e pode ter
  suporte parcial a pacotes de terceiro.

### OpenLineage
- Confirmar (Cosmos já tem suporte nativo a OpenLineage desde a versão 1.1, sem precisar
  mudar código da DAG, mas **só nos modos de execução `local` e `virtualenv`** — validar se
  isso cobre as 4 DAGs atuais, já que usamos `LOCAL`) e verificar se há alguma limitação
  conhecida específica com dbt Fusion (o formato do `run_results.json` do Fusion difere do
  dbt Core, o que já causou bugs reportados na geração de lineage/assets no Cosmos com
  Fusion — checar se isso nos afeta antes de assumir que funciona igual nas 4 DAGs).
- Configurar o provider oficial de OpenLineage do Airflow para emitir os eventos.
- Escolher e configurar um backend de visualização (Marquez, por ser open-source e mais
  simples de rodar localmente em Docker) — adicionar como serviço no
  `docker-compose.override.yml`, com a mesma atenção à rede Docker que já resolvemos para o
  Postgres.
- Validar visualmente que o grafo de lineage aparece ponta a ponta (banco → dbt → Airflow) no
  Marquez para pelo menos uma das 4 DAGs.

## 4. Governança: contratos de dados e versionamento de modelo

### Data contracts
- Aplicar `contracts: {enforced: true}` nos modelos de marts (começando por `fct_orders`),
  com `data_type` e `constraints` (`not_null`, `primary_key` onde fizer sentido) declarados
  no `schema.yml` de cada coluna.
- Confirmar que isso realmente valida tipo/constraint **no banco antes de materializar**
  (comportamento esperado do contract), rodando um teste proposital que quebre o contrato
  (ex: mudar o tipo de uma coluna no SQL) para confirmar que o dbt falha no lugar certo, não
  só depois via teste solto.
- Confirmar suporte de contracts nos dois adapters (Postgres e DuckDB) e nos dois motores.

### Versionamento semântico de modelo
- Adicionar `versions` (recurso nativo de model versioning do dbt) em `fct_orders` como
  `v1`, com `defined_in` apontando para o arquivo atual, documentando no `description` que
  qualquer mudança de granularidade da tabela deve subir para `v2`.
- Explicar, com um exemplo concreto hipotético (ex: "se `fct_orders` passar a ter uma linha
  por item de pedido em vez de uma por pedido"), como ficaria o processo de criar `fct_orders_v2`
  sem quebrar consumidores do `v1` (incluindo o exposure do item 1, que deve continuar
  apontando para a versão correta).

## Formato de entrega
1. Implementação item por item (exposure → schema macro → dbt-expectations → OpenLineage →
   contracts → versioning), cada um validado antes do próximo.
2. Diff de todos os arquivos novos/alterados.
3. Para cada item, confirmar explicitamente se funciona nos 4 cenários (Core/Fusion ×
   DuckDB/Postgres) ou se há limitação em algum — não assumir paridade sem testar,
   especialmente com dbt Fusion.
4. Print/descrição do grafo de lineage no Marquez e do grafo `dbt docs` mostrando o exposure.
5. Atualizar `README.md` com uma nova seção de "Maturidade e Governança" cobrindo os 4 pontos.

## Restrição
Não aplicar `contracts: enforced: true` em todos os modelos de uma vez — começar só por
`fct_orders`, confirmar que não quebra nenhuma das 4 DAGs, e só depois perguntar se eu quero
estender para os demais marts. Mudança de schema gerado pelo macro do item 2 pode impactar
dados já existentes no banco — avisar antes de aplicar se isso implicar recriar schemas.