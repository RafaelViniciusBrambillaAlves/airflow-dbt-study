# Prompt: Geração de dados fake (~10.000.000 linhas) e organização do dataset

## Contexto
Hoje o projeto tem 3 seeds pequenos e com dados fixos (poucas linhas cada), usados pelos 4
cenários de comparação (Core/Fusion × DuckDB/Postgres):

```
include/dbt/ecommerce/seeds/raw_customers.csv   (customer_id, first_name, last_name, email, signup_date)
include/dbt/ecommerce/seeds/raw_orders.csv      (order_id, customer_id, product_id, quantity, discount_pct, status, order_date)
include/dbt/ecommerce/seeds/raw_products.csv    (product_id, product_name, category, unit_price)
```

Quero gerar uma versão com **~10.000.000 linhas em `raw_orders`** (e uma quantidade proporcional
de `customers`/`products`) para testar o pipeline com volume real, mas **não sei qual é a
forma correta de organizar isso no projeto** — não quero simplesmente criar um
`seeds_10000000/` ou duplicar DAGs de forma improvisada sem entender se é essa mesmo a prática
recomendada.

## Parte 1 — Decisão de arquitetura (responder antes de gerar qualquer dado)

- **dbt seeds são recomendados para 10.000.000 linhas?** Verificar, com base na documentação
  oficial do dbt, qual é o limite/recomendação de tamanho para uso de `seeds` (eles existem
  para dados de referência pequenos e estáveis, não para carga de fatos em volume). Se
  10.000.000 linhas em `raw_orders` for um anti-padrão como seed, explicar a alternativa correta
  (ex: gerar o CSV e carregar via um mecanismo de ingestão — um script/task Python separado
  que popula uma tabela "raw" no banco, tratada como `source` do dbt — em vez de usar `dbt
  seed` para isso).
- **Um projeto só, ou dataset "pequeno" e "grande" coexistindo?** Recomendar como manter os
  dois volumes de dado (o pequeno atual, útil para teste rápido/CI, e o grande novo, para
  teste de carga) sem duplicar a estrutura do zero — ex: parametrizar por variável de
  ambiente/Airflow Variable qual dataset usar, em vez de criar uma pasta e um conjunto de
  DAGs paralelo para cada volume.
- **DAGs**: avaliar se faz sentido criar novas DAGs para o dataset grande (o que dobraria de
  4 para 8 DAGs) ou se é melhor parametrizar as 4 DAGs existentes para rodar com o volume
  pequeno ou grande sob demanda. Recomendar uma opção e justificar - eu tenho preferência por
  manter simples, mas quero ouvir o trade-off antes de decidir.

## Parte 2 — Geração dos dados fake

- Usar uma biblioteca adequada para geração de dados fake em Python (ex: `Faker`), com
  seed fixa de aleatoriedade (`Faker.seed()`/`random.seed()`) para o dataset ser
  reprodutível — gerar duas vezes deve produzir o mesmo resultado.
- Gerar um script Python (não notebook), organizado, reaproveitável, colocado em um local
  coerente com o resto do projeto (ex: `include/scripts/generate_fake_data.py` ou
  equivalente — sugerir o melhor local).
- **Manter integridade referencial**: todo `customer_id` em `raw_orders` deve existir em
  `raw_customers`; todo `product_id` em `raw_orders` deve existir em `raw_products`. Não
  gerar IDs soltos.
- **Distribuições realistas**, não puramente uniformes:
  - `signup_date` e `order_date` coerentes cronologicamente (pedido não pode ser anterior ao
    cadastro do cliente).
  - `status` do pedido com distribuição parecida com um e-commerce real (maioria
    `completed`, minoria `cancelled`/`returned`), mantendo proporção parecida com a amostra
    original.
  - `discount_pct` e `quantity` com variação plausível (não todo mundo comprando a mesma
    quantidade).
  - Alguns clientes com múltiplos pedidos e outros sem nenhum, e alguns produtos vendendo
    muito mais que outros — para o dataset servir de teste real para os testes dbt
    (`not_null`, `relationships`, etc.) e para os modelos de marts (`fct_orders`,
    `dim_customers`, `dim_products`) mostrarem números interessantes.
- Parâmetro de quantidade configurável no script (não hardcoded em 10.000.000), para eu poder
  gerar outros volumes no futuro sem reescrever o script.

## Parte 3 — Integração com o projeto

- Aplicar a decisão da Parte 1: se a recomendação for manter como seed, mostrar onde o CSV
  grande deve ficar e como o `dbt_project.yml`/DAGs devem apontar para ele; se a recomendação
  for migrar para um mecanismo de ingestão real, implementar esse mecanismo (task Python no
  Airflow que gera/carrega os dados antes do `dbt build`, mais o ajuste correspondente do
  `sources.yml`).
- Ajustar o que for necessário nas 4 DAGs (ou no ponto único de configuração, se optarmos por
  parametrizar) para rodar com o dataset grande sob demanda, sem quebrar a execução com o
  dataset pequeno atual.
- Rodar `dbt build`/`dbt test` com o novo volume e confirmar que os testes existentes
  continuam passando (nenhum teste deve falhar só por causa do volume, a menos que revele um
  problema real de qualidade de dado — nesse caso, reportar).
- Medir e reportar o novo tempo de execução das 4 combinações com o dataset grande, para
  comparar com os tempos já registrados com o dataset pequeno.

## Formato de entrega
1. Recomendação de arquitetura da Parte 1, com justificativa e fonte (documentação oficial
   do dbt sobre seeds), antes de qualquer código.
2. Script de geração de dados completo, comentado em português.
3. Os 3 CSVs gerados (ou instrução de como/onde eles são gerados, se forem grandes demais
   para mostrar por completo).
4. Diff de tudo que precisou mudar no projeto dbt/DAGs para suportar o novo volume.
5. Resultado da nova medição de tempo das 4 DAGs com 10.000.000 linhas.
6. Atualização do `README.md` com a decisão tomada e os novos números.

## Restrição
Não criar `seeds_10000000/`, DAGs duplicadas ou qualquer estrutura paralela improvisada sem
antes justificar por que essa é a abordagem correta (ou propor a alternativa melhor). Nenhuma
mudança na estrutura atual do projeto sem eu confirmar.