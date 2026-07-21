# Prompt de continuação: documentação e idioma

Já temos o projeto criado com esta estrutura:

```
airflow-dbt-study/
├── Dockerfile
├── requirements.txt
├── packages.txt
├── .env.example
├── .gitignore
├── dags/
│   └── dbt_dag.py
├── plugins/
│   └── .gitkeep
└── include/dbt/ecommerce/
    ├── dbt_project.yml
    ├── packages.yml
    ├── profiles.yml
    ├── seeds/
    │   ├── raw_customers.csv
    │   ├── raw_products.csv
    │   └── raw_orders.csv
    ├── models/
    │   ├── staging/
    │   │   ├── _staging__sources.yml
    │   │   ├── _staging__models.yml
    │   │   ├── stg_customers.sql
    │   │   ├── stg_products.sql
    │   │   └── stg_orders.sql
    │   ├── intermediate/
    │   │   ├── _intermediate__models.yml
    │   │   └── int_orders_enriched.sql
    │   └── marts/
    │       ├── _marts__models.yml
    │       ├── dim_customers.sql
    │       ├── dim_products.sql
    │       └── fct_orders.sql
    └── tests/
        └── assert_positive_order_amounts.sql
```

Agora preciso de dois ajustes, sem alterar a lógica/funcionamento do que já foi construído, como o esse repositório é de um projeto de estudo é muito importante você prestar atenção:

## 1. Criar um `README.md` na raiz do projeto, separado dos arquivos de código

Esse README deve documentar o projeto como se fosse a documentação oficial de um repositório
profissional, com as seguintes seções:

- **Visão geral**: o que o projeto faz.
- **Stack utilizada**: lista das tecnologias (Airflow, Astro CLI, dbt, Cosmos, banco de dados
  escolhido) com uma frase dizendo o papel de cada uma.
- **Arquitetura**: um diagrama em texto (ASCII ou Mermaid) mostrando o fluxo:
  seeds → staging → intermediate → marts, e onde o Airflow/Cosmos entra nesse fluxo.
- **Explicação**: Como é um projeto para estudo, faça uma explicação de cada parte do Airflow(dags, fluxos) e Dbt(models, seeds, staging, intermediate, marts, tests)  
- **Decisões de arquitetura e o porquê**: uma subseção por decisão, explicando o problema que
  ela resolve e qual seria a alternativa mais simples/ingênua. Cobrir pelo menos:
  - Por que Astro CLI em vez de docker-compose puro
  - Por que Cosmos em vez de `BashOperator` chamando `dbt run` direto
  - Por que a divisão em camadas staging/intermediate/marts
  - Por que o banco de dados escolhido foi o adequado para este caso
  - Como os testes do dbt (genéricos e o singular test) se encaixam no pipeline e o que cada
    um está validando
- **Estrutura de pastas**: uma tabela curta explicando o propósito de cada diretório
  (`dags/`, `include/dbt/`, `plugins/`, etc.).
- **Como rodar o projeto localmente**: passo a passo com os comandos exatos
  (`astro dev start`, como rodar `dbt build`/`dbt test` manualmente para debug, como acessar a
  UI do Airflow, como ver a documentação gerada pelo dbt).
- **Próximos passos**: uma lista curta do que ainda pode ser evoluído (dados reais, CI/CD,
  incremental models, dbt Fusion, etc.), já que este é um projeto de estudo em evolução.

## 2. Traduzir todos os comentários de código para português

- Percorrer todos os arquivos do projeto (`.py`, `.sql`, `.yml`) e traduzir para português
  qualquer comentário que esteja em inglês.
- Não traduzir nomes de variáveis, colunas, modelos, chaves de configuração do Airflow/dbt ou
  qualquer identificador técnico — só os comentários explicativos.
- Manter o mesmo nível de detalhe, só mudando o idioma.
- Ao final, listar quais arquivos foram alterados nesta etapa.

## Formato de entrega
1. Primeiro o `README.md` completo.
2. Depois, para cada arquivo que teve comentário traduzido, mostrar o diff ou o trecho alterado
   (não precisa reescrever o arquivo inteiro se a mudança for só no comentário).