Prompt: Nova DAG com dbt Fusion — `ecommerce_dbt_fusion_dag`

## Contexto
Já temos o projeto funcionando com dbt Core + Cosmos + DuckDB (DAG atual, `dbt_dag.py`),
com os modelos em `include/dbt/ecommerce/` (seeds `raw_customers`, `raw_products`,
`raw_orders` → staging → intermediate → marts) e testes genéricos e singulares já
configurados.

Agora quero adicionar o **dbt Fusion** (motor em Rust, mais rápido que o dbt Core em Python)
rodando **em paralelo** ao pipeline existente, para eu poder comparar os dois na prática —
não é para substituir o dbt Core, é para conviverem no mesmo projeto.

## O que preciso

### 1. Nova DAG
- `dag_id = "ecommerce_dbt_fusion_dag"`, em um novo arquivo (ex: `dags/dbt_fusion_dag.py`),
  seguindo exatamente o mesmo padrão de configuração da DAG atual (`ProjectConfig`,
  `ProfileConfig`, `RenderConfig`, mesmo `ExecutionMode` já validado como estável no projeto,
  schedule, tags, defaults) — só trocando o que for necessário para apontar para o Fusion.
- Mesmos dados (seeds), mesmas transformações (staging → intermediate → marts) e mesmos
  testes — é o mesmo projeto dbt, só executado por outro motor. Não duplicar modelos SQL
  a menos que seja estritamente necessário; explicar se dá para apontar as duas DAGs para o
  mesmo diretório `include/dbt/ecommerce/` ou se o Fusion exige uma estrutura própria.

### 2. Instalação do dbt Fusion no ambiente
- Ajustar o `Dockerfile` para instalar o dbt Fusion (binário via script oficial da dbt Labs,
  já que não é distribuído via pip como o dbt Core), sem quebrar a instalação existente do
  dbt Core — os dois precisam coexistir no mesmo container.
- Explicar a licença do Fusion (ELv2) e o que isso implica para uso comercial vs. estudo,
  já que é uma decisão relevante de arquitetura.
- Confirmar como o Cosmos identifica que deve chamar o binário do Fusion em vez do dbt Core
  nessa nova DAG (variável de ambiente, `dbt_executable_path`, ou outro mecanismo suportado).

### 3. Compatibilidade com o Cosmos
- Antes de implementar, verificar (com base em documentação/changelog oficial, não suposição)
  qual `ExecutionMode` do Cosmos tem suporte confirmado para dbt Fusion hoje, e se há
  limitação conhecida em relação ao que já usamos com o dbt Core.
- Se houver qualquer funcionalidade que funciona no Core e não é suportada (ou é
  experimental) no Fusion via Cosmos, avisar explicitamente antes de implementar, em vez de
  simular que funciona.

### 4. Profile do dbt para o Fusion
- Definir se o Fusion usa o mesmo `profiles.yml`/target DuckDB já existente ou se precisa de
  configuração própria, e implementar a que for correta.

### 5. Documentação
- Atualizar o `README.md`: nova seção explicando a existência das duas DAGs, por que fazem
  sentido coexistir num projeto de estudo, e o resultado da comparação de performance.
- Comentários no código da nova DAG em português, seguindo o padrão já usado no resto do
  projeto.

## Formato de entrega
1. Confirmação de compatibilidade Cosmos + dbt Fusion (com fonte) antes de qualquer código.
2. Diff do `Dockerfile`.
3. Conteúdo completo de `dags/dbt_fusion_dag.py`.
4. Ajustes necessários em `profiles.yml` (se houver), com diff.
5. Comandos para rodar e validar a nova DAG localmente.

## Restrição
Se algo não for suportado hoje pelo Cosmos com Fusion, não improvisar um workaround frágil —
explicar a limitação, propor a alternativa mais próxima do padrão oficial, e perguntar antes
de aplicar.
Considerar melhores práticas da documentação aqui `https://docs.getdbt.com/docs/local/connect-data-platform/duckdb-setup?version=2.0&name=Fusion`