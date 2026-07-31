# Prompt: Guardrails do projeto via hooks + settings.json (Claude Code)

## Contexto
Quero implementar guardrails reais (não só instrução em prompt) para quando eu ou o Claude
Code formos mexer neste projeto (`airflow-dbt-study/`). Já tive o hábito de pedir "nunca
sobrescreva configuração existente sem confirmar" via texto, mas isso depende do modelo
lembrar — quero migrar as regras não-negociáveis para o mecanismo de **hooks** do Claude Code,
que são determinísticos (script decide via exit code, não depende do modelo "lembrar").

## O que preciso

### 1. Estrutura de settings
- Criar/ajustar `.claude/settings.json` na raiz do projeto (versionado no git, compartilhado
  caso eu use em outra máquina), contendo os hooks e permissões não-negociáveis deste projeto.
- Explicar a diferença entre `.claude/settings.json` (compartilhado/commitado) e
  `.claude/settings.local.json` (pessoal, não commitado) — usar `settings.local.json` para
  qualquer coisa específica da minha máquina (paths locais, por exemplo), e o `settings.json`
  só para regras que fazem sentido para qualquer pessoa trabalhando neste repo.
- Adicionar `.claude/settings.local.json` ao `.gitignore` se ainda não estiver.

### 2. Guardrails de `PreToolUse` (bloquear antes de executar)
Implementar hooks que **bloqueiam** (exit code 2, com mensagem explicando o motivo) as
seguintes ações, coerentes com as regras que já uso neste projeto:
- Qualquer edição/sobrescrita de arquivos de configuração já existentes e "sensíveis" do
  projeto sem confirmação explícita meu — priorizar: `profiles.yml`, `dbt_project.yml`,
  `docker-compose.override.yml`, `airflow_settings.yaml`, `.env` (não o `.env.example`), e as
  4 DAGs (`dags/*.py`).
- Comandos destrutivos no Bash: `rm -rf`, `git push --force`, `git reset --hard`, `docker
  volume rm`/`docker system prune` (evitar apagar o volume do Postgres sem eu saber),
  `dbt run --full-refresh` sem confirmação (se aplicável ao fluxo).
- Qualquer leitura/edição de `.env` real (não o `.env.example`) — mesmo que só para leitura,
  negar por padrão, já que pode conter credenciais.
- Explicar exatamente como o hook identifica o comando/arquivo (campo do JSON recebido via
  stdin no evento `PreToolUse`) e como escrever o `matcher` correto para cada caso (Bash vs.
  Edit/Write).

### 3. Guardrails de `PostToolUse` (validar depois de executar)
- Hook que roda `dbt parse` (ou equivalente leve) depois de qualquer edição em
  `include/dbt/ecommerce/`, para eu saber imediatamente se uma mudança quebrou o projeto dbt,
  antes mesmo de rodar a DAG.
- Hook opcional de lint/formatação para arquivos Python das DAGs e scripts de ingestão, se eu
  já tiver ou quiser adotar um formatter (perguntar antes de introduzir uma ferramenta nova
  no projeto).

### 4. Regra geral de confirmação
- Para qualquer ação que normalmente seria bloqueada pelos hooks acima mas que eu realmente
  quero fazer, qual é o fluxo correto: usar `permissionDecision: "ask"` no hook (que escala
  para um prompt de confirmação) em vez de negar (`deny`) direto sempre — reservar `deny`
  puro para ações realmente irreversíveis/perigosas (ex: `rm -rf`), e `ask` para as demais
  (ex: editar `dbt_project.yml`).

### 5. Segurança dos próprios hooks
- Os hooks rodam com minhas permissões de usuário, sem sandbox — então os scripts devem ser
  simples, explícitos e auditáveis (nada de baixar/executar código externo dentro do hook).
- Como qualquer coisa que puder escrever no `.claude/settings.json` pode plantar um hook que
  roda automaticamente (inclusive em `SessionStart`), reforçar que esse arquivo deve ser
  revisado como código de CI: qualquer mudança nele passa por revisão minha antes de eu
  aceitar, mesmo sendo eu mesmo quem geralmente edita.

## Formato de entrega
1. `.claude/settings.json` completo, com os hooks comentados explicando o que cada um faz.
2. Os scripts de hook (shell ou Python, o que for mais simples e auditável), separados em
   arquivos próprios (ex: `.claude/hooks/`), não inline gigante dentro do JSON.
3. Uma tabela resumindo: evento (`PreToolUse`/`PostToolUse`), o que dispara o hook, e a ação
   (`deny`/`ask`/`allow` + log).
4. Instruções de como testar cada guardrail (ex: tentar editar `.env` de propósito e confirmar
   que é bloqueado, com `claude --debug` para ver o hook disparando).
5. Atualizar o `README.md` com uma seção curta explicando que o projeto usa hooks para reforçar
   as regras de "nunca sobrescrever configuração sem confirmação".

## Restrição
Não implementar nenhum hook que bloqueie silenciosamente sem mensagem clara do motivo — toda
negação deve devolver uma explicação legível para mim (e para o modelo, que vai ver o stderr).
Não adicionar hooks "extras" além do que pedi sem antes propor e eu confirmar.