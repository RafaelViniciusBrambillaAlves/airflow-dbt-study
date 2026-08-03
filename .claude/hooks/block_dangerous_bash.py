#!/usr/bin/env python3
"""
Hook PreToolUse (Bash).

Bloqueia (deny) comandos destrutivos/irreversiveis, e pede confirmacao
(ask) para comandos arriscados mas as vezes legitimos.

`deny` reservado para o que realmente nao tem volta (apagar arquivos,
reescrever historico do git, apagar o volume do Postgres). `ask` para o
que e as vezes necessario, so nao deve rodar sem eu perceber (full-refresh).
"""
import json
import re
import sys

# (regex, motivo) -> bloqueio total, sem opcao de "sim, continue"
DENY_PATTERNS = [
    (
        r"\brm\s+(-\w*\s+)*-\w*r\w*f\w*\b|\brm\s+(-\w*\s+)*-\w*f\w*r\w*\b",
        "'rm -rf' (ou variacao com as flags -r/-f combinadas) e destrutivo e "
        "irreversivel.",
    ),
    (
        r"\bgit\s+push\b[^\n]*(--force\b|-f\b)",
        "'git push --force' pode sobrescrever historico remoto de forma "
        "irreversivel.",
    ),
    (
        r"\bgit\s+reset\s+--hard\b",
        "'git reset --hard' descarta mudancas locais sem chance de "
        "recuperacao.",
    ),
    (
        r"\bdocker\s+volume\s+rm\b",
        "'docker volume rm' pode apagar postgres_warehouse_data (o "
        "warehouse Postgres do benchmark) ou o metastore do Airflow.",
    ),
    (
        r"\bdocker\s+system\s+prune\b",
        "'docker system prune' pode remover volumes/imagens usados pelo "
        "projeto sem aviso especifico.",
    ),
]

# (regex, motivo) -> pede confirmacao, nao bloqueia direto
ASK_PATTERNS = [
    (
        r"\bdbt\s+(run|build)\b[^\n]*--full-refresh\b",
        "'--full-refresh' reconstroi modelos incrementais do zero; confirme "
        "que isso e intencional antes de rodar.",
    ),
]

def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return

    tool_input = payload.get("tool_input")
    command = tool_input.get("command", "") or ""

    for pattern, reason in DENY_PATTERNS:
        if re.search(pattern, command):
            output = {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                }
            }
            print(json.dumps(output))
            return 

    for pattern, reason in ASK_PATTERNS:
        if re.search(pattern, command):
            output = {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "ask",
                    "permissionDecisionReason": reason,
                }
            }
            print(json.dumps(output))
            return 
