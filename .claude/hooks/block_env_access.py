#!/usr/bin/env python3
"""
Hook PreToolUse (Read / Edit / Write / MultiEdit).

Nega por padrao qualquer acesso - leitura ou escrita - ao `.env` real do
projeto, ja que ele contem credenciais reais (POSTGRES_PASSWORD etc). O
`.env.example` continua livre, pois nunca tem segredo de verdade.

Regra: `deny` puro, sem "ask", porque nao ha cenario legitimo em que o
modelo precise ler ou editar credenciais diretamente - se precisar de um
valor especifico, o usuario compartilha manualmente.
"""

import json
import os
import sys

def _real_path(file_path: str) -> str:
    cwd = os.getcwd()

    try:
        return os.path.realpath(file_path, cwd).replace(os.sep, "/")
    except Exception as e:
        return file_path

def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception as e:
        return 

    tool_input = payload.get("tool_input", {}) or {}
    file_path = tool_input.get("file_path")
    if not file_path:
        return 

    basename = os.path.basename(file_path)

    if basename == ".env":
        output = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    "'.env' contem credenciais reais (ex: POSTGRES_PASSWORD) e e "
                    "negado por padrao para leitura/edicao. Use '.env.example' como "
                    "referencia de estrutura, ou peca ao usuario o valor especifico "
                    "que precisar."
                ),
            }
        }
        print(json.dumps(output))

if __name__ == "__main__":
    main()