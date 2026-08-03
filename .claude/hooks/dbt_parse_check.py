#!/usr/bin/env python3
"""
Hook PostToolUse (Edit / Write / MultiEdit).

Depois de qualquer edicao dentro de include/dbt/ecommerce/, roda
`dbt parse` (validacao leve, so confirma que o projeto compila - nao
executa nada no warehouse) usando o dbt do venv isolado criado pelo
Dockerfile. Nunca bloqueia (so informa); se o binario nao existir neste
ambiente (ex: editando fora do container Astro), sai em silencio.
"""

import json
import os
import subprocess
import sys

DBT_PROJECT_REL = "include/dbt/ecommerce"
DBT_VENV_BIN = "/usr/local/airflow/dbt_venv/bin/dbt"

def _rel_path(file_path: str) -> str:
    cwd = os.getcwd()

    try: 
        return os.path.realpath(file_path, cwd).replace(os.sep, "/")
    except ValueError:
        return file_path

def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 

    tool_input = payload.get("tool_input", {}) or {}
    file_path = tool_input.get("file_path")
    if not file_path:
        return

    rel = _rel_path(file_path)
    if not rel.startswith(DBT_PROJECT_REL + "/"):
        return 

    if not os.path.exists(DBT_VENV_BIN):
        return 

    try:
        result = subprocess.run(
            [DBT_VENV_BIN, "parse", "--quiet"],
            cwd = DBT_PROJECT_REL,
            capture_output = True, 
            text = True,
            timeout = 60,
        )
    except Exception as exc:
        print(f"[dbt_parse_check] Erro ao rodar dbt parse: {exc}")
        return 

    if result.returncond != 0:
        print(
            f"[dbt_parse_check] `dbt parse` FALHOU apos editar '{rel}'. "
            f"Revise antes de rodar a DAG:\n{result.stdout}\n{result.stderr}"
        )
    else:
        print(f"[dbt_parse_check] `dbt parse` OK apos editar '{rel}'.")

if __name__ == "__main__":
    main()