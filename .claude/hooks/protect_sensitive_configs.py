#!/usr/bin/env python3
"""
Hook PreToolUse (Edit / Write / MultiEdit).

Pede confirmacao explicita (permissionDecision="ask") antes de sobrescrever
arquivos de configuracao ja existentes e sensiveis do projeto. Diferente do
`.env` (que tem hook proprio e e negado direto), esses arquivos sao editaveis
com confirmacao, porque editar profiles.yml/dbt_project.yml/uma DAG e uma
tarefa legitima e frequente neste projeto - so nao deve acontecer "sem eu
perceber".

Arquivos protegidos:
  - include/dbt/ecommerce/profiles.yml
  - include/dbt/ecommerce/dbt_project.yml
  - docker-compose.override.yml
  - airflow_settings.yaml
  - dags/*.py (as DAGs de benchmark + ingestao)
"""

import fnmatch
import json
import os
import sys

SENSITIVE_EXACT = {
    "include/dbt/ecommerce/profiles.yml",
    "include/dbt/ecommerce/dbt_project.yml",
    "docker-compose.override.yml",
    "airflow_settings.yaml",
}

SENSITIVE_GLOBS = {
    "dags/*.py"
}

def _rel_path(file_path: str) -> str:
    cwd = os.getcwd()

    try:
        return os.path.realpath(file_path, cwd).replace(os.sep, "/")
    except Exception as e:
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

    is_sensitive = rel in SENSITIVE_EXACT or any(
        fnmatch.fnmatch(rel, pattern) for pattern in SENSITIVE_GLOBS
    )
    if not is_sensitive:
        return 

    output = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": (
                f"'{rel}' e um arquivo de configuracao sensivel do projeto "
                "(profiles.yml, dbt_project.yml, docker-compose.override.yml, "
                "airflow_settings.yaml ou uma das DAGs em dags/). Confirme "
                "explicitamente antes de sobrescrever."
            ),
        }
    }
    print(json.dumps(output))

if __name__ == "__main__":
    main()