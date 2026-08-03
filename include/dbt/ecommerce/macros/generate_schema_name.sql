{#
    Fixa explicitamente a convenção de schema do projeto em vez de depender do
    default implícito do dbt-core. O default real (macro embutido) já concatena
    "{target.schema}_{custom_schema}" sem sufixo de usuário/dev - isso só
    aconteceria com generate_schema_name_for_env, nunca usado aqui. Este macro
    não altera o schema atual (analytics_core_raw, analytics_core_staging, ...),
    só o torna explícito e estável entre versões maiores do dbt-core.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}