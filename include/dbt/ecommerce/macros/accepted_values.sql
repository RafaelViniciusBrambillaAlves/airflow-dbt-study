{#
    ==============================================================================
    MACRO: test_accepted_values
    ==============================================================================
    OBJETIVO:
    Sobrescreve o teste nativo "accepted_values" para compatibilidade com o 
    dbt-fusion e dbt-core 1.9+.

    O QUE FAZ:
    Verifica se todos os valores de uma coluna específica estão contidos em uma 
    lista de valores permitidos. Se encontrar algum valor fora da lista, retorna 
    essa linha (o que faz o teste falhar).

    PARÂMETROS:
    - model (Relation): A tabela/view que está sendo testada.
    - arguments (dict): Dicionário contendo as configurações do teste.
      Espera as chaves:
        - 'values': Lista de valores permitidos (ex: ['completed', 'pending']).
        - 'field' (opcional): Nome da coluna, caso o teste seja no nível do modelo.
    - column_name (str, opcional): Nome da coluna injetado pelo dbt se o teste 
      for declarado dentro da coluna no YAML.

    RETORNO:
    Uma query SQL (SELECT) contendo apenas os valores que violam a regra 
    (ou seja, que não estão na lista de valores aceitos).
    ==============================================================================
#}
{% macro test_accepted_values(model, arguments, column_name=None) %}
    
    {% set values = none %}
    {% set field = column_name %}

    {# Tenta pegar do kwargs (variável especial do Jinja para dbt-fusion) #}
    {% if kwargs is mapping %}
        {% set values = kwargs.get('values', values) %}
        {% if not field and kwargs.get('field') %}
            {% set field = kwargs.get('field') %}
        {% endif %}
    {% endif %}

    {# Sobrescreve com o arguments se existir (formato dbt-core 1.9+) #}
    {% if arguments is mapping %}
        {% set values = arguments.get('values', values) %}
        {% if not field and arguments.get('field') %}
            {% set field = arguments.get('field') %}
        {% endif %}
    {% endif %}

    with all_values as (
        select {{ field }} as value_field
        from {{ model }}
        where {{ field }} is not null
    ),
    validation_errors as (
        select value_field
        from all_values
        where value_field not in (
            {% for value in values -%}
                '{{ value }}'
                {%- if not loop.last %}, {% endif -%}
            {%- endfor %}
        )
    )
    select * from validation_errors

{% endmacro %}