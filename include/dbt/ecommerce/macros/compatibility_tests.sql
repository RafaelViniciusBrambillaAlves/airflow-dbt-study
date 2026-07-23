{% macro test_relationships(model, arguments, column_name=None) %}
    {#
        Sobrescreve o relationships nativo para aceitar o novo formato 'arguments'
        exigido pelo dbt-fusion, mantendo compatibilidade com o dbt-core 1.9.
    #}
    {% set parent_model = arguments['to'] %}
    {% set parent_field = arguments['field'] %}
    {% set child_field = column_name if column_name else arguments['field'] %}

    select child.{{ child_field }}
    from {{ model }} as child
    left join {{ parent_model }} as parent
      on child.{{ child_field }} = parent.{{ parent_field }}
    where child.{{ child_field }} is not null
      and parent.{{ parent_field }} is null

{% endmacro %}


{% macro test_accepted_values(model, arguments, column_name=None) %}
    {#
        Sobrescreve o accepted_values nativo para aceitar o novo formato 'arguments'
        exigido pelo dbt-fusion, mantendo compatibilidade com o dbt-core 1.9.
    #}
    {% set values = arguments['values'] %}
    {% set field = column_name if column_name else arguments['field'] %}

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