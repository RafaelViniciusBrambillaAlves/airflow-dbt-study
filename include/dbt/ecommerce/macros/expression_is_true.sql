{% macro test_expression_is_true(model, arguments, column_name=None) %}

    {#
        Lida com a compatibilidade do novo formato 'arguments' do dbt 1.8+/Fusion.
        Se 'arguments' for um dict (novo formato), extraímos a 'expression'.
        'column_name' é capturado para evitar erros quando o teste
        é usado no nível de coluna no YAML.
    #}

    {% if arguments is mapping %}
        {% set expr = arguments.get('expression', 'true') %}
    {% else %}
        {% set expr = arguments %}
    {% endif %}

    select *
    from {{ model }}
    where not ({{ expr }})

{% endmacro %}