{#
    ==============================================================================
    MACRO: test_expression_is_true
    ==============================================================================
    OBJETIVO:
    Teste customizado para validar se uma regra de negócio (expressão SQL) 
    é verdadeira para todas as linhas de uma tabela. Compatível com dbt-fusion.

    O QUE FAZ:
    Ele monta uma query procurando linhas onde a expressão passada seja FALSA 
    (usando a cláusula WHERE NOT). Se encontrar alguma linha onde a regra falha, 
    o teste do dbt falha.

    PARÂMETROS:
    - model (Relation): A tabela/view que está sendo testada.
    - arguments (dict ou string): Pode vir como dicionário (novo formato) ou 
      string (formato antigo). Espera a chave 'expression'.
      Ex: {'expression': 'lifetime_value >= 0'}
    - column_name (str, opcional): Capturado para evitar erros de sintaxe se o 
      teste for declarado no nível da coluna, mas não é usado na lógica deste macro.

    RETORNO:
    Uma query SQL (SELECT *) retornando as linhas onde a expressão é falsa.
    ==============================================================================
#}
{% macro test_expression_is_true(model, arguments, column_name=None) %}

     {# Define um valor padrão seguro para não gerar SQL vazio #}
    {% set expr = 'true' %}

    {# Tenta pegar do kwargs (variável especial do Jinja para dbt-fusion) #}
    {% if kwargs is defined and kwargs is mapping %}
        {% if kwargs.get('expression') %}{% set expr = kwargs.get('expression') %}{% endif %}
    {% endif %}
    
    {# Lida com o formato arguments (dbt 1.8+ / Core) #}
    {% if arguments is defined and arguments is mapping %}
        {% if arguments.get('expression') %}{% set expr = arguments.get('expression') %}{% endif %}
    {% elif arguments is string and arguments %}
        {% set expr = arguments %}
    {% endif %}


    select *
    from {{ model }}
    where not ({{ expr }})

{% endmacro %}