{#
    ==============================================================================
    MACRO: test_relationships
    ==============================================================================
    OBJETIVO:
    Sobrescreve o teste nativo "relationships" do dbt para garantir compatibilidade 
    com o novo formato de sintaxe exigido pelo dbt-fusion (que exige o bloco 
    "arguments"), mantendo compatibilidade reversa com o dbt-core 1.9+.

    O QUE FAZ:
    Valida a integridade referencial (chave estrangeira). Ele busca registros na 
    tabela "filho" (model) cuja chave estrangeira existe, mas não possui 
    correspondência na tabela "pai" (parent_model). Se retornar > 0 linhas, o teste falha.

    PARÂMETROS:
    - model (Relation): A tabela/view que está sendo testada (tabela filho).
    - arguments (dict): Dicionário contendo as configurações do teste.
      Espera as chaves:
        - 'to': Refers to ref() da tabela pai (ex: ref('stg_customers')).
        - 'field': Nome da coluna na tabela pai que será comparada.
    - column_name (str, opcional): Nome da coluna na tabela filho. Se o teste 
      for declarado no nível da coluna no YAML, o dbt injeta isso automaticamente. 
      Se não, tenta buscar no arguments.

    RETORNO:
    Uma query SQL (SELECT) que retorna as linhas órfãs (que violam o relacionamento).
    ==============================================================================
#}
{% macro test_relationships(model, arguments, column_name=None) %}

    {% set parent_model = none %}
    {% set parent_field = none %}

    {# Tenta pegar do kwargs (variável especial do Jinja para dbt-fusion) #}
    {% if kwargs is mapping %}
        {% set parent_model = kwargs.get('to') %}
        {% set parent_field = kwargs.get('field') %}
    {% endif %}

    {# Sobrescreve com o arguments se existir (formato dbt-core) #}
    {% if arguments is mapping %}
        {% set parent_model = arguments.get('to', parent_model) %}
        {% set parent_field = arguments.get('field', parent_field) %}
    {% endif %}

    {% set child_field = column_name if column_name else parent_field %}

    select child.{{ child_field }}
    from {{ model }} as child
    left join {{ parent_model }} as parent
      on child.{{ child_field }} = parent.{{ parent_field }}
    where child.{{ child_field }} is not null
      and parent.{{ parent_field }} is null

{% endmacro %}