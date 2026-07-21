-- Teste singular (customizado): uma regra de negócio que nenhum teste genérico
-- consegue expressar.
-- Regra: um pedido completed deve sempre ter net_amount estritamente positivo.
-- Um teste genérico `not_null`/`accepted_values` não consegue verificar
-- "quantity * price * (1 - discount) > 0 para pedidos completed" — isso
-- precisa de SQL de verdade.
-- O dbt considera este teste REPROVADO se a query retornar alguma linha.

SELECT
    order_id,
    net_amount
FROM {{ ref('fct_orders') }}
WHERE order_status = 'completed'
    AND net_amount <= 0