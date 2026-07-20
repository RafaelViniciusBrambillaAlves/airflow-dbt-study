-- Singular (custom) test: a business rule that no generic test can express.
-- Rule: a completed order must always have a strictly positive net_amount.
-- A generic `not_null`/`accepted_values` test can't check "quantity * price
-- * (1 - discount) > 0 for completed orders" — that needs real SQL.
-- dbt considers this test FAILED if the query returns any rows.
SELECT
    order_id,
    net_amount
FROM {{ ref('fct_orders') }}
WHERE order_status = 'completed'
    AND net_amount <= 0