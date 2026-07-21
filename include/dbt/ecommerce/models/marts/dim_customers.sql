WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(net_amount) FILTER (WHERE order_status = 'completed') AS lifetime_value
    FROM {{ ref('int_orders_enriched') }}
    GROUP BY customer_id
)

SELECT
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customers.email,
    customers.signup_date,
    COALESCE(order_stats.total_orders, 0) AS total_orders,
    COALESCE(order_stats.lifetime_value, 0) AS lifetime_value
FROM customers
LEFT JOIN order_stats
    ON customers.customer_id = order_stats.customer_id