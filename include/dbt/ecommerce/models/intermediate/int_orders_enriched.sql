WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

enriched AS (
    SELECT 
        orders.order_id,
        orders.customer_id,
        orders.product_id,
        orders.order_date,
        orders.order_status,
        orders.quantity,
        products.unit_price,
        orders.discount_pct,
        round(
            orders.quantity * products.unit_price * (1 - orders.discount_pct), 
            2
        ) AS net_amount
    FROM orders
    LEFT JOIN products
        ON orders.product_id = products.product_id
)

SELECT * FROM enriched