with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

enriched as (
    select
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
        ) as net_amount
    from orders
    left join products
        on orders.product_id = products.product_id
)

select * from enriched
