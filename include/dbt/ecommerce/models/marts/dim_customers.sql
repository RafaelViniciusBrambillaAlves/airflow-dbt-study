with customers as (
    select * from {{ ref('stg_customers') }}
),

order_stats as (
    select
        customer_id,
        COUNT(distinct order_id) as total_orders,
        SUM(net_amount) filter (where order_status = 'completed')
            as lifetime_value
    from {{ ref('int_orders_enriched') }}
    group by customer_id
)

select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customers.email,
    customers.signup_date,
    COALESCE(order_stats.total_orders, 0) as total_orders,
    COALESCE(order_stats.lifetime_value, 0) as lifetime_value
from customers
left join order_stats
    on customers.customer_id = order_stats.customer_id
