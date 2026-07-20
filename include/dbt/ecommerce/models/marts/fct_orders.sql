-- Fact table: one row per order line, ready for BI tools. Grain and keys
-- are documented in the schema.yml right next to this file.

SELECT 
    order_id,
    customer_id,
    product_id,
    order_date,
    order_status,
    quantity,
    unit_price,
    discount_amount,
    net_amount
FROM {{ ref('int_orders_enriched') }}