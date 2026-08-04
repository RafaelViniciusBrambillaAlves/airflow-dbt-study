-- Tabela fato: uma linha por item de pedido, pronta para
-- ferramentas de BI.
-- A granularidade e as chaves estão documentadas no schema.yml
-- ao lado deste arquivo.

select
    order_id,
    customer_id,
    product_id,
    order_date,
    order_status,
    quantity,
    unit_price,
    discount_pct,
    net_amount
from {{ ref('int_orders_enriched') }}
