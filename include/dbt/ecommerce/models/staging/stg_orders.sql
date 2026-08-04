with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        product_id,
        CAST(quantity as INTEGER) as quantity,
        CAST(discount_pct as DECIMAL(4, 2)) as discount_pct,
        LOWER(TRIM(status)) as order_status,
        CAST(order_date as DATE) as order_date
    from source
)

select * from renamed
