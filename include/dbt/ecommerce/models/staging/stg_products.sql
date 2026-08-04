with source as (
    select * from {{ source('raw', 'raw_products') }}
),

renamed as (
    select
        product_id,
        TRIM(product_name) as product_name,
        TRIM(category) as category,
        CAST(unit_price as DECIMAL(10, 2)) as unit_price
    from source
)

select * from renamed
