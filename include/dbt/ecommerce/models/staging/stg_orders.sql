WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(discount_pct AS DECIMAL(4, 2)) AS discount_pct,
        LOWER(TRIM(status)) AS order_status,
        CAST(order_date AS date) AS order_date
    FROM source
)

SELECT * FROM renamed