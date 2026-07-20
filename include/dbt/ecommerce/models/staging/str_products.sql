WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_products') }}
),

renamed AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        CAST(unit_price AS DECIMAL(10, 2)) AS unit_price
    FROM source
)

SELECT * FROM renamed