-- Staging: 1:1 with the source table. Only renaming, casting and light
-- cleaning happen here — no joins, no business logic.
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_customers') }}
),

renamed AS (
    SELECT 
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        CAST(signup_date AS date) AS signup_date
    FROM source
)

SELECT * FROM renamed