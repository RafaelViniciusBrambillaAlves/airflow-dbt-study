-- Staging: 1:1 com a tabela de origem. Aqui só acontece renomeação, cast e
-- limpeza leve - sem joins, sem lógica de negócio.

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