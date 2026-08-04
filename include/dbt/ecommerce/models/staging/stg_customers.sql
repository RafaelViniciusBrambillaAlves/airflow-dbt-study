-- Staging: 1:1 com a tabela de origem. Aqui só acontece renomeação, cast e
-- limpeza leve - sem joins, sem lógica de negócio.

with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

renamed as (
    select
        customer_id,
        TRIM(first_name) as first_name,
        TRIM(last_name) as last_name,
        LOWER(TRIM(email)) as email,
        CAST(signup_date as date) as signup_date
    from source
)

select * from renamed
