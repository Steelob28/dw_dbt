{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

select
{{ dbt_utils.generate_surrogate_key(['customerlname', 'customerid']) }} as customer_key,
customerid,
customerfname,
customerlname,
customerbday,
customerphone
FROM {{ source('samssubs_landing', 'customer') }}