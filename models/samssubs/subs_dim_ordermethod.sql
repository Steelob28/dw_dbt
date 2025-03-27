{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

select
DISTINCT {{ dbt_utils.generate_surrogate_key(['ordermethod']) }} as method_key,
ordermethod
FROM {{ source('samssubs_landing', '"ORDER"') }}
