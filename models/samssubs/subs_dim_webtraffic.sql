{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

select
DISTINCT {{ dbt_utils.generate_surrogate_key(['traffic_source']) }} as trafficsource_key,
traffic_source
FROM {{ source('samssubs_landing', 'web_traffic_events') }}