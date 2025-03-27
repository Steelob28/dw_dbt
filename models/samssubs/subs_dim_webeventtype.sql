{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

select
DISTINCT {{ dbt_utils.generate_surrogate_key(['event_name']) }} as eventtype_key,
event_name
FROM {{ source('samssubs_landing', 'web_traffic_events') }}