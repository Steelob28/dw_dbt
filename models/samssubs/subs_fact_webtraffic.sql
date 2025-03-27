{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

select
wt.trafficsource_key,
p.webpage_key,
e.eventtype_key,
d.date_key,
count(*) as interationcount
FROM {{ source('samssubs_landing', 'web_traffic_events') }} wts 
    INNER JOIN {{ ref('subs_dim_webtraffic') }} wt
        ON wts.traffic_source = wt.traffic_source
    INNER JOIN {{ ref('subs_dim_webpage') }} p
        ON wts.page_url = p.page_url
    INNER JOIN {{ ref('subs_dim_webeventtype') }} e
        ON wts.event_name = e.event_name
    INNER JOIN {{ ref('subs_dim_date') }} d
        ON CAST(wts.event_timestamp AS DATE) = d.date
GROUP BY ALL
