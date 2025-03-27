{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

select
{{ dbt_utils.generate_surrogate_key(['p.productid', 'p.productname']) }} as product_key,
p.productid,
p.productname,
p.productcalories,
p.productcost,
s.length,
s.breadtype
FROM {{ source('samssubs_landing', 'product') }} p
    LEFT JOIN {{ source('samssubs_landing', 'sandwich') }} s
        ON p.productid = s.productid
