{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

select
c.customer_key,
e.employee_key,
p.product_key,
d.date_key,
s.store_key,
om.method_key,
od.orderlineqty as quantity,
od.orderlineprice as price
FROM {{ source('samssubs_landing', '"ORDER"') }} o
    INNER JOIN {{source('samssubs_landing', 'orderdetails') }} od
        ON o.ordernumber = od.ordernumber
    INNER JOIN {{source('samssubs_landing', 'employee') }} es
        ON o.employeeid = es.employeeid
    INNER JOIN {{ ref('subs_dim_customer') }} c
        ON o.customerid = c.customerid
    INNER JOIN {{ ref('subs_dim_employee') }} e 
        ON o.employeeid = e.employeeid
    INNER JOIN {{ ref('subs_dim_product') }} p
        ON od.productid = p.productid
    INNER JOIN {{ ref('subs_dim_date') }} d
        ON o.orderdate = d.date
    INNER JOIN {{ ref('subs_dim_store') }} s
        ON es.storeid = s.storeid
    INNER JOIN {{ ref('subs_dim_ordermethod') }} om
        ON o.ordermethod = om.ordermethod
