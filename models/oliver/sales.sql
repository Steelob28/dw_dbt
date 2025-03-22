{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}


SELECT
d.date_id as date,
c.firstname as cust_first_name,
c.lastname as cust_last_name,
p.product_name,
s.store_name,
e.firstname as emp_first_name,
e.lastname as emp_last_name,
sf.quantity,
sf.unit_price
FROM {{ ref('fact_sales') }} sf
    
    LEFT JOIN {{ ref('oliver_dim_customer') }} c
        ON sf.cust_key = c.cust_key
    
    LEFT JOIN {{ ref('oliver_dim_date') }} d
        ON sf.date_key = d.date_key
    
    LEFT JOIN {{ ref('oliver_dim_employee') }} e
        ON sf.employee_key = e.employee_key

    LEFT JOIN {{ ref('oliver_dim_product') }} p 
        ON sf.product_key = p.product_key
    
    LEFT JOIN {{ ref('oliver_dim_store') }} s
        ON sf.store_key = s.store_key
    


