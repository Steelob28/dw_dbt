{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
)

SELECT
{{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
date_day as date,
year_number as year,
month_of_year as month,
day_of_month as day
from cte_date