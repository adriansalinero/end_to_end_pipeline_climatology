{{ 
    config(
        materialized='table',
        partition_by={
            "field": "DATE_TRUNC(date, YEAR)"
        },  
        cluster_by=['name']
    )
}}



select
    countries.name,
    DATE_TRUNC(years.date, YEAR) as date,
    avg(years.temp_avg) as temp_avg,
from {{ ref('stg_years_unioned') }} as years
inner join {{ ref('stg_stations') }} as stations
on years.id = stations.id
inner join {{ ref('stg_countries') }} as countries
on stations.country_code = countries.code
group by countries.name, DATE_TRUNC(years.date, YEAR)



{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}