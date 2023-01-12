{{ 
    config(
        materialized='table',
        cluster_by=['name']
    )
}}



select
    countries.name,
    avg(years.temp_avg) as temp_avg,
    max(temp_max) as temp_max,
    min(temp_min) as temp_min,
from {{ ref('stg_years_unioned') }} as years
inner join {{ ref('stg_stations') }} as stations
on years.id = stations.id
inner join {{ ref('stg_countries') }} as countries
on stations.country_code = countries.code
group by countries.name



{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}