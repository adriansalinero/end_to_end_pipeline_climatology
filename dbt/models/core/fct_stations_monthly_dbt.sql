{{ 
    config(
        materialized='table',
        partition_by={
            "field": "DATE_TRUNC(date, YEAR)"
        },        
        cluster_by=['id']
    )
}}


with average as
(
    select
        id,
        DATE_TRUNC(date, MONTH) as date,
        max(temp_max) as temp_max_abs,
        avg(temp_max) as temp_max_avg,
        avg(temp_avg) as temp_avg_avg,
        avg(temp_min) as temp_min_avg,
        min(temp_min) as temp_min_abs,
        sum(precipitation) as precipitation_total,
        sum(case when precipitation > 1 then 1 else 0 end) as precipitation_days,
    from {{ ref('stg_years_unioned') }} 
    group by id, DATE_TRUNC(date, MONTH)
    )
select
    years.id,
    years.date,
    years.temp_max_abs,
    years.temp_max_avg,
    years.temp_avg_avg,
    years.temp_min_avg,
    years.temp_min_abs,
    years.precipitation_total,
    years.precipitation_days,   
    stations.name as station_name,
    stations.latitude_longitude,
from average as years
inner join {{ ref('stg_stations') }} as stations
on years.id = stations.id




{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}