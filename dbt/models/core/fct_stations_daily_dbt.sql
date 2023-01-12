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
        date,
        avg(temp_max) as temp_max,
        avg(temp_min) as temp_min,
        avg(temp_avg) as temp_avg,
        sum(precipitation) as precipitation,
    from {{ ref('stg_years_unioned') }} 
    group by id, date
    )
select
    years.id,
    years.date,
    years.temp_max,
    years.temp_min,
    years.temp_avg,
    years.precipitation,
    stations.name as station_name,
    stations.latitude_longitude,
from average as years
inner join {{ ref('stg_stations') }} as stations
on years.id = stations.id




{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}