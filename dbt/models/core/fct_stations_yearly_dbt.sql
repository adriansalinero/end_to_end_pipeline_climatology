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
        DATE_TRUNC(date, YEAR) as date,
        avg(temp_avg) as temp_avg,
        avg(precipitation) as precipitation,
    from {{ ref('stg_years_unioned') }} 
    group by id, DATE_TRUNC(date, YEAR)
    )
select
    years.id,
    years.date,
    years.temp_avg,
    stations.name as station_name,
    stations.latitude_longitude,
from average as years
inner join {{ ref('stg_stations') }} as stations
on years.id = stations.id




{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}