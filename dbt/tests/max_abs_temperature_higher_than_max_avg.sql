-- The absolute maximum temperature must be higher than the average of the maximum temperature
select
    id,
    date,
from {{ ref('fct_stations_monthly_dbt' )}}
where temp_max_avg > temp_max_abs