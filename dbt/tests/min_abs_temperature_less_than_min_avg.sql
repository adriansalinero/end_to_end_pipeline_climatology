-- The absolute minimum temperature must be lower than the average of the minimum temperature
select
    id,
    date,
from {{ ref('fct_stations_monthly_dbt' )}}
where temp_min_avg < temp_min_abs