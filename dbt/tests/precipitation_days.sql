-- Precipitation days can't be more than 31 per month
select
    precipitation_days,
from {{ ref('fct_stations_monthly_dbt' )}}
where precipitation_days > 31