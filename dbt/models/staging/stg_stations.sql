{{ config(materialized='view') }}

select
    *,
    CONCAT(latitude,', ', longitude) as latitude_longitude,
    substr(id,1,2) as country_code
from {{ source('staging','stations') }}