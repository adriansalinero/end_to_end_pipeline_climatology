{{ config(materialized='view') }}



{% for year in range( var('starting_year'), var('ending_year')+1 )     %}
    select 
        * ,
        {{ create_column_from_meteo_element(element_value="'TAVG'") }} as temp_avg,
        {{ create_column_from_meteo_element(element_value="'TMIN'") }} as temp_min,
        {{ create_column_from_meteo_element(element_value="'TMAX'") }} as temp_max,
        {{ create_column_from_meteo_element(element_value="'PRCP'") }} as precipitation,
    from `climatology`.`{{year}}`
    {{ 'union all' if not loop.last }}
{% endfor %}


{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}