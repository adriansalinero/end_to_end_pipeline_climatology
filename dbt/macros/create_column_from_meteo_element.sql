--Macro used to create new columns when the meteorological_element equals to certain values

{% macro create_column_from_meteo_element(element_value) -%}

    case
        when meteorological_element = {{ element_value }} THEN cast (data_value/10 as numeric)
        else null
    end

{%- endmacro %}



