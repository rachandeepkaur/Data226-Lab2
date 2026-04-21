{{ config(materialized='view') }}

select
    LOCATION_NAME::varchar as location_name,
    "DATE"::date as observation_date,
    TEMP_MAX::float as temp_max,
    TEMP_MIN::float as temp_min,
    TEMP_MEAN::float as temp_mean,
    IS_FORECAST::boolean as is_forecast,
    MODEL_NAME::varchar as model_name
from {{ source('weather', 'weather_final_daily') }}
