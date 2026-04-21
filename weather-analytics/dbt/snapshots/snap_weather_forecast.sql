{% snapshot snap_weather_forecast %}

{{
    config(
        unique_key=['LOCATION_NAME', 'FORECAST_DATE'],
        strategy='timestamp',
        updated_at='CREATED_AT',
    )
}}

select
    LOCATION_NAME::varchar as LOCATION_NAME,
    FORECAST_DATE::date as FORECAST_DATE,
    PREDICTED_TEMP_MAX::float as PREDICTED_TEMP_MAX,
    MODEL_NAME::varchar as MODEL_NAME,
    TRAIN_START_DATE::date as TRAIN_START_DATE,
    TRAIN_END_DATE::date as TRAIN_END_DATE,
    CREATED_AT::timestamp_ntz as CREATED_AT
from {{ source('weather', 'weather_forecast_daily') }}

{% endsnapshot %}
