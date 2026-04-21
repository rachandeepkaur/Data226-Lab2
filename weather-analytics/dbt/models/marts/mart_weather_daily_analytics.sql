{{ config(materialized='table') }}

with enriched as (
    select
        location_name,
        observation_date,
        temp_max,
        temp_min,
        temp_mean,
        is_forecast,
        model_name,
        avg(temp_max) over (
            partition by location_name, is_forecast
            order by observation_date
            rows between 6 preceding and current row
        ) as temp_max_7d_moving_avg,
        lag(temp_max) over (
            partition by location_name, is_forecast
            order by observation_date
        ) as prev_day_temp_max
    from {{ ref('stg_weather_final_daily') }}
)

select
    location_name,
    observation_date,
    temp_max,
    temp_min,
    temp_mean,
    is_forecast,
    model_name,
    temp_max_7d_moving_avg,
    case
        when prev_day_temp_max is not null and abs(prev_day_temp_max) > 1e-9 then
            (temp_max - prev_day_temp_max) / prev_day_temp_max * 100.0
        else null
    end as temp_max_pct_change_vs_prior_day
from enriched
