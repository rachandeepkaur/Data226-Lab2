-- Singular test: flag implausible day-over-day swings (data quality guardrail).
select *
from {{ ref('mart_weather_daily_analytics') }}
where temp_max_pct_change_vs_prior_day is not null
  and abs(temp_max_pct_change_vs_prior_day) > 150
