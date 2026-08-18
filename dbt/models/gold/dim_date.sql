select cast(day as date) as date_key, year(day) as year, month(day) as month,
    dayofweek(day) as weekday_number, dayname(day) as weekday_name,
    dayofweek(day) in (0,6) as is_weekend
from generate_series(
    (select min(date) from {{ ref('stg_weather') }}),
    (select max(date) from {{ ref('stg_weather') }}), interval 1 day
) dates(day)
