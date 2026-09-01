with demand as (
    select date, sum(trip_count) as trip_count, sum(total_amount) as total_amount
    from {{ ref('mart_zone_daily') }} group by 1
), daily as (
    select d.date_key as date, coalesce(f.trip_count,0) as trip_count,
        coalesce(f.total_amount,0) as total_amount,
        w.precipitation_mm, w.snowfall_mm, w.min_temperature_c, w.max_temperature_c,
        w.quality_flagged_values
    from {{ ref('dim_date') }} d
    join {{ ref('stg_weather') }} w on d.date_key=w.date
    left join demand f on d.date_key=f.date
)
select *, trip_count - lag(trip_count) over (order by date) as change_from_previous_day,
    avg(trip_count) over (order by date rows between 6 preceding and current row) as trailing_7_day_trips
from daily
