select f.date, z.borough, sum(f.trip_count) as trip_count,
    sum(f.fare_amount) as fare_amount, sum(f.total_amount) as total_amount,
    count(distinct f.zone_key) as active_pickup_zones
from {{ ref('mart_zone_daily') }} f
join {{ ref('dim_zone') }} z on f.zone_key=z.zone_key
group by 1,2
