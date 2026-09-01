select pickup_date as date, pickup_zone_key as zone_key,
    count(*) as trip_count, sum(passenger_count) as passenger_count,
    count(passenger_count) as trips_with_passenger_count,
    avg(trip_distance) as average_trip_distance,
    avg(trip_duration_minutes) as average_trip_duration,
    sum(fare_amount) as fare_amount, sum(total_amount) as total_amount,
    count(*) filter (where quality_status='SUSPICIOUS') as suspicious_trip_count
from {{ ref('fct_trips') }} group by 1,2
