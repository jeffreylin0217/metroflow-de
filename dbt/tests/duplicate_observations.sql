{{ config(severity='warn') }}
-- A matching observed signature is evidence to investigate, not proof of a duplicate ride.
select source_period,pickup_datetime,dropoff_datetime,pickup_zone_key,dropoff_zone_key,
       trip_distance,total_amount,count(*) as occurrences
from {{ ref('fct_trips') }} group by 1,2,3,4,5,6,7 having count(*)>1
