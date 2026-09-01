select trip_id from {{ ref('fct_trips') }}
where dropoff_datetime<pickup_datetime or trip_distance<0
 or strftime(pickup_datetime,'%Y-%m')<>source_period
