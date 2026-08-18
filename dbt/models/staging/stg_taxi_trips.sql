with typed as (
    select source_period, source_sha256, source_row,
        source_sha256 || ':' || source_row::varchar as trip_id,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        cast(tpep_pickup_datetime as date) as pickup_date,
        PULocationID as pickup_zone_key, DOLocationID as dropoff_zone_key,
        passenger_count, trip_distance, fare_amount, tip_amount, total_amount, payment_type,
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0 as trip_duration_minutes
    from {{ source('bronze', 'trips') }}
), flags as (
    select *,
        pickup_datetime is null or dropoff_datetime is null as missing_timestamp,
        coalesce(dropoff_datetime < pickup_datetime, false) as reversed_timestamp,
        coalesce(strftime(pickup_datetime, '%Y-%m') <> source_period, false) as outside_source_month,
        trip_distance is null or not isfinite(trip_distance) or trip_distance < 0 as invalid_distance,
        fare_amount is null or total_amount is null or tip_amount is null
          or not isfinite(fare_amount) or not isfinite(total_amount) or not isfinite(tip_amount) as invalid_money,
        pickup_zone_key is null or dropoff_zone_key is null
          or pickup_zone_key not in (select zone_key from {{ ref('stg_zones') }})
          or dropoff_zone_key not in (select zone_key from {{ ref('stg_zones') }}) as unknown_zone,
        coalesce(trip_duration_minutes > 180, false) as long_duration,
        coalesce(trip_distance = 0, false) as zero_distance,
        coalesce(fare_amount < 0 or total_amount < 0 or tip_amount < 0, false) as negative_money,
        passenger_count is null or not isfinite(passenger_count) or passenger_count <= 0
          or passenger_count <> floor(passenger_count) as unusual_passengers,
        payment_type is null or payment_type not in (0,1,2,3,4,5,6) as unusual_payment
    from typed
)
select *, case
    when missing_timestamp or reversed_timestamp or outside_source_month or invalid_distance or invalid_money or unknown_zone
        then 'INVALID'
    when long_duration or zero_distance or negative_money or unusual_passengers or unusual_payment then 'SUSPICIOUS'
    else 'ACCEPTED' end as quality_status
from flags
