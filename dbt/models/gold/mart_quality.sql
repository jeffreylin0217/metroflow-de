select source_period, quality_status, count(*) as trip_count,
    count(*) filter (where missing_timestamp) as missing_timestamp,
    count(*) filter (where reversed_timestamp) as reversed_timestamp,
    count(*) filter (where outside_source_month) as outside_source_month,
    count(*) filter (where invalid_distance) as invalid_distance,
    count(*) filter (where invalid_money) as invalid_money,
    count(*) filter (where unknown_zone) as unknown_zone,
    count(*) filter (where long_duration) as long_duration,
    count(*) filter (where zero_distance) as zero_distance,
    count(*) filter (where negative_money) as negative_money,
    count(*) filter (where unusual_passengers) as unusual_passengers,
    count(*) filter (where unusual_payment) as unusual_payment
from {{ ref('stg_taxi_trips') }} group by 1,2
