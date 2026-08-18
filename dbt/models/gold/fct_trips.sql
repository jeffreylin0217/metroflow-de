{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ replace_partitions() }}") }}
select trip_id, source_period, source_sha256, source_row,
    pickup_datetime, dropoff_datetime, pickup_date,
    pickup_zone_key, dropoff_zone_key,
    case when isfinite(passenger_count) and passenger_count >= 0 and passenger_count=floor(passenger_count)
         then passenger_count end as passenger_count,
    trip_distance, trip_duration_minutes,
    cast(fare_amount as decimal(18,2)) as fare_amount,
    cast(tip_amount as decimal(18,2)) as tip_amount,
    cast(total_amount as decimal(18,2)) as total_amount,
    payment_type, quality_status
from {{ ref('stg_taxi_trips') }}
where quality_status <> 'INVALID'
{% if is_incremental() %}
    and source_period in (
    {% for period in var('changed_periods', []) %}'{{ period }}'{% if not loop.last %},{% endif %}{% else %}null{% endfor %}
    )
{% endif %}
