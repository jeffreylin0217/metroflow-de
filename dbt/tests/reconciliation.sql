-- Count preservation proves that the dimension/weather joins do not multiply demand.
with counts as (
 select (select count(*) from {{ ref('fct_trips') }}) as facts,
 (select coalesce(sum(trip_count),0) from {{ ref('mart_zone_daily') }}) as zone_trips,
 (select coalesce(sum(trip_count),0) from {{ ref('mart_borough_daily') }}) as borough_trips,
 (select coalesce(sum(trip_count),0) from {{ ref('mart_weather_demand') }}) as weather_trips,
 (select coalesce(sum(trip_count),0) from {{ ref('mart_quality') }} where quality_status<>'INVALID') as retained,
 (select count(*) from {{ source('bronze','trips') }}) as input_rows,
 (select coalesce(sum(trip_count),0) from {{ ref('mart_quality') }}) as classified
)
select * from counts where facts<>zone_trips or facts<>borough_trips or facts<>weather_trips
 or facts<>retained or input_rows<>classified
