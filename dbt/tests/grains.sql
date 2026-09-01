select 'zone_day' as model from {{ ref('mart_zone_daily') }} group by date,zone_key having count(*)>1
union all
select 'borough_day' from {{ ref('mart_borough_daily') }} group by date,borough having count(*)>1
union all
select 'quality' from {{ ref('mart_quality') }} group by source_period,quality_status having count(*)>1
