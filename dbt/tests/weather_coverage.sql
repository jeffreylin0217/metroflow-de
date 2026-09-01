select distinct f.pickup_date from {{ ref('fct_trips') }} f
left join {{ ref('stg_weather') }} w on f.pickup_date=w.date
where w.date is null
