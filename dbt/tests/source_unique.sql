select 'zone' as source from {{ source('bronze','zones') }} group by zone_key having count(*)<>1
union all
select 'weather' from {{ source('bronze','weather') }} group by date having count(*)<>1
