-- Input uniqueness is validated before dbt; ranking is a defensive conformance step.
select * exclude (rn) from (
    select *, row_number() over (partition by date order by station) as rn
    from {{ source('bronze', 'weather') }}
) where rn = 1
