select * from {{ source('bronze', 'zones') }}
