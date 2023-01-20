-- Query passengers staging table using Jinja to create dim table

with passengers as (

    SELECT *
    FROM {{ ref('stg_passengers') }}
)

select *
from passengers