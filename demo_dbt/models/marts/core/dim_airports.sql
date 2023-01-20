-- Query airports staging table using Jinja to create dim table

with airports as (

    SELECT *
    FROM {{ ref('stg_airports') }}
)

select *
from airports