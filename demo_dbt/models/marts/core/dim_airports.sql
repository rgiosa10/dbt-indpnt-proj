with airports as (

    SELECT *
    FROM {{ ref('stg_airports') }}
)

select *
from airports