with airlines as (

    SELECT *
    FROM {{ ref('stg_airlines') }}
)

select *
from airlines