with tickets as (

    SELECT *
    FROM {{ ref('stg_tickets') }}
)

select *
from tickets