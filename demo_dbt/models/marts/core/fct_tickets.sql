-- Query tickets staging table using Jinja to create fct table

with tickets as (

    SELECT *
    FROM {{ ref('stg_tickets') }}
)

select *
from tickets