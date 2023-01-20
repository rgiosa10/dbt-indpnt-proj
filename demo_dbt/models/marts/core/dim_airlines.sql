-- Query airlines staging table using Jinja to create dim table

with airlines as (

    SELECT *
    FROM {{ ref('stg_airlines') }}
)

select *
from airlines