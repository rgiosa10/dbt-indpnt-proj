with passengers as (

    SELECT *
    FROM {{ ref('stg_passengers') }}
)

select *
from passengers