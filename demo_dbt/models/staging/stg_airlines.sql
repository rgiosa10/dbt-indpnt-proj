-- use with statement for transformations

with airlines as (

    SELECT
        airline.iata as airline_iata, 
        airline.name as airline_name, 
        airline.icao as airline_icao,
        airline.callsign as airline_callsign, --rename column to align with data model
        airline.country as airline_country, --rename column to align with data model
        CURRENT_TIMESTAMP as created_at, -- created_at column generated and filled with timestamp
        NULL as modified_at -- modified_at column generated with Null
    FROM {{ source('air_travel', 'air_travel') }}
    GROUP BY 1,2,3,4,5
)

select * -- query final in order to allow dim_passengers to reference
from airlines
