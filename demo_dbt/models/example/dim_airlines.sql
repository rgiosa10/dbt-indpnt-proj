{{ config(materialized='table') }}

with airlines as (

    SELECT
        airline.iata as airline_iata, 
        airline.name as airline_name, 
        airline.icao as airline_icao,
        airline.callsign as airline_callsign,
        airline.country as airline_country,
        CURRENT_TIMESTAMP as created_at,
        NULL as modified_at
    FROM {{ source('air_travel', 'air_travel') }}
    GROUP BY 1,2,3,4,5
)

select *
from airlines
