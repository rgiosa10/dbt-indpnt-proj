{{ config(materialized='table') }}

with origin_airports as (

    SELECT
        origin.iata as airport_iata,
        origin.name as airport_name, 
        origin.city as airport_city, 
        origin.country as airport_country,
        origin.icao as airport_icao,
        origin.latitude as airport_latitude,
        origin.longitude as airport_longitude,
        origin.altitude as airport_altitude,
        origin.tz_timezone as airport_tz_timezone,
        CURRENT_TIMESTAMP as created_at,
        NULL as modified_at
    FROM `idyllic-vehicle-374218.air_travel.air_travel`
    GROUP BY 1,2,3,4,5,6,7,8,9
),

dest_airports as (

    SELECT
        destination.iata as airport_iata,
        destination.name as airport_name, 
        destination.city as airport_city, 
        destination.country as airport_country,
        destination.icao as airport_icao,
        destination.latitude as airport_latitude,
        destination.longitude as airport_longitude,
        destination.altitude as airport_altitude,
        destination.tz_timezone as airport_tz_timezone,
        CURRENT_TIMESTAMP as created_at,
        NULL as modified_at
    FROM `idyllic-vehicle-374218.air_travel.air_travel`
    GROUP BY 1,2,3,4,5,6,7,8,9
),

final as (
    SELECT * FROM origin_airports
    UNION ALL
    SELECT * FROM dest_airports
)

SELECT *
FROM final
where airport_iata is not null
GROUP BY airport_iata,airport_name, airport_city, airport_country, airport_icao, airport_latitude, airport_longitude, airport_altitude, airport_tz_timezone, created_at, modified_at


