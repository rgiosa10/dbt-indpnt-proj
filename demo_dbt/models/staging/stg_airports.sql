-- use with statement nesting for transformations

with origin_airports as (

    SELECT
        origin.iata as airport_iata,--rename column to align with data model
        origin.name as airport_name, --rename column to align with data model
        origin.city as airport_city, --rename column to align with data model
        origin.country as airport_country, --rename column to align with data model
        origin.icao as airport_icao, --rename column to align with data model
        origin.latitude as airport_latitude, --rename column to align with data model
        origin.longitude as airport_longitude, --rename column to align with data model
        origin.altitude as airport_altitude, --rename column to align with data model
        origin.tz_timezone as airport_tz_timezone, --rename column to align with data model
        CURRENT_TIMESTAMP as created_at, -- created_at column generated and filled with timestamp
        NULL as modified_at -- modified_at column generated with Null
    FROM {{ source('air_travel', 'air_travel') }}
    GROUP BY 1,2,3,4,5,6,7,8,9 -- remove any duplicates
),

dest_airports as (

    SELECT
        destination.iata as airport_iata, --rename column to align with data model
        destination.name as airport_name, --rename column to align with data model
        destination.city as airport_city, --rename column to align with data model
        destination.country as airport_country, --rename column to align with data model
        destination.icao as airport_icao, --rename column to align with data model
        destination.latitude as airport_latitude, --rename column to align with data model
        destination.longitude as airport_longitude, --rename column to align with data model
        destination.altitude as airport_altitude, --rename column to align with data model
        destination.tz_timezone as airport_tz_timezone, --rename column to align with data model
        CURRENT_TIMESTAMP as created_at, -- created_at column generated and filled with timestamp
        NULL as modified_at -- modified_at column generated with Null
    FROM {{ source('air_travel', 'air_travel') }}
    GROUP BY 1,2,3,4,5,6,7,8,9 -- remove any duplicates
),

final as (
    SELECT * FROM origin_airports 
    UNION ALL -- BigQuery does not use UNION so use UNION ALL
    SELECT * FROM dest_airports -- perform UNION to concat the two queries
)

SELECT * -- Query this combined query and remove any duplicates as UNION ALL does not remove duplicate values
FROM final
where airport_iata is not null
GROUP BY 
    airport_iata,
    airport_name,
    airport_city,
    airport_country,
    airport_icao,
    airport_latitude,
    airport_longitude,
    airport_altitude,
    airport_tz_timezone,
    created_at,
    modified_at


