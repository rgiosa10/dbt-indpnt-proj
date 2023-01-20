-- use with statement nesting for transformations

with passengers as (

    SELECT
        passenger.email as email_id, --rename column to align with data model
        passenger.first_name as first_name, --rename column to align with data model
        passenger.last_name as last_name, --rename column to align with data model
        passenger.birth_date as birth_date, --rename column to align with data model
        passenger.street as passenger_street, --rename column to align with data model
        passenger.city as passenger_city, --rename column to align with data model
        passenger.state as passenger_state, --rename column to align with data model
        passenger.zip as passenger_zip, --rename column to align with data model
        DATETIME(2001, 01, 01, 00, 00, 00) as effective_start_date, -- set the effective start date
        NULL as effective_end_date, -- set effective_end_date as Null
        CURRENT_TIMESTAMP as created_at, -- created_at column generated and filled with timestamp
        NULL as modified_at -- modified_at column generated with Null
    FROM {{ source('air_travel', 'air_travel') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

final as (
  SELECT 
      GENERATE_UUID() as passenger_sk, -- create surrogate key with UUID function 
      email_id,
      first_name, 
      last_name,
      birth_date,
      passenger_street,
      passenger_city,
      passenger_state,
      passenger_zip,
      effective_start_date,
      effective_end_date,
      created_at,
      modified_at
  FROM passengers
)

SELECT * -- query final in order to allow dim_passengers to reference
FROM final