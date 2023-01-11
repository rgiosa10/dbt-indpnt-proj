with passengers as (

    SELECT
        passenger.email as email_id,
        passenger.first_name as first_name, 
        passenger.last_name as last_name,
        passenger.birth_date as birth_date,
        passenger.street as passenger_street,
        passenger.city as passenger_city,
        passenger.state as passenger_state,
        passenger.zip as passenger_zip,
        DATETIME(2001, 01, 01, 00, 00, 00) as effective_start_date,
        NULL as effective_end_date,
        CURRENT_TIMESTAMP as created_at,
        NULL as modified_at
    FROM `idyllic-vehicle-374218.air_travel.air_travel`
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

final as (
  SELECT 
      GENERATE_UUID() as passenger_sk,
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

SELECT * 
FROM final