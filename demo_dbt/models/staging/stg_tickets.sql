with tickets as (

    SELECT
        eticket_num,
        confirmation,
        ticket_date,
        price,
        seat,
        status,
        origin.iata as origin_iata, 
        destination.iata as dest_iata, 
        airline.iata as airline_iata,
        passenger.email as email_id,
    FROM {{ source('air_travel', 'air_travel') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

consolidated_tickets as (
    SELECT *
    FROM tickets
    INNER JOIN (
        SELECT email_id, passenger_sk
        FROM {{ ref('stg_passengers') }} 
        ) as p
    ON tickets.email_id = p.email_id
),

final as (
    SELECT
        eticket_num,
        confirmation,
        ticket_date,
        price,
        seat,
        status,
        origin_iata, 
        dest_iata, 
        airline_iata,
        passenger_sk,
        CURRENT_TIMESTAMP as created_at,
        NULL as modified_at
    from consolidated_tickets
)

SELECT *
FROM final

        