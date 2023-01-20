with ticket_updates as (
    SELECT *
    FROM {{ source('air_travel', 'ticket_updates') }}
),

consolidated as (
    SELECT *
    FROM ticket_updates
    INNER JOIN (
        SELECT 
            eticket_num,
            origin_iata, 
            dest_iata, 
            airline_iata,
            passenger_sk,
            created_at,
            modified_at
        FROM {{ ref('stg_tickets') }} 
        ) as stgt
    ON ticket_updates.eticket_num = stgt.eticket_num
)

SELECT *
FROM consolidated
    
