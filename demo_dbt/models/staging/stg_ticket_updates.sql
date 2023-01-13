with ticket_updates as (
    SELECT *
    FROM {{ source('ticket_updates', 'ticket_updates') }}
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

MERGE INTO {{ ref('fct_tickets') }} as trg
USING consolidated as src
ON trg.eticket_num = src.eticket_num
WHEN MATCHED THEN
    UPDATE SET 
        price = src.price,
        seat = src.seat,
        status = src.status,
        modified_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
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
        NULL as modified_at) VALUES (src.eticket_num,
        src.confirmation,
        src.ticket_date,
        src.price,
        src.seat,
        src.status,
        src.origin_iata, 
        src.dest_iata, 
        src.airline_iata,
        src.passenger_sk,
        CURRENT_TIMESTAMP as created_at,
        NULL as modified_at)
    
