-- use with statement nesting for transformations

with tickets as (

    SELECT
        eticket_num,
        confirmation,
        ticket_date,
        price,
        seat,
        status,
        origin.iata as origin_iata, --rename column to align with data model
        destination.iata as dest_iata, --rename column to align with data model
        airline.iata as airline_iata, --rename column to align with data model
        passenger.email as email_id, --rename column to align with data model
    FROM {{ source('air_travel', 'air_travel') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10 -- use to remove duplicates
),

consolidated_tickets as (
    SELECT *
    FROM tickets
    INNER JOIN (
        SELECT email_id, passenger_sk -- Select email_id and passenger_sk to get surrogate key
        FROM {{ ref('stg_passengers') }} 
        ) as p
    ON tickets.email_id = p.email_id -- join the data on email_id
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
        CURRENT_TIMESTAMP as created_at, -- created_at column generated and filled with timestamp
        NULL as modified_at -- modified_at column generated with Null
    from consolidated_tickets
)

SELECT * -- Query final in order to enable creation of fct_tickets table
FROM final

        