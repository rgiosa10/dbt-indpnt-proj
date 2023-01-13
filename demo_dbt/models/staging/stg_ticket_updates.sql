{{
    config(
        materialized='incremental',
        unique_key='eticket_num',
        on_schema_change='fail'
    )
}}

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
),

using_clause as (

    SELECT
        price,
        seat,
        status
    FROM consolidated

    {% if is_incremental() %}

        WHERE price != (SELECT price FROM {{ this }}) or seat != (SELECT seat FROM {{ this }}) or status != (SELECT status FROM {{ this }})

    {% endif %}
),

updates as (
    SELECT
        price,
        seat,
        status,
        CURRENT_TIMESTAMP as modified_at
    FROM using_clause

    {% if is_incremental() %}

        WHERE eticket_num IN (SELECT eticket_num FROM {{ this }})

    {% endif %}

),

inserts AS (

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
        created_at,
        modified_at
    FROM using_clause
    WHERE eticket_num NOT IN (SELECT eticket_num FROM updates)

)

SELECT * FROM updates UNION ALL SELECT * FROM inserts
    
