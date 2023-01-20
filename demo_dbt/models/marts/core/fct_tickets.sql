{{
    config(
        materialized='incremental',
        unique_key='eticket_num',
        on_schema_change='fail'
    )
}}

with tickets as (

    SELECT *
    FROM {{ ref('stg_tickets') }}
),

using_clause as (

    SELECT
        price,
        seat,
        status
    FROM {{ ref('stg_ticket_updates') }}

    {% if is_incremental() %}

        WHERE price != (SELECT price FROM {{ tickets }}) or seat != (SELECT seat FROM {{ tickets }}) or status != (SELECT status FROM {{ tickets }})

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

        WHERE eticket_num IN (SELECT eticket_num FROM {{ tickets }})

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

SELECT * FROM updates
UNION ALL
SELECT * FROM inserts