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
)

select *
from tickets