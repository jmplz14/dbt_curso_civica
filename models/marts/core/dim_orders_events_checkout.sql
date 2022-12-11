{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS (
      SELECT * 
      FROM {{ ref('stg_sql_server_dbo_events') }}
    ),
    stg_events_type AS (
      SELECT * 
      FROM {{ ref('stg_sql_server_dbo_events_type') }}
    ),
     
    dim_order_events_checkout AS (
        SELECT
            order_id,
            event_id
        FROM stg_events 
        WHERE 
          order_id is not null  
          AND event_type_id = (SELECT event_type_id from stg_events_type WHERE event_type = 'checkout') 
    )

SELECT * FROM dim_order_events_checkout