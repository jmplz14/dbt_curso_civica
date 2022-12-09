{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_events') }}
    ),

new_fct_events AS (
    SELECT
        events.event_id,
        events.event_type_id,
        events.session_id,
        events.product_id,
        events.order_id,
        events.page_url,
        events.created_at_utc
    FROM stg_events as events
  )

SELECT * FROM new_fct_events
