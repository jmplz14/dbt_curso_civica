

WITH src_events_type AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'events') }}
    ),

new_events_type AS (
        SELECT
            DISTINCT event_type,
            md5(event_type) as event_type_id

            
            
        FROM src_events_type
    )

SELECT * FROM new_events_type

