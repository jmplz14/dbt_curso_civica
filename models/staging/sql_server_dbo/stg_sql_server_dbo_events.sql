WITH src_events AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'events') }}
    ),

new_events AS (
        SELECT
            event_id as event_id_nk,
            md5(event_id) as event_id,
            md5(event_type) as event_type_id,
            md5(session_id) as session_id,
            case 
                when product_id = '' then null
                else md5(product_id)
            end as product_id,
            case 
                when order_id = '' then null
                else md5(order_id)
            end as order_id,
            page_url,
            created_at as created_at_utc

            
        FROM src_events
    )

SELECT * FROM new_events