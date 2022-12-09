WITH src_orders AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'orders') }}
    ),

new_orders_status AS (
        SELECT
            DISTINCT status as order_status,
            md5(order_status) as order_status_id
            
        FROM src_orders
    )

SELECT * FROM new_orders_status