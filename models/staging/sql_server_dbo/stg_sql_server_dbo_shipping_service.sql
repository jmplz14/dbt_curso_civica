WITH src_orders AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'orders') }}
    ),

new_orders_shipping_service AS (
        SELECT
            DISTINCT shipping_service,
            md5(shipping_service) as shipping_service_id
            
        FROM src_orders
        where shipping_service != ''
    )

SELECT * FROM new_orders_shipping_service