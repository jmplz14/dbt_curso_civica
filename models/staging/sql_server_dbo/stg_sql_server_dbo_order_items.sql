WITH src_order_items AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

new_order_items AS (
        SELECT
            md5(order_id),
            md5(product_id),
            quantity,
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_order_items
    )

SELECT * FROM new_order_items