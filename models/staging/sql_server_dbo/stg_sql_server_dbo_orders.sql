WITH src_orders AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'orders') }}
    ),

new_orders AS (
        SELECT
            md5(order_id) as order_id,
            order_id as order_id_natural,
            case 
                when promo_id = '' then null
                else md5(promo_id)
            end as promo_id,
            md5(user_id) as user_id,
            md5(address_id) as address_id,
            md5(status) as order_status_id,
            case 
                when shipping_service = '' then null
                else md5(shipping_service)
            end as shipping_service_id,
            created_at,
            order_cost,
            shipping_cost,
            order_total,
            case 
                when tracking_id = '' then null
                else md5(tracking_id)
            end as tracking_id,
            estimated_delivery_at,
            delivered_at
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_orders
    )

SELECT * FROM new_orders