WITH src_orders AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'orders') }}
    ),

new_orders AS (
        SELECT
            md5(order_id) as order_id,
            order_id as order_id_nk,
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
            created_at as created_at_utc,
            order_cost as order_cost_usd,
            shipping_cost as shipping_cost_usd,
            order_total as order_total_usd,
            case 
                when tracking_id = '' then null
                else md5(tracking_id)
            end as tracking_id,
            estimated_delivery_at as estimated_delivery_at_utc,
            delivered_at as delivered_at_utc,
            
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_orders
    )

SELECT * FROM new_orders