{{
  config(
    materialized='table'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
),


renamed_casted AS (
    SELECT
        order_id,
        order_id_nk,
        case 
            when promo_id is null then md5('no_promo')
            else promo_id
        end as promo_id,
        user_id,
        address_id,
        order_status_id,
        shipping_service_id,
        created_at_utc,
        order_cost_usd,
        shipping_cost_usd,
        order_total_usd,
        tracking_id,
        estimated_delivery_at_utc,
        delivered_at_utc
    FROM stg_orders
    )

SELECT * FROM renamed_casted