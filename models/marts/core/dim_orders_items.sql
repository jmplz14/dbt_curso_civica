{{
  config(
    materialized='table'
  )
}}

WITH stg_orders_items AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
),


new_dim_order_items AS (
    SELECT
        order_id,
        product_id,
        quantity
    FROM stg_orders_items
    )

SELECT * FROM new_dim_order_items