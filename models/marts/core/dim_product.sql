{{
  config(
    materialized='table'
  )
}}

WITH stg_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_products') }}
),


new_dim_products AS (
    SELECT
      product_id,
      product_id_nk,
      price,
      name,
      inventory
    FROM stg_products 
    )

SELECT * FROM new_dim_products