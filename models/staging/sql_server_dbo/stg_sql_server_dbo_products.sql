WITH src_products AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'products') }}
    ),

new_products AS (
        SELECT
            md5(product_id) as product_id,
            product_id as product_id_nk,
            price as price_usd,
            name,
            inventory,
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_products
    )

SELECT * FROM new_products