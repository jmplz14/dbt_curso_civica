WITH src_products AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'products') }}
    ),

new_products AS (
        SELECT
            md5(product_id),
            price,
            name,
            inventory,
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_products
    )

SELECT * FROM new_products