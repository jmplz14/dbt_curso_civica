WITH snapshot_products AS (
        SELECT * 
        FROM {{ ref('product_snapshots') }}
    ),

new_products_price AS (
        SELECT
            md5(product_id) as product_id,
            price as price_usd,
            dbt_valid_from,
            dbt_valid_to,
            dbt_scd_id,
            dbt_updated_at,
            _fivetran_deleted,
            _fivetran_synced         
        FROM snapshot_products
        where dbt_valid_to is not null
    )

SELECT * FROM new_products_price