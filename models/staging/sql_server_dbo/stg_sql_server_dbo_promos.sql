WITH src_promos AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'promos') }}
    ),

new_promos AS (
        SELECT
            md5(promo_id) as promo_id,
            promo_id as promo_id_natural,
            discount,
            case 
                when status = 'inactive' then False
                else True
            end as status

        FROM src_promos
    )
    
SELECT * FROM new_promos