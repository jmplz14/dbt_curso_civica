WITH stg_promos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_promos') }}
    ),
dim_promos_new AS (
    SELECT
        promo_id,
        promo_id_nk,
        discount,
        status
    FROM stg_promos
    union
    SELECT
        md5('no_promo') as promo_id,
        'no_promo' as promo_id_nk,
        0 as discount,
        True as state 
    
    )

SELECT * FROM dim_promos_new