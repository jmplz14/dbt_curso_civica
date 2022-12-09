WITH src_addresses AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

new_addresses_country AS (
        SELECT
            DISTINCT country,
            md5(country) as country_id
            
        FROM src_addresses
    )

SELECT * FROM new_addresses_country