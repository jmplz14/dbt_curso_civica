WITH src_addresses AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

new_addresses_zipcode AS (
        SELECT
            DISTINCT zipcode,
            md5(zipcode) as zipcode_id,
            md5(state) as state_id
            
            
        FROM src_addresses
    )

SELECT * FROM new_addresses_zipcode