WITH src_addresses AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

new_addresses AS (
        SELECT
            md5(address_id) as address_id,
            address_id as address_id_nk,
            address,
            md5(zipcode) as zipcode_id,
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_addresses
    )

SELECT * FROM new_addresses