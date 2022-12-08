WITH src_addresses AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

new_addresses AS (
        SELECT
            md5(address_id) as address_id,
            address_id as address_id_natural,
            md5(zipcode),
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_addresses
    )

SELECT * FROM new_addresses