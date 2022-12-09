WITH src_users AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'users') }}
    ),

new_users AS (
        SELECT
            md5(user_id) as user_id,
            user_id as user_id_nk,
            email,
            updated_at as updated_at_utc,
            md5(address_id) as address_id,
            created_at as created_at_utc,
            cast (REPLACE (phone_number, '-', '') as integer) as phone_number,
            first_name,
            last_name,
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_users
    )

SELECT * FROM new_users