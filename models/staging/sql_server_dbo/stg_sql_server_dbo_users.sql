WITH src_users AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'users') }}
    ),

new_users AS (
        SELECT
            md5(user_id),
            email,
            updated_at,
            md5(address_id),
            created_at,
            cast (REPLACE (phone_number, '-', '') as integer) as phone_number,
            first_name,
            last_name,
            _fivetran_deleted,
            _fivetran_synced         
        FROM src_users
    )

SELECT * FROM new_users