WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_users') }}
    ),


dim_user_new AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        created_at_utc,
        updated_at_utc,
        address_id
    FROM stg_users
    )

SELECT * FROM dim_user_new