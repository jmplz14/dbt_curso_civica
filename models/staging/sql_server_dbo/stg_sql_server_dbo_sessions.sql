WITH src_sessions AS (
        SELECT * 
        FROM {{ source('sql_server_dbo', 'events') }}
    ),

new_sessions AS (
        SELECT 
            session_id as session_id_natural,
            md5(session_id) as session_id,
            md5(user_id) as user_id,
            min(created_at) as created_at


            
        FROM src_sessions
        group by 1,2,3

    )

SELECT * FROM new_sessions