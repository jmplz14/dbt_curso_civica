WITH stg_sessions AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_sessions') }}
    ),

dim_sessions_new AS (
    SELECT
        session_id_nk,
        session_id,
        user_id,
        created_at_utc
    FROM stg_sessions
    )

SELECT * FROM dim_sessions_new