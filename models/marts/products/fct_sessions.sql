{% set array_event_types = get_distinct_values_column(ref('stg_sql_server_dbo_events_type'),'event_type') %}


WITH stg_sessions AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_sessions') }}
    ),
   
stg_users AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_users') }}
    ),
stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_events') }}
    ),
stg_events_type AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_events_type') }}
    ),


fct_sessions_new AS (
    SELECT
        sessions.session_id,
        sessions.user_id,
        users.first_name,
        users.last_name,
        users.email,
        min(events.created_at_utc) as first_event_time_utc,
        max(events.created_at_utc) as last_event_time_utc,
        {% for event_type in array_event_types %}
           
            sum(case when events_type.event_type = '{{event_type}}' then 1 else 0 end) as {{event_type}}_amount
            
        {%- if not loop.last %},{% endif -%}
        
       

        {% endfor %}
    FROM stg_sessions as sessions
    JOIN stg_users as users on users.user_id = sessions.user_id
    JOIN stg_events as events on events.session_id = sessions.session_id
    JOIN stg_events_type as events_type on events.event_type_id = events_type.event_type_id

    group by 1,2,3,4,5
    )    

SELECT * FROM fct_sessions_new