{% set array_event_types = get_distinct_values_column(ref('stg_sql_server_dbo_events_type'),'event_type') %}


WITH sessions_by_zipcode AS (
        SELECT * 
        FROM {{ ref('sessions_by_zipcode') }}
    ),
    dim_zipcodes AS (
        SELECT * 
        FROM {{ ref('dim_zipcodes') }}
    ),

    sessions_by_status_new AS (
        SELECT
        zipcodes.state_id,
        sum(total_sessions) as total_sessions,
        {% for event_type in array_event_types %}
           
            sum(total_{{event_type}}) as total_{{event_type}},
            sum(sessions_with_{{event_type}}) as sessions_with_{{event_type}}
        {%- if not loop.last %},{% endif -%}
        {% endfor %}

        from sessions_by_zipcode as sessions
        join dim_zipcodes AS zipcodes on zipcodes.zipcode_id = sessions.zipcode_id
        group by 1

        

        

    )

SELECT * FROM sessions_by_status_new
