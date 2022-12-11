{% set array_event_types = get_distinct_values_column(ref('stg_sql_server_dbo_events_type'),'event_type') %}


WITH fct_sessions AS (
    SELECT * 
    FROM {{ ref('fct_sessions') }}
    ),

fct_embudo_new AS (
    SELECT
    count(*) as total_sessions,
       {% for event_type in array_event_types %}
           
            sum({{event_type}}_amount) as total_{{event_type}},
            sum(case when {{event_type}}_amount > 0 then 1 else 0 end) as sessions_with_{{event_type}}
        {%- if not loop.last %},{% endif -%}
        {% endfor %}
    from fct_sessions

)

SELECT * FROM fct_embudo_new
