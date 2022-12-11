WITH fct_events AS (
    SELECT * 
    FROM {{ ref('fct_events') }}
    ),
    dim_time AS (
    SELECT * 
    FROM {{ ref('dim_time') }}
    ),


    fct_events_transform AS (
        SELECT
            product_id,
            cast(events.created_at_utc as date) as created_at_utc
        from fct_events as events
        where product_id is not null

    ),

    product_visit AS (
        SELECT
            events.product_id,
            dim_time.mes,
            dim_time.anio,
            count(*) as total_monthly_visits
        from fct_events_transform as events
        join dim_time on dim_time.fecha_forecast = events.created_at_utc
        group by 1,2,3
        order by 1,2,3

    )

    

SELECT * FROM product_visit
