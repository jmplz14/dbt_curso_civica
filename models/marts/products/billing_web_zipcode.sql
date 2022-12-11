WITH dim_orders_by_chekcout AS (
        SELECT * 
        FROM {{ ref('dim_orders_events_checkout') }}
    ),
    fct_orders AS (
        SELECT * 
        FROM {{ ref('fct_orders') }}
    ),
    dim_addresses AS (
        SELECT * 
        FROM {{ ref('dim_addresses') }}
    ),
    dim_users AS (
        SELECT * 
        FROM {{ ref('dim_users') }}
    ),
    
    billing_new AS (
        SELECT
        orders.user_id,
        sum(
            case when orders_by_chekcout.event_id is not null 
                then 
                    orders.order_total_usd 
                else 
                    0 
            end
        ) as total_bulling_web_usd,
        
        sum(
            case when orders_by_chekcout.event_id is null 
                then 
                    orders.order_total_usd 
                else 
                    0 
            end
        ) as total_bulling_no_web_usd
        
        from fct_orders as orders
        left join dim_orders_by_chekcout as orders_by_chekcout on orders.order_id = orders_by_chekcout.order_id
        group by 1

    ),
    
    billing_web_new_zipcode as (
        select 
            addresses.zipcode_id,
            sum(total_bulling_web_usd) as total_bulling_web_usd,
            sum(total_bulling_no_web_usd) as total_bulling_no_web_usd
        from billing_new as billing
        join dim_users as users on users.user_id = billing.user_id
        join dim_addresses as addresses on users.address_id = addresses.address_id
        group by 1
    )


SELECT * FROM billing_web_new_zipcode