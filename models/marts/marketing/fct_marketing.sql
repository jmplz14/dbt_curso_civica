WITH dim_users AS (
    SELECT * 
    FROM {{ ref('dim_users') }}
    ),
    dim_addresses AS (
    SELECT * 
    FROM {{ ref('dim_addresses') }}
    ),
    fct_orders AS (
    SELECT * 
    FROM {{ ref('fct_orders') }}
    ),
    dim_orders_items AS (
    SELECT * 
    FROM {{ ref('dim_orders_items') }}
    ),
    dim_promos AS (
    SELECT * 
    FROM {{ ref('dim_promos') }}
    ),

    count_orders AS (
    SELECT

        user_id,
        count(*) AS total_number_orders,
        sum(order_cost_usd) AS total_order_cost_usd,
        sum(shipping_cost_usd) AS total_shipping_cost_usd,
        sum((order_cost_usd + shipping_cost_usd) - order_total_usd ) AS total_disconunt_usd

    FROM fct_orders AS orders
    group by 1
    ), 

    count_products AS (
        SELECT
            user_id,
            count(product_id) as total_product,
            sum(quantity) as total_quantity_product
        FROM dim_orders_items AS orders_items
        join fct_orders AS orders on orders.order_id = orders_items.order_id
        group by 1
    ),
    fct_marketing as (
        select
        orders.user_id,
        users.first_name,
        users.last_name,
        users.email,
        users.phone_number,
        users.created_at_utc,
        users.updated_at_utc,
        addresses.address,
        addresses.zipcode,
        addresses.state,
        addresses.country,
        orders.total_number_orders,
        orders.total_order_cost_usd,
        orders.total_shipping_cost_usd,
        orders.total_disconunt_usd,
        product.total_product,
        product.total_quantity_product
        from count_orders as orders
        join count_products as product on orders.user_id = product.user_id
        join dim_users as users on orders.user_id = users.user_id
        join dim_addresses as addresses on addresses.address_id = users.address_id
    )


SELECT * FROM fct_marketing
