WITH billing_web_zipcode AS (
        SELECT * 
        FROM {{ ref('billing_web_zipcode') }}
    ),
    dim_zipcodes AS (
        SELECT * 
        FROM {{ ref('dim_zipcodes') }}
    ),
    
    billing_web_new_state as (
        select 
            zipcodes.state_id,
            sum(total_bulling_web_usd) as total_bulling_web_usd,
            sum(total_bulling_no_web_usd) as total_bulling_no_web_usd
        from billing_web_zipcode as billing
        join dim_zipcodes as zipcodes on zipcodes.zipcode_id = billing.zipcode_id
        group by 1
    )


SELECT * FROM billing_web_new_state