WITH stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_addresses') }}
    ),
    stg_zipcodes AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_zipcode') }}
    ),
    stg_states AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_states') }}
    ),
    stg_countries AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_countries') }}
    ),
     

dim_addresses_new AS (
    SELECT
        addresses.address_id,
        addresses.address_id_nk,
        addresses.address,
        zipcodes.zipcode,
        zipcodes.zipcode_id,
        states.state,
        states.state_id,
        countries.country,
        countries.country_id

    FROM stg_addresses as addresses
    JOIN stg_zipcodes as zipcodes on zipcodes.zipcode_id = addresses.zipcode_id
    JOIN stg_states as states on zipcodes.state_id = states.state_id
    JOIN stg_countries as countries on states.country_id = countries.country_id
    )

SELECT * FROM dim_addresses_new
