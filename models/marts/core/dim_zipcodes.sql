WITH stg_zipcodes AS (
        SELECT * 
        FROM {{ ref('stg_sql_server_dbo_zipcode') }}
    ),

     

    dim_zipcode_new AS (
        SELECT
            zipcodes.zipcode,
            zipcodes.zipcode_id,
            zipcodes.state_id

        FROM stg_zipcodes as zipcodes 

    )

SELECT * FROM dim_zipcode_new