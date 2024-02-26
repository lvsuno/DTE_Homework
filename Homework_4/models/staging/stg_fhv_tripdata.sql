{{
    config(
        materialized='view'
    )
}}


with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}
    where EXTRACT(year  FROM  pickup_datetime ) = 2019
),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        drop_off_datetime,
        pulocation_id,
        dolocation_id,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}


