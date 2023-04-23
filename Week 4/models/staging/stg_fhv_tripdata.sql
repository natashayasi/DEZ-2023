{{ config(materialized='view') }}

select
    CAST(dispatching_base_num as string) as dispatching_base_number
    ,CAST(pickup_datetime as timestamp) as pickup_datetime
    ,CAST(dropOff_datetime as timestamp) as dropoff_datetime
    ,CAST(PUlocationID as integer) as pickup_locationid
    ,CAST(DOlocationID as integer) as dropoff_locationid
    ,CAST(SR_Flag as string) as sr_flag
    ,CAST(Affiliated_base_number as string) as affiliated_base_number
from {{ source('staging', 'fhv_tripdata') }}
WHERE pickup_datetime BETWEEN '2019-01-01' AND '2020-12-31'
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

    limit 100

{% endif %}