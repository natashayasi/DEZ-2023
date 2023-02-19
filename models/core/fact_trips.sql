{{ config(materialized='table') }}

select
    dispatching_base_number
    ,pickup_datetime
    ,dropoff_datetime
    ,pickup_locationid
    ,dropoff_locationid
    ,sr_flag
    ,affiliated_base_number
    ,trip_id
from {{ ref("stg_fhv_tripdata") }}