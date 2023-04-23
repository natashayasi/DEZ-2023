{{ config(materialized='table') }}

select
    dispatching_base_number
    ,pickup_datetime
    ,dropoff_datetime
    ,pickup_locationid
    ,dropoff_locationid
    ,sr_flag
    ,affiliated_base_number
from {{ ref("stg_fhv_tripdata") }} as fhv
inner join {{ ref('dim_zones') }} as pickup_zone
on fhv.pickup_locationid = pickup_zone.locationid
    and pickup_zone.borough != 'Unknown'
inner join {{ ref('dim_zones') }} as dropoff_zone
on fhv.dropoff_locationid = dropoff_zone.locationid
    and dropoff_zone.borough != 'Unknown'