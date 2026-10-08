with vehicles as (
    select * from {{ ref('stg_vehicles') }}
),

trips_summary as (
    select
        vehicle_id,
        count(trip_id) as total_trips_assigned,
        sum(distance_km) as total_km_driven,
        sum(fare_amount) as total_fare_generated
    from {{ ref('stg_trips') }}
    group by vehicle_id
)

select
    v.vehicle_id,
    v.license_plate,
    v.make,
    v.model,
    v.vehicle_year,
    v.vehicle_type,
    coalesce(t.total_trips_assigned, 0) as total_trips_assigned,
    round(coalesce(t.total_km_driven, 0.0), 2) as total_km_driven,
    round(coalesce(t.total_fare_generated, 0.0), 2) as total_fare_generated
from vehicles v
left join trips_summary t on v.vehicle_id = t.vehicle_id
