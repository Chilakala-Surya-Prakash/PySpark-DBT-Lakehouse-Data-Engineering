with trips as (
    select * from {{ ref('stg_trips') }}
),

drivers as (
    select driver_id, first_name || ' ' || last_name as driver_name, driver_rating from {{ ref('stg_drivers') }}
),

customers as (
    select customer_id, first_name || ' ' || last_name as customer_name, email from {{ ref('stg_customers') }}
),

vehicles as (
    select vehicle_id, make || ' ' || model as vehicle_name, vehicle_type from {{ ref('stg_vehicles') }}
)

select
    t.trip_id,
    t.driver_id,
    d.driver_name,
    d.driver_rating,
    t.customer_id,
    c.customer_name,
    t.vehicle_id,
    v.vehicle_name,
    v.vehicle_type,
    t.started_at,
    t.ended_at,
    t.start_location,
    t.end_location,
    t.distance_km,
    t.fare_amount,
    t.payment_method,
    t.trip_status,
    t.duration_minutes,
    t.fare_per_km
from trips t
left join drivers d on t.driver_id = d.driver_id
left join customers c on t.customer_id = c.customer_id
left join vehicles v on t.vehicle_id = v.vehicle_id
