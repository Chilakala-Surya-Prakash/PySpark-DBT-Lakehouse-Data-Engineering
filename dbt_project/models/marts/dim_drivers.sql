with drivers as (
    select * from {{ ref('stg_drivers') }}
),

vehicles as (
    select * from {{ ref('stg_vehicles') }}
),

driver_stats as (
    select
        driver_id,
        count(trip_id) as total_trips,
        sum(case when trip_status = 'Completed' then 1 else 0 end) as completed_trips,
        sum(case when trip_status = 'Cancelled' then 1 else 0 end) as cancelled_trips,
        sum(case when trip_status = 'Completed' then fare_amount else 0 end) as total_revenue
    from {{ ref('stg_trips') }}
    group by driver_id
)

select
    d.driver_id,
    d.first_name || ' ' || d.last_name as driver_name,
    d.phone_number,
    d.city as operating_city,
    d.driver_rating,
    v.vehicle_id,
    v.make || ' ' || v.model as vehicle_model,
    v.vehicle_type,
    v.license_plate,
    coalesce(s.total_trips, 0) as total_trips,
    coalesce(s.completed_trips, 0) as completed_trips,
    coalesce(s.cancelled_trips, 0) as cancelled_trips,
    round(coalesce(s.total_revenue, 0.0), 2) as total_revenue
from drivers d
left join vehicles v on d.vehicle_id = v.vehicle_id
left join driver_stats s on d.driver_id = s.driver_id
