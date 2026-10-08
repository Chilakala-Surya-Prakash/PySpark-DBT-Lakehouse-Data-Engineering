with stg_cust as (
    select * from {{ ref('stg_customers') }}
),

trips_agg as (
    select
        customer_id,
        count(trip_id) as lifetime_trips,
        sum(fare_amount) as lifetime_spend,
        min(started_at) as first_trip_at,
        max(started_at) as latest_trip_at
    from {{ ref('stg_trips') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name as full_name,
    c.email,
    c.phone_number,
    c.city,
    c.signup_date,
    coalesce(t.lifetime_trips, 0) as lifetime_trips,
    coalesce(t.lifetime_spend, 0.0) as lifetime_spend,
    t.first_trip_at,
    t.latest_trip_at
from stg_cust c
left join trips_agg t on c.customer_id = t.customer_id
