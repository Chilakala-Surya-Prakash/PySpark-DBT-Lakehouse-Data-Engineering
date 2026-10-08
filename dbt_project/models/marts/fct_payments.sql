with payments as (
    select * from {{ ref('stg_payments') }}
),

trips as (
    select trip_id, driver_id, customer_id, fare_amount, trip_status from {{ ref('stg_trips') }}
)

select
    p.payment_id,
    p.trip_id,
    t.driver_id,
    t.customer_id,
    p.payment_method,
    p.amount as payment_amount,
    t.fare_amount as trip_fare,
    p.payment_status,
    p.paid_at
from payments p
left join trips t on p.trip_id = t.trip_id
