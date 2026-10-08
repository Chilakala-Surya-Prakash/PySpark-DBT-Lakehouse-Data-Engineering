with fct as (
    select * from {{ ref('fct_trips') }}
)

select
    driver_id,
    driver_name,
    strftime(started_at, '%Y-%m') as year_month,
    count(trip_id) as total_trips,
    sum(case when trip_status = 'Completed' then 1 else 0 end) as completed_trips,
    sum(case when trip_status = 'Cancelled' then 1 else 0 end) as cancelled_trips,
    round(sum(case when trip_status = 'Completed' then fare_amount else 0 end), 2) as total_monthly_earnings,
    round(avg(distance_km), 2) as avg_trip_distance,
    round(avg(duration_minutes), 2) as avg_trip_duration
from fct
group by 1, 2, 3
order by year_month desc, total_monthly_earnings desc
