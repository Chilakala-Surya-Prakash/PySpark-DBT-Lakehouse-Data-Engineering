with cust as (
    select * from {{ ref('dim_customers') }}
)

select
    city,
    strftime(signup_date, '%Y-%m') as signup_cohort,
    count(customer_id) as customer_count,
    sum(lifetime_trips) as total_cohort_trips,
    round(sum(lifetime_spend), 2) as total_cohort_spend,
    round(avg(lifetime_spend), 2) as avg_spend_per_customer
from cust
group by city, strftime(signup_date, '%Y-%m')
order by signup_cohort desc, total_cohort_spend desc
