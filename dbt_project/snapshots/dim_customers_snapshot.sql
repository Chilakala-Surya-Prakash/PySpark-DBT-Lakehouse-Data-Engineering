{% snapshot dim_customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='last_updated_at',
    )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    city,
    signup_date,
    last_updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
