{% snapshot dim_drivers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='driver_id',
      strategy='timestamp',
      updated_at='last_updated_at',
    )
}}

select
    driver_id,
    first_name,
    last_name,
    phone_number,
    vehicle_id,
    driver_rating,
    city,
    last_updated_at
from {{ ref('stg_drivers') }}

{% endsnapshot %}
