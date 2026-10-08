with locations as (
    select * from {{ ref('stg_locations') }}
)

select
    location_id,
    city,
    state,
    country,
    latitude,
    longitude,
    last_updated_at
from locations
