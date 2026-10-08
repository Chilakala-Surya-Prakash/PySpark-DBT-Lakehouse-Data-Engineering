with source as (
    select * from read_parquet('../data/silver/locations/*.parquet')
),

renamed as (
    select
        cast(location_id as integer) as location_id,
        trim(city) as city,
        trim(state) as state,
        trim(country) as country,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(last_updated_timestamp as timestamp) as last_updated_at
    from source
)

select * from renamed
