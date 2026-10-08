with source as (
    select * from read_parquet('../data/silver/drivers/*.parquet')
),

renamed as (
    select
        cast(driver_id as integer) as driver_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        phone_clean as phone_number,
        cast(vehicle_id as integer) as vehicle_id,
        round(cast(driver_rating as double), 2) as driver_rating,
        trim(city) as city,
        cast(last_updated_timestamp as timestamp) as last_updated_at
    from source
)

select * from renamed
