with source as (
    select * from read_parquet('../data/silver/vehicles/*.parquet')
),

renamed as (
    select
        cast(vehicle_id as integer) as vehicle_id,
        upper(trim(license_plate)) as license_plate,
        trim(model) as model,
        trim(make) as make,
        cast(year as integer) as vehicle_year,
        trim(vehicle_type) as vehicle_type,
        cast(last_updated_timestamp as timestamp) as last_updated_at
    from source
)

select * from renamed
