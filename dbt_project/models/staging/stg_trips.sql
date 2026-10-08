with source as (
    select * from read_parquet('../data/silver/trips/*.parquet')
),

renamed as (
    select
        cast(trip_id as integer) as trip_id,
        cast(driver_id as integer) as driver_id,
        cast(customer_id as integer) as customer_id,
        cast(vehicle_id as integer) as vehicle_id,
        cast(trip_start_time as timestamp) as started_at,
        cast(trip_end_time as timestamp) as ended_at,
        trim(start_location) as start_location,
        trim(end_location) as end_location,
        cast(distance_km as double) as distance_km,
        round(cast(fare_amount as double), 2) as fare_amount,
        trim(payment_method) as payment_method,
        trim(trip_status) as trip_status,
        round(cast(trip_duration_minutes as double), 2) as duration_minutes,
        round(cast(fare_per_km as double), 2) as fare_per_km,
        cast(last_updated_timestamp as timestamp) as last_updated_at
    from source
)

select * from renamed
