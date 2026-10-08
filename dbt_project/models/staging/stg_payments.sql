with source as (
    select * from read_parquet('../data/silver/payments/*.parquet')
),

renamed as (
    select
        cast(payment_id as integer) as payment_id,
        cast(trip_id as integer) as trip_id,
        trim(payment_method) as payment_method,
        round(cast(amount as double), 2) as amount,
        trim(payment_status) as payment_status,
        cast(payment_timestamp as timestamp) as paid_at,
        cast(last_updated_timestamp as timestamp) as last_updated_at
    from source
)

select * from renamed
