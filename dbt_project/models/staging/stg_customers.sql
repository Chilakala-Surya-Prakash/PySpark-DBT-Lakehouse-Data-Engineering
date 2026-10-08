with source as (
    select * from read_parquet('../data/silver/customers/*.parquet')
),

renamed as (
    select
        cast(customer_id as integer) as customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        phone_clean as phone_number,
        trim(city) as city,
        cast(signup_date as date) as signup_date,
        cast(last_updated_timestamp as timestamp) as last_updated_at
    from source
)

select * from renamed
