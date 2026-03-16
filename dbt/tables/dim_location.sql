with source as (
    select * from {{ source('plaid_raw', 'dim_location') }}
),

final as (
    select
        location_id,
        address,
        city,
        region,
        postal_code,
        country,
        lat,
        lon,
        store_number
    from source
)

select * from final
