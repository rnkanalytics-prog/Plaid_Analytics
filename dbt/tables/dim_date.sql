with source as (
    select * from {{ source('plaid_raw', 'dim_date') }}
),

final as (
    select
        date_id,
        date,
        year,
        quarter,
        month,
        day_of_month,
        day_of_week
    from source
)

select * from final
