with source as (
    select * from {{ source('plaid_raw', 'dim_category') }}
),

final as (
    select
        category_id,
        initcap(replace(category_primary,    '_', ' ')) as category_primary,
        initcap(replace(category_detail,     '_', ' ')) as category_detail,
        initcap(replace(category_confidence, '_', ' ')) as category_confidence
    from source
)

select * from final
