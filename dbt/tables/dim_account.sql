with source as (
    select * from {{ source('plaid_raw', 'dim_account') }}
),

final as (
    select
        account_pk,
        account_id,
        account_owner
    from source
)

select * from final
