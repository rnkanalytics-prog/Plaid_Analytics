with source as (
    select * from {{ source('plaid_raw', 'fact_transactions') }}
),

dates as (
    select * from {{ source('plaid_raw', 'dim_date') }}
),

categories as (
    select * from {{ source('plaid_raw', 'dim_category') }}
),

locations as (
    select * from {{ source('plaid_raw', 'dim_location') }}
),

staged as (
    select
        -- keys
        t.transaction_id,
        t.plaid_transaction_id,
        t.account_id,
        t.date_id,
        t.location_id,
        t.category_id,

        -- transaction fields cleaned
        initcap(regexp_replace(t.transaction_name, r'[*#/\\]', ''))  as transaction_name,
        abs(t.amount_usd)                                             as amount_usd,
        case
            when t.amount_usd > 0 then 'Debit'
            else 'Credit'
        end                                                           as transaction_direction,
        initcap(replace(t.payment_channel, '_', ' '))                as payment_channel,
        initcap(replace(t.transaction_type, '_', ' '))               as transaction_type,
        t.iso_currency_code,
        t.merchant_entity_id,

        -- category fields cleaned
        initcap(replace(c.category_primary,    '_', ' '))            as category_primary,
        initcap(replace(c.category_detail,     '_', ' '))            as category_detail,
        initcap(replace(c.category_confidence, '_', ' '))            as category_confidence,

        -- country fix
        case
            when l.country = 'US' then 'USA'
            else l.country
        end                                                           as country,

        -- pending flag as Yes/No
        case
            when date(d.date) = current_date() then 'Yes'
            else 'No'
        end                                                           as pending,

        -- days since transaction calculated fresh every run
        date_diff(current_date(), date(d.date), day)                 as days_since_transaction,

        -- amount buckets
        case
            when abs(t.amount_usd) < 10   then 'Under $10'
            when abs(t.amount_usd) < 50   then '$10 - $50'
            when abs(t.amount_usd) < 100  then '$50 - $100'
            when abs(t.amount_usd) < 500  then '$100 - $500'
            else 'Over $500'
        end                                                           as amount_bucket,

        -- large transaction flag as Yes/No
        case
            when abs(t.amount_usd) > 1000 then 'Yes'
            else 'No'
        end                                                           as is_large_transaction

    from source t
    left join dates     d on t.date_id     = d.date_id
    left join categories c on t.category_id = c.category_id
    left join locations  l on t.location_id = l.location_id
)

select * from staged
