CREATE OR REPLACE VIEW `plaid-031426.plaid_dbt.analytics_view` AS

SELECT
    -- transaction keys
    f.transaction_id,
    f.plaid_transaction_id,

    -- account
    f.account_id,
    f.account_owner,
    f.account_pk,

    -- date
    f.date,
    f.year,
    f.quarter,
    f.month,
    f.day_of_month,
    f.day_of_week,

    -- category
    f.category_primary,
    f.category_detail,
    f.category_confidence,

    -- location
    f.address,
    f.city,
    f.region,
    f.postal_code,
    f.country,
    f.lat,
    f.lon,
    f.store_number,

    -- transaction details
    f.transaction_name,
    f.amount_usd,
    f.transaction_direction,
    f.payment_channel,
    f.transaction_type,
    f.iso_currency_code,
    f.merchant_entity_id,
    f.pending,
    f.is_large_transaction,
    f.days_since_transaction,
    f.amount_bucket

FROM `plaid-031426.plaid_dbt.fact_transactions` f
