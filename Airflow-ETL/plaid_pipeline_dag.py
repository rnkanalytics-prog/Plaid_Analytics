from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import random
import time
import uuid
import datetime as dt

from faker import Faker
from google.cloud import storage
from google.cloud import bigquery

import plaid
from plaid.api import plaid_api
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.products import Products
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
PLAID_CLIENT_ID = Variable.get('PLAID_CLIENT_ID')
PLAID_SECRET    = Variable.get('PLAID_SECRET')
GCS_PROJECT     = 'plaid-031426'
GCS_BUCKET      = 'plaid-raw-data'
BQ_DATASET      = 'plaid_raw'

fake = Faker()

# ── Account owners ────────────────────────────────────────────────────────────
ACCOUNT_OWNERS = [
    {"account_id": "oM4wKDvbaDUwBAZEz17eSkqElAjJMXToM1q5x", "owner": "John Doe"},
    {"account_id": "LLvqdaJMXaSqMpjnJ96eSyZ5LJa9ArCkQqjKd", "owner": "Sarah Johnson"},
    {"account_id": "p1NWrDvg4Did3BLV7DmjcE8W4ky9rpcprbojm", "owner": "Marcus Williams"},
    {"account_id": "MpvKa4JlX4UBNgGWEDXmFKVr7vyDJACLlN6pm", "owner": "Emily Chen"},
    {"account_id": "1bd9oVQJ8VU8XjWeGkN9SdvLNyA1m3upKNjXV", "owner": "David Rodriguez"},
]

ACCOUNT_ID_TO_OWNER = {a["account_id"]: a["owner"] for a in ACCOUNT_OWNERS}


# ── Task 0: Truncate BigQuery tables ─────────────────────────────────────────
def truncate_tables():
    bq_client = bigquery.Client(project=GCS_PROJECT)

    tables = [
        'fact_transactions',
        'dim_account',
        'dim_location',
        'dim_category',
        'dim_date'
    ]

    for table in tables:
        table_id = f'{GCS_PROJECT}.{BQ_DATASET}.{table}'
        try:
            bq_client.query(f'TRUNCATE TABLE `{table_id}`').result()
            print(f'Truncated {table}')
        except Exception as e:
            print(f'Could not truncate {table} (may not exist yet): {e}')

    print('All tables truncated!')


# ── Task 1: Extract ───────────────────────────────────────────────────────────
def extract():
    configuration = plaid.Configuration(
        host=plaid.Environment.Sandbox,
        api_key={
            'clientId': PLAID_CLIENT_ID,
            'secret':   PLAID_SECRET,
        }
    )
    api_client = plaid.ApiClient(configuration)
    client     = plaid_api.PlaidApi(api_client)

    public_token_request = SandboxPublicTokenCreateRequest(
        institution_id='ins_109508',
        initial_products=[Products('transactions')]
    )
    public_token_response = client.sandbox_public_token_create(public_token_request)
    public_token = public_token_response['public_token']

    exchange_request  = ItemPublicTokenExchangeRequest(public_token=public_token)
    exchange_response = client.item_public_token_exchange(exchange_request)
    access_token      = exchange_response['access_token']

    time.sleep(10)

    end_date   = date.today()
    start_date = end_date - timedelta(days=365)

    transactions_request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        options=TransactionsGetRequestOptions(count=500)
    )
    response          = client.transactions_get(transactions_request)
    transactions      = response['transactions']
    real_transactions = [t.to_dict() for t in transactions]

    # Distribute real transactions across all 5 account owners
    for txn in real_transactions:
        owner_info           = random.choice(ACCOUNT_OWNERS)
        txn['account_id']    = owner_info['account_id']
        txn['account_owner'] = owner_info['owner']

    merchants = [
        {"name": "Starbucks",       "category": "FOOD_AND_DRINK",      "detail": "FOOD_AND_DRINK_COFFEE",           "channel": "in store", "type": "place",   "min": 4,    "max": 12,   "website": "starbucks.com"},
        {"name": "McDonald's",      "category": "FOOD_AND_DRINK",      "detail": "FOOD_AND_DRINK_FAST_FOOD",        "channel": "in store", "type": "place",   "min": 5,    "max": 20,   "website": "mcdonalds.com"},
        {"name": "Chipotle",        "category": "FOOD_AND_DRINK",      "detail": "FOOD_AND_DRINK_FAST_FOOD",        "channel": "in store", "type": "place",   "min": 10,   "max": 25,   "website": "chipotle.com"},
        {"name": "Uber Eats",       "category": "FOOD_AND_DRINK",      "detail": "FOOD_AND_DRINK_RESTAURANT",       "channel": "online",   "type": "place",   "min": 20,   "max": 60,   "website": "ubereats.com"},
        {"name": "DoorDash",        "category": "FOOD_AND_DRINK",      "detail": "FOOD_AND_DRINK_RESTAURANT",       "channel": "online",   "type": "place",   "min": 20,   "max": 70,   "website": "doordash.com"},
        {"name": "Whole Foods",     "category": "FOOD_AND_DRINK",      "detail": "FOOD_AND_DRINK_GROCERIES",        "channel": "in store", "type": "place",   "min": 30,   "max": 200,  "website": "wholefoodsmarket.com"},
        {"name": "Trader Joe's",    "category": "FOOD_AND_DRINK",      "detail": "FOOD_AND_DRINK_GROCERIES",        "channel": "in store", "type": "place",   "min": 25,   "max": 150,  "website": "traderjoes.com"},
        {"name": "Amazon",          "category": "GENERAL_MERCHANDISE", "detail": "GENERAL_MERCHANDISE_ONLINE",      "channel": "online",   "type": "place",   "min": 10,   "max": 300,  "website": "amazon.com"},
        {"name": "Target",          "category": "GENERAL_MERCHANDISE", "detail": "GENERAL_MERCHANDISE_DEPARTMENT",  "channel": "in store", "type": "place",   "min": 20,   "max": 200,  "website": "target.com"},
        {"name": "Walmart",         "category": "GENERAL_MERCHANDISE", "detail": "GENERAL_MERCHANDISE_DEPARTMENT",  "channel": "in store", "type": "place",   "min": 15,   "max": 250,  "website": "walmart.com"},
        {"name": "Apple",           "category": "GENERAL_MERCHANDISE", "detail": "GENERAL_MERCHANDISE_ELECTRONICS", "channel": "online",   "type": "place",   "min": 1,    "max": 1500, "website": "apple.com"},
        {"name": "Best Buy",        "category": "GENERAL_MERCHANDISE", "detail": "GENERAL_MERCHANDISE_ELECTRONICS", "channel": "in store", "type": "place",   "min": 20,   "max": 1000, "website": "bestbuy.com"},
        {"name": "Netflix",         "category": "ENTERTAINMENT",       "detail": "ENTERTAINMENT_STREAMING",         "channel": "online",   "type": "special", "min": 15,   "max": 23,   "website": "netflix.com"},
        {"name": "Spotify",         "category": "ENTERTAINMENT",       "detail": "ENTERTAINMENT_STREAMING",         "channel": "online",   "type": "special", "min": 10,   "max": 16,   "website": "spotify.com"},
        {"name": "Hulu",            "category": "ENTERTAINMENT",       "detail": "ENTERTAINMENT_STREAMING",         "channel": "online",   "type": "special", "min": 8,    "max": 18,   "website": "hulu.com"},
        {"name": "AMC Theaters",    "category": "ENTERTAINMENT",       "detail": "ENTERTAINMENT_MOVIES",            "channel": "in store", "type": "place",   "min": 12,   "max": 50,   "website": "amctheatres.com"},
        {"name": "United Airlines", "category": "TRAVEL",              "detail": "TRAVEL_FLIGHTS",                  "channel": "online",   "type": "special", "min": 150,  "max": 800,  "website": "united.com"},
        {"name": "Airbnb",          "category": "TRAVEL",              "detail": "TRAVEL_LODGING",                  "channel": "online",   "type": "special", "min": 100,  "max": 500,  "website": "airbnb.com"},
        {"name": "Uber",            "category": "TRANSPORTATION",      "detail": "TRANSPORTATION_TAXI",             "channel": "in store", "type": "place",   "min": 8,    "max": 60,   "website": "uber.com"},
        {"name": "Lyft",            "category": "TRANSPORTATION",      "detail": "TRANSPORTATION_TAXI",             "channel": "in store", "type": "place",   "min": 8,    "max": 55,   "website": "lyft.com"},
        {"name": "CVS Pharmacy",    "category": "GENERAL_MERCHANDISE", "detail": "GENERAL_MERCHANDISE_PHARMACY",   "channel": "in store", "type": "place",   "min": 10,   "max": 100,  "website": "cvs.com"},
        {"name": "Equinox",         "category": "PERSONAL_CARE",       "detail": "PERSONAL_CARE_GYMS",              "channel": "in store", "type": "place",   "min": 150,  "max": 250,  "website": "equinox.com"},
        {"name": "Con Edison",      "category": "RENT_AND_UTILITIES",  "detail": "RENT_AND_UTILITIES_ELECTRICITY",  "channel": "online",   "type": "special", "min": 80,   "max": 200,  "website": "coned.com"},
        {"name": "Verizon",         "category": "RENT_AND_UTILITIES",  "detail": "RENT_AND_UTILITIES_PHONE",        "channel": "online",   "type": "special", "min": 60,   "max": 150,  "website": "verizon.com"},
        {"name": "Gusto Payroll",   "category": "INCOME",              "detail": "INCOME_WAGES",                    "channel": "other",    "type": "special", "min": 3000, "max": 8000, "website": None},
        {"name": "Sallie Mae",      "category": "LOAN_PAYMENTS",       "detail": "LOAN_PAYMENTS_STUDENT_LOAN",      "channel": "online",   "type": "special", "min": 200,  "max": 600,  "website": "salliemae.com"},
        {"name": "Chase Mortgage",  "category": "LOAN_PAYMENTS",       "detail": "LOAN_PAYMENTS_MORTGAGE",          "channel": "online",   "type": "special", "min": 1500, "max": 3000, "website": "chase.com"},
    ]

    def generate_transaction():
        merchant   = random.choice(merchants)
        owner_info = random.choice(ACCOUNT_OWNERS)
        days_ago   = random.randint(0, 365)
        txn_date   = (date.today() - timedelta(days=days_ago)).isoformat()
        amount     = round(random.uniform(merchant['min'], merchant['max']), 2)
        if merchant['category'] == 'INCOME':
            amount = -amount
        return {
            "transaction_id":    str(uuid.uuid4()).replace('-', '')[:38],
            "account_id":        owner_info['account_id'],
            "account_owner":     owner_info['owner'],
            "amount":            amount,
            "authorized_date":   txn_date,
            "authorized_datetime": None,
            "category":          None,
            "category_id":       None,
            "check_number":      None,
            "counterparties":    [],
            "date":              txn_date,
            "datetime":          None,
            "iso_currency_code": "USD",
            "location": {
                "address": None, "city": None, "region": None,
                "postal_code": None, "country": None,
                "lat": None, "lon": None, "store_number": None
            },
            "logo_url":          None,
            "merchant_entity_id": str(uuid.uuid4()),
            "merchant_name":     merchant['name'],
            "name":              merchant['name'],
            "payment_channel":   merchant['channel'],
            "payment_meta": {
                "reference_number": None, "ppd_id": None, "payee": None,
                "by_order_of": None, "payer": None, "payment_method": None,
                "payment_processor": None, "reason": None
            },
            "pending":           False,
            "pending_transaction_id": None,
            "personal_finance_category": {
                "confidence_level": random.choice(["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]),
                "detailed":  merchant['detail'],
                "primary":   merchant['category'],
                "version":   "v2"
            },
            "personal_finance_category_icon_url": f"https://plaid-category-icons.plaid.com/PFC_{merchant['category']}.png",
            "transaction_code":  None,
            "transaction_type":  merchant['type'],
            "unofficial_currency_code": None,
            "website":           merchant['website']
        }

    print("Generating 500,000 fake transactions...")
    fake_transactions = [generate_transaction() for _ in range(500000)]

    combined = real_transactions + fake_transactions
    print(f"Combined total: {len(combined)} transactions")

    # Save as newline delimited JSON
    with open('/tmp/raw_transactions.jsonl', 'w') as f:
        for txn in combined:
            f.write(json.dumps(txn, default=str) + '\n')

    print("Saved as newline delimited JSON")

    # Upload with extended timeout
    today      = dt.datetime.today().strftime('%Y-%m-%d')
    gcs_client = storage.Client(project=GCS_PROJECT)
    bucket     = gcs_client.bucket(GCS_BUCKET)
    blob       = bucket.blob(f'extract/raw_transactions_{today}.jsonl')
    blob.upload_from_filename('/tmp/raw_transactions.jsonl', timeout=600)

    print(f'Uploaded {len(combined)} transactions to GCS')


# ── Task 2: Transform (Native PySpark) ───────────────────────────────────────
def transform():
    import os
    os.environ['JAVA_HOME']               = '/opt/homebrew/opt/openjdk@11'
    os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    gcs_client = storage.Client(project=GCS_PROJECT)
    bucket     = gcs_client.bucket(GCS_BUCKET)

    # Get latest file from GCS
    blobs  = list(bucket.list_blobs(prefix='extract/'))
    latest = sorted(blobs, key=lambda x: x.time_created, reverse=True)[0]
    latest.download_to_filename('/tmp/raw_transactions.jsonl')
    print(f"Downloaded {latest.name}")

    # Delete old transform files for today before writing new ones
    today_str  = dt.datetime.today().strftime('%Y-%m-%d')
    old_blobs  = list(bucket.list_blobs(prefix=f'transform/plaid_transform_{today_str}'))
    for old_blob in old_blobs:
        old_blob.delete()
        print(f'Deleted old file: {old_blob.name}')

    # Start Spark session
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('plaid_transform') \
        .config('spark.driver.host', 'localhost') \
        .config('spark.driver.bindAddress', '127.0.0.1') \
        .config('spark.driver.memory', '4g') \
        .config('spark.sql.shuffle.partitions', '8') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # Read JSON natively with Spark
    df = spark.read.json('/tmp/raw_transactions.jsonl')
    print(f"Loaded {df.count()} rows into Spark")

    # Drop unneeded columns
    columns_to_drop = [
        'authorized_date', 'authorized_datetime', 'category', 'category_id',
        'check_number', 'datetime', 'pending_transaction_id', 'transaction_code',
        'unofficial_currency_code', 'payment_meta', 'logo_url',
        'personal_finance_category_icon_url', 'counterparties', 'website'
    ]
    df = df.drop(*columns_to_drop)

    # Fill account owner using native Spark map
    owner_map = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in ACCOUNT_ID_TO_OWNER.items()] for item in pair]
    )
    df = df.withColumn(
        'account_owner',
        F.when(F.col('account_owner').isNull(), owner_map[F.col('account_id')])
        .otherwise(F.col('account_owner'))
    )

    # Fill merchant_entity_id with Spark uuid()
    df = df.withColumn(
        'merchant_entity_id',
        F.when(F.col('merchant_entity_id').isNull(), F.expr('uuid()'))
        .otherwise(F.col('merchant_entity_id'))
    )

    # Flatten personal_finance_category
    df = df.withColumn('category_primary',    F.col('personal_finance_category.primary'))
    df = df.withColumn('category_detail',     F.col('personal_finance_category.detailed'))
    df = df.withColumn('category_confidence', F.col('personal_finance_category.confidence_level'))
    df = df.drop('personal_finance_category')

    # Flatten location
    df = df.withColumn('address',      F.col('location.address'))
    df = df.withColumn('city',         F.col('location.city'))
    df = df.withColumn('region',       F.col('location.region'))
    df = df.withColumn('postal_code',  F.col('location.postal_code'))
    df = df.withColumn('country',      F.col('location.country'))
    df = df.withColumn('lat',          F.col('location.lat').cast(DoubleType()))
    df = df.withColumn('lon',          F.col('location.lon').cast(DoubleType()))
    df = df.withColumn('store_number', F.col('location.store_number'))
    df = df.drop('location')

    # Fill location nulls with NYC boroughs using native Spark
    boroughs_cities    = ['Manhattan', 'Brooklyn', 'Queens', 'The Bronx', 'Staten Island']
    boroughs_lats      = [40.7831, 40.6782, 40.7282, 40.8448, 40.5795]
    boroughs_lons      = [-73.9712, -73.9442, -73.7949, -73.8648, -74.1502]
    boroughs_postcodes = ['10001', '11201', '11354', '10451', '10301']

    city_array   = F.array(*[F.lit(c) for c in boroughs_cities])
    lat_array    = F.array(*[F.lit(l) for l in boroughs_lats])
    lon_array    = F.array(*[F.lit(l) for l in boroughs_lons])
    postal_array = F.array(*[F.lit(p) for p in boroughs_postcodes])
    rand_idx     = (F.rand() * 5).cast('int')

    df = df.withColumn('city',         F.when(F.col('city').isNull(),         city_array[rand_idx]).otherwise(F.col('city')))
    df = df.withColumn('region',       F.when(F.col('region').isNull(),       F.lit('NY')).otherwise(F.col('region')))
    df = df.withColumn('postal_code',  F.when(F.col('postal_code').isNull(),  postal_array[rand_idx]).otherwise(F.col('postal_code')))
    df = df.withColumn('country',      F.when(F.col('country').isNull(),      F.lit('US')).otherwise(F.col('country')))
    df = df.withColumn('lat',          F.when(F.col('lat').isNull(),          lat_array[rand_idx] + (F.rand() * 0.02 - 0.01)).otherwise(F.col('lat')))
    df = df.withColumn('lon',          F.when(F.col('lon').isNull(),          lon_array[rand_idx] + (F.rand() * 0.02 - 0.01)).otherwise(F.col('lon')))
    df = df.withColumn('address',      F.when(F.col('address').isNull(),      F.concat(F.lit(''), F.expr('uuid()'))).otherwise(F.col('address')))
    df = df.withColumn('store_number', F.when(F.col('store_number').isNull(), F.lpad((F.rand() * 900 + 100).cast('int').cast('string'), 3, '0')).otherwise(F.col('store_number')))

    # Fix data types
    df = df.withColumn('date',       F.to_date(F.col('date'), 'yyyy-MM-dd'))
    df = df.withColumn('amount_usd', F.round(F.col('amount'), 2))
    df = df.drop('amount')

    # Rename columns
    df = df.withColumnRenamed('transaction_id', 'plaid_transaction_id')
    df = df.withColumnRenamed('name',           'transaction_name')
    df = df.drop('merchant_name')

    # Add date dimensions
    df = df.withColumn('year',         F.year('date'))
    df = df.withColumn('quarter',      F.quarter('date'))
    df = df.withColumn('month',        F.date_format('date', 'MMMM'))
    df = df.withColumn('day_of_month', F.dayofmonth('date'))
    df = df.withColumn('day_of_week',  F.date_format('date', 'EEEE'))

    # Select final columns
    df = df.select(
        'plaid_transaction_id', 'account_id', 'account_owner',
        'transaction_name', 'amount_usd',
        'date', 'iso_currency_code', 'payment_channel',
        'pending', 'transaction_type', 'merchant_entity_id',
        'category_primary', 'category_detail', 'category_confidence',
        'address', 'city', 'region', 'postal_code', 'country', 'lat', 'lon', 'store_number',
        'year', 'quarter', 'month', 'day_of_month', 'day_of_week'
    )

    print(f"Transformed {df.count()} rows with native PySpark!")

    # Save to parquet
    df.write.mode('overwrite').parquet('/tmp/plaid_transform.parquet')
    print("Saved to parquet locally")

    # Upload parquet parts to GCS
    import glob
    parquet_files = glob.glob('/tmp/plaid_transform.parquet/part-*.parquet')
    for i, file in enumerate(parquet_files):
        blob = bucket.blob(f'transform/plaid_transform_{today_str}_part{i}.parquet')
        blob.upload_from_filename(file, timeout=600)

    print(f'Uploaded {len(parquet_files)} parquet parts to GCS')
    spark.stop()


# ── Task 3: Load ──────────────────────────────────────────────────────────────
def load():
    import pandas as pd

    gcs_client = storage.Client(project=GCS_PROJECT)
    bucket     = gcs_client.bucket(GCS_BUCKET)

    today_str = dt.datetime.today().strftime('%Y-%m-%d')
    blobs     = list(bucket.list_blobs(prefix=f'transform/plaid_transform_{today_str}'))

    print(f"Found {len(blobs)} parquet parts to load")

    dfs = []
    for blob in blobs:
        local_path = f'/tmp/{blob.name.split("/")[-1]}'
        blob.download_to_filename(local_path)
        dfs.append(pd.read_parquet(local_path))

    df = pd.concat(dfs, ignore_index=True)

    # Deduplicate on plaid_transaction_id to prevent any duplicates
    before = len(df)
    df = df.drop_duplicates(subset=['plaid_transaction_id'])
    after  = len(df)
    print(f"Deduped: {before} → {after} rows ({before - after} duplicates removed)")

    # Fix date column type for BigQuery
    df['date'] = pd.to_datetime(df['date'])

    print(f"Final row count: {len(df)}")

    # Build star schema tables
    dim_date = df[['date','year','quarter','month','day_of_month','day_of_week']]\
        .drop_duplicates().copy().reset_index(drop=True)
    dim_date.insert(0, 'date_id', range(1, len(dim_date) + 1))

    dim_location = df[['address','city','region','postal_code','country','lat','lon','store_number']]\
        .copy().reset_index(drop=True)
    dim_location.insert(0, 'location_id', range(1, len(dim_location) + 1))

    dim_category = df[['category_primary','category_detail','category_confidence']]\
        .drop_duplicates(subset='category_primary').copy().reset_index(drop=True)
    dim_category.insert(0, 'category_id', range(1, len(dim_category) + 1))

    dim_account = df[['account_id','account_owner']]\
        .drop_duplicates().copy().reset_index(drop=True)
    dim_account.insert(0, 'account_pk', range(1, len(dim_account) + 1))

    date_map     = dim_date.set_index('date')['date_id']
    category_map = dim_category.set_index('category_primary')['category_id']

    fact_transactions = df[[
        'plaid_transaction_id','account_id','amount_usd','transaction_name',
        'iso_currency_code','payment_channel','pending',
        'transaction_type','merchant_entity_id',
    ]].copy().reset_index(drop=True)

    # Use plaid_transaction_id as primary key — guaranteed unique
    fact_transactions.insert(0, 'transaction_id', fact_transactions['plaid_transaction_id'])
    fact_transactions.insert(2, 'account_id',  fact_transactions.pop('account_id'))
    fact_transactions.insert(3, 'date_id',     df['date'].map(date_map).values)
    fact_transactions.insert(4, 'location_id', dim_location['location_id'])
    fact_transactions.insert(5, 'category_id', df['category_primary'].map(category_map).values)

    today_str = dt.datetime.today().strftime('%Y-%m-%d')
    tables = {
        'fact_transactions': fact_transactions,
        'dim_account':       dim_account,
        'dim_location':      dim_location,
        'dim_category':      dim_category,
        'dim_date':          dim_date,
    }

    for table_name, df_table in tables.items():
        filename = f'/tmp/{table_name}.parquet'
        df_table.to_parquet(filename, index=False)
        blob = bucket.blob(f'load/{table_name}_{today_str}.parquet')
        blob.upload_from_filename(filename, timeout=600)

    bq_client = bigquery.Client(project=GCS_PROJECT)
    for table_name, df_table in tables.items():
        table_id   = f'{GCS_PROJECT}.{BQ_DATASET}.{table_name}'
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
        job = bq_client.load_table_from_dataframe(df_table, table_id, job_config=job_config)
        job.result()
        print(f'Loaded {table_name} — {len(df_table)} rows into BigQuery')

    print('All tables loaded into BigQuery!')


# ── Task 4: dbt ───────────────────────────────────────────────────────────────
def dbt_run():
    import subprocess
    result = subprocess.run(
        ['dbt', 'run',
         '--project-dir', '/Users/ramizkhatib/plaid_dbt',
         '--profiles-dir', '/Users/ramizkhatib/.dbt'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f'dbt run failed:\n{result.stderr}')
    print('dbt run completed successfully!')


# ── DAG Definition ────────────────────────────────────────────────────────────
default_args = {
    'owner':            'Ramiz',
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='plaid_pipeline',
    default_args=default_args,
    description='Daily Plaid pipeline: Truncate -> Extract -> PySpark -> Load -> dbt',
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 15),
    catchup=False,
    tags=['plaid', 'etl', 'bigquery', 'dbt', 'pyspark'],
) as dag:

    t0_truncate = PythonOperator(
        task_id='truncate_tables',
        python_callable=truncate_tables,
    )

    t1_extract = PythonOperator(
        task_id='extract_from_plaid',
        python_callable=extract,
    )

    t2_transform = PythonOperator(
        task_id='transform_data_pyspark',
        python_callable=transform,
    )

    t3_load = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load,
    )

    t4_dbt = PythonOperator(
        task_id='dbt_run',
        python_callable=dbt_run,
    )

    t0_truncate >> t1_extract >> t2_transform >> t3_load >> t4_dbt
