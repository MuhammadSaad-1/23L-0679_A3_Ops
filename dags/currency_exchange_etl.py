"""
    An Apache Airflow ETL pipeline scheduled with Astro CLI to:
    1. Extract daily currency exchange rates from the Frankfurter API.
    2. Transform data into a standardized structure using Named XComs.
    3. Load data into MongoDB with persistent storage and Mongo Express UI.
    
Security Practices:
    - Zero credentials in source code. Secrets are strictly required from environment variables (.env).
    - Pipeline fails fast if environment secrets are not provided.
    - Database is isolated from host network; accessible only via internal Docker network.
    - Passwords are automatically masked in task execution logs.
"""

import os
from datetime import datetime, timezone
import logging
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': '23L-0679',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

# API Endpoints
API_URL_V2 = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
API_URL_V1_FALLBACK = "https://api.frankfurter.dev/v1/latest?base=EUR&symbols=USD,GBP,JPY"

# Non-sensitive network configuration defaults
MONGO_HOST = os.getenv("MONGO_HOST", "assignment3_mongodb")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
DATABASE_NAME = os.getenv("MONGO_DB_NAME", "currency_db")
COLLECTION_NAME = "exchange_rates"

# Named XCom Keys
RAW_DATA_XCOM_KEY = "raw_currency_data"
TRANSFORMED_DATA_XCOM_KEY = "transformed_currency_data"


def _get_mongo_client():
    """
    Helper function to securely retrieve MongoDB credentials from environment
    and instantiate MongoClient without exposing secrets in code.
    Enforces authentication against the admin authentication database.
    Raises EnvironmentError if credentials are missing.
    """
    mongo_user = os.getenv("MONGO_ROOT_USER")
    mongo_pass = os.getenv("MONGO_ROOT_PASSWORD")

    if not mongo_user or not mongo_pass:
        raise EnvironmentError(
            "Security validation failed: MONGO_ROOT_USER or MONGO_ROOT_PASSWORD is not set "
            "in the environment variables. Please check your .env configuration."
        )

    # Candidate hostnames to resolve on the Docker bridge network
    candidates = [
        MONGO_HOST,
        "assignment3_mongodb",
        "mongodb"
    ]
    unique_candidates = list(dict.fromkeys([h for h in candidates if h]))

    client = None
    last_err = None

    for host in unique_candidates:
        uri = f"mongodb://{mongo_user}:{mongo_pass}@{host}:{MONGO_PORT}/?authSource=admin"
        try:
            test_client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            test_client.admin.command('ping')
            client = test_client
            logging.info("Successfully authenticated and connected to MongoDB at host '%s'!", host)
            break
        except Exception as err:
            last_err = err
            logging.warning("Failed to authenticate to MongoDB at host '%s': %s", host, err)

    if not client:
        raise last_err or ConnectionError("Failed to authenticate to MongoDB on any candidate host.")

    return client, mongo_user


def extract_currency_rates(**context):
    """
    Task 1: Extract Currency Exchange Rates from Frankfurter API.
    Pushes raw response data to Named XCom: 'raw_currency_data'.
    """
    logging.info("Starting Task 1: Extract Currency Exchange Rates from Frankfurter API...")
    
    headers = {"User-Agent": "Airflow-ETL-Student-Pipeline/1.0"}
    
    try:
        logging.info("Attempting fetch from primary v2 endpoint: %s", API_URL_V2)
        response = requests.get(API_URL_V2, headers=headers, timeout=15)
        response.raise_for_status()
        raw_json = response.json()
    except Exception as exc:
        logging.warning("Primary v2 endpoint failed (%s). Attempting v1 fallback: %s", exc, API_URL_V1_FALLBACK)
        response = requests.get(API_URL_V1_FALLBACK, headers=headers, timeout=15)
        response.raise_for_status()
        raw_json = response.json()

    logging.info("Successfully fetched API payload: %s", raw_json)

    # Push to Named XCom
    ti = context['ti']
    ti.xcom_push(key=RAW_DATA_XCOM_KEY, value=raw_json)
    logging.info("Pushed raw data to Named XCom with key: '%s'", RAW_DATA_XCOM_KEY)


def transform_exchange_rate_data(**context):
    """
    Task 2: Transform Exchange Rate Data.
    Pulls raw data from Named XCom, structures records according to the required schema,
    and pushes transformed records to Named XCom: 'transformed_currency_data'.
    Handles both Frankfurter v2 (list of quote objects) and v1 (nested dict) responses.
    """
    logging.info("Starting Task 2: Transform Exchange Rate Data...")
    ti = context['ti']
    
    # Pull from Named XCom
    raw_data = ti.xcom_pull(key=RAW_DATA_XCOM_KEY, task_ids='extract_currency_exchange_rates')
    if raw_data is None:
        raise ValueError(f"No data received from Named XCom key '{RAW_DATA_XCOM_KEY}'")

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed_records = []

    # Handle Frankfurter v2 endpoint response format (List of objects: [{'date': ..., 'base': ..., 'quote': ..., 'rate': ...}])
    if isinstance(raw_data, list):
        logging.info("Detected Frankfurter v2 list format with %d items.", len(raw_data))
        for item in raw_data:
            record = {
                "base_currency": str(item.get("base", "EUR")),
                "target_currency": str(item.get("quote")),
                "exchange_rate": float(item.get("rate", 0.0)),
                "rate_date": str(item.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))),
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at
            }
            transformed_records.append(record)
            logging.info("Transformed record: %s", record)

    # Handle Frankfurter v1 / dict fallback format ({'base': ..., 'date': ..., 'rates': {...}})
    elif isinstance(raw_data, dict):
        logging.info("Detected Frankfurter dictionary format.")
        base_currency = raw_data.get('base', 'EUR')
        rate_date = raw_data.get('date', datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        rates = raw_data.get('rates') or raw_data.get('quotes') or {}
        
        for target_currency, exchange_rate in rates.items():
            record = {
                "base_currency": str(base_currency),
                "target_currency": str(target_currency),
                "exchange_rate": float(exchange_rate),
                "rate_date": str(rate_date),
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at
            }
            transformed_records.append(record)
            logging.info("Transformed record: %s", record)
    else:
        raise TypeError(f"Unexpected data type for raw_data: {type(raw_data)}")

    logging.info("Total transformed records created: %d", len(transformed_records))

    # Push transformed records to Named XCom
    ti.xcom_push(key=TRANSFORMED_DATA_XCOM_KEY, value=transformed_records)
    logging.info("Pushed transformed records to Named XCom with key: '%s'", TRANSFORMED_DATA_XCOM_KEY)


def load_data_into_mongodb(**context):
    """
    Task 3: Load Data into MongoDB.
    Pulls transformed data from Named XCom and loads records into MongoDB.
    Uses upsert to ensure idempotent runs without duplicate clutter.
    """
    logging.info("Starting Task 3: Load Data into MongoDB...")
    ti = context['ti']
    
    # Pull from Named XCom
    transformed_records = ti.xcom_pull(key=TRANSFORMED_DATA_XCOM_KEY, task_ids='transform_exchange_rate_data')
    if not transformed_records:
        raise ValueError(f"No records received from Named XCom key '{TRANSFORMED_DATA_XCOM_KEY}'")

    client, mongo_user = _get_mongo_client()

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    upserted_count = 0
    for record in transformed_records:
        filter_query = {
            "base_currency": record["base_currency"],
            "target_currency": record["target_currency"],
            "rate_date": record["rate_date"]
        }
        result = collection.update_one(filter_query, {"$set": record}, upsert=True)
        if result.upserted_id or result.modified_count:
            upserted_count += 1
        logging.info("Saved record for %s -> %s (%s)", 
                     record["base_currency"], record["target_currency"], record["rate_date"])

    total_docs = collection.count_documents({})
    logging.info("Successfully loaded data. Processed %d records. Total documents in '%s.%s': %d", 
                 len(transformed_records), DATABASE_NAME, COLLECTION_NAME, total_docs)
    
    client.close()


# Define the DAG (Compatible with Airflow 3.x / Astro Runtime 3.x)
with DAG(
    dag_id='currency_exchange_etl',
    default_args=default_args,
    description='ETL pipeline integrating daily Frankfurter currency exchange rates into MongoDB',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['mlops', 'assignment3', 'frankfurter', 'mongodb', 'xcom'],
) as dag:

    # Task 1: Extract Currency Exchange Rates
    extract_task = PythonOperator(
        task_id='extract_currency_exchange_rates',
        python_callable=extract_currency_rates,
    )

    # Task 2: Transform Exchange Rate Data
    transform_task = PythonOperator(
        task_id='transform_exchange_rate_data',
        python_callable=transform_exchange_rate_data,
    )

    # Task 3: Load Data into MongoDB
    load_task = PythonOperator(
        task_id='load_data_into_mongodb',
        python_callable=load_data_into_mongodb,
    )

    # Set linear pipeline dependencies
    extract_task >> transform_task >> load_task
