from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
import os
import time
from dotenv import load_dotenv
from pathlib import Path

# --- Configuration ---
STOCKS = ["AAPL", "GOOGL", "MSFT"]
BASE_URL = "https://www.alphavantage.co/query"

# Find and load .env
env_path = Path("/opt/airflow") / ".env"
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# --- Database Connection ---
def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database="stock_prices",
        user="dataengineer",
        password="secretpassword",
        port=5432,
    )

# --- Fetch Stock Data ---
def fetch_stock_data(symbol, retries=3, backoff=2):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact",
    }
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "Time Series (Daily)" not in data:
                raise ValueError(f"API error for {symbol}: {data.get('Note', 'Unknown')}")
            print(f"  [{symbol}] Success! {len(data['Time Series (Daily)'])} days.")
            return data
        except (requests.RequestException, ValueError) as e:
            print(f"  [{symbol}] Attempt {attempt} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)

# --- Insert Into Database ---
def insert_data_to_db(data, symbol):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        );
    """)
    time_series = data["Time Series (Daily)"]
    rows = 0
    for date_str, values in time_series.items():
        try:
            cursor.execute("""
                INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING;
            """, (
                symbol, date_str,
                float(values["1. open"]), float(values["2. high"]),
                float(values["3. low"]), float(values["4. close"]),
                int(values["5. volume"]),
            ))
            rows += 1
        except Exception as e:
            print(f"  [!] Row error {symbol} {date_str}: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"  [{symbol}] Inserted {rows} rows.")

# --- Default args ---
default_args = {
    "owner": "Lynixt",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 7, 17),
}

# --- DAG ---
with DAG(
    dag_id="stock_pipeline",
    default_args=default_args,
    description="Fetch daily stock prices and load into PostgreSQL",
    schedule_interval="0 8 * * 1-5",
    catchup=False,
    tags=["stocks", "beginner"],
) as dag:

    def task_fetch(**context):
        results = {}
        for stock in STOCKS:
            results[stock] = fetch_stock_data(stock)
        return results

    def task_insert(**context):
        ti = context["task_instance"]
        results = ti.xcom_pull(task_ids="fetch_stock_data")
        for stock, data in results.items():
            insert_data_to_db(data, stock)

    fetch_data = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=task_fetch,
    )

    insert_data = PythonOperator(
        task_id="insert_data_to_db",
        python_callable=task_insert,
    )

    fetch_data >> insert_data