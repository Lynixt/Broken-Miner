import requests
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import time

from pathlib import Path

# Find .env file relative to this script's location
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# --- Configuration ---
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")  # We'll get this next
STOCKS = ["AAPL", "GOOGL", "MSFT"]
BASE_URL = "https://www.alphavantage.co/query"

# --- Database Connection ---
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="stock_prices",
        user="dataengineer",
        password="secretpassword",
        port=5432
    )
    
# --- The Miner (with retry logic) ---
def fetch_stock_data(symbol, retries=3, backoff=2):
    """
    Fetches stock data from Alpha Vantage.
    If it fails, it retries with increasing wait time.
    This is the 'Broken Miner' defense.
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }
    
    for attempt in range(1, retries + 1):
        try:
            print(f"  [{symbol}] Attempt {attempt}...")
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()  # Raise error for bad HTTP status
            data = response.json()
            
            # --- Data Validation (The Ore Purity Check) ---
            if "Time Series (Daily)" not in data:
                error_msg = data.get("Note", data.get("Error Message", "Unknown API error"))
                raise ValueError(f"API returned unexpected data for {symbol}: {error_msg}")
            
            print(f"  [{symbol}] Success! Got {len(data['Time Series (Daily)'])} days of data.")
            return data
          
        except (requests.RequestException, ValueError) as e:
            print(f"  [{symbol}] Failed: {e}")
            if attempt == retries:
                print(f"  [{symbol}] ALL RETRIES EXHAUSTED. Giving up.")
                raise  # Re-raise the exception after final retry
            wait_time = backoff ** attempt
            print(f"  [{symbol}] Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            
# --- The Inserter (Saving to Storage Container) ---
def insert_data_to_db(data, symbol):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create table if it doesn't exist (first run)
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
    rows_inserted = 0
    
    for date_str, values in time_series.items():
        try:
            cursor.execute("""
                INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING;
            """, (
                symbol,
                date_str,
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(values["5. volume"])
            ))
            rows_inserted += 1
        except Exception as e:
            print(f"  [!] Failed to insert row for {symbol} on {date_str}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"  [{symbol}] Inserted {rows_inserted} new rows into database.")

# --- Main (Run Manually for Now) ---
if __name__ == "__main__":
    print(f"=== Starting Stock Ingestion at {datetime.now()} ===")
    for stock in STOCKS:
        try:
            data = fetch_stock_data(stock)
            insert_data_to_db(data, stock)
        except Exception as e:
            print(f"[!] FATAL: Could not process {stock}: {e}")
            # In production, this would trigger an alert
    print(f"=== Ingestion Complete at {datetime.now()} ===")