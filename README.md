# Stock Data Pipeline

Automated pipeline that fetches daily stock prices from Alpha Vantage API and stores them in PostgreSQL. Runs on Apache Airflow with Telegram alerts on failure.

## Architecture

[Alpha Vantage API]
│
▼
[Airflow DAG - stock_pipeline]
│
├── fetch_stock_data (with 3 retries, exponential backoff)
│
└── insert_data_to_db (idempotent upsert)
│
▼
[PostgreSQL - stock_prices table]
│
└── on_failure → Telegram alert

## Tech Stack

- Python 3
- Apache Airflow 2.8
- PostgreSQL 15
- Docker & Docker Compose
- Telegram Bot API (alerts)

## How to Run

1. Clone the repo
   git clone https://github.com/Lynixt/Broken-Miner.git
   cd broken-miner

2. Create a `.env` file with:
   ALPHA_VANTAGE_API_KEY=your_key_here
   TELEGRAM_BOT_TOKEN=your_token_here
   TELEGRAM_CHAT_ID=your_chat_id_here

3. Start the services
   docker compose up -d --build

4. Open Airflow at http://localhost:8080 (username: admin, password: admin)

5. Trigger the `stock_pipeline` DAG

## Features

- **Retry logic**: API calls retry up to 3 times with exponential backoff
- **Idempotency**: `ON CONFLICT DO NOTHING` prevents duplicate rows
- **Telegram alerts**: Automatic notification when the pipeline fails
- **Scheduled**: Runs Monday-Friday at 8:00 AM UTC

## What I Learned

- Building end-to-end data pipelines with Airflow
- Docker Compose for multi-container applications
- Error handling and alerting for production systems
- Environment variable management and security
- Debugging containerized pipelines
- Git version control and project documentation

## Project Status

Project 1 of 3 for data engineering portfolio. Working and actively maintained.
