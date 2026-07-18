FROM apache/airflow:2.8.1
RUN pip install requests psycopg2-binary python-dotenv