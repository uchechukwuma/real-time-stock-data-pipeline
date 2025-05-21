import os
from dotenv import load_dotenv
import time
import json
import logging
import requests
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import io
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psycopg2

# Load environment variables
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
SYMBOL = os.getenv('STOCK_SYMBOL')

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

REQUIRED_COLUMNS = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
MAX_API_RETRIES = 3
MAX_KAFKA_RETRIES = 3
BASE_DELAY = 1  # seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&apikey={API_KEY}&datatype=csv'

def get_latest_timestamp_from_db() -> Optional[str]:
    """Connect to Postgres and get latest timestamp to send only new data."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM stock_prices;")
        latest = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        if latest:
            # Format to string for comparison
            return latest.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            return None
    except Exception as e:
        logger.warning(f"Could not fetch latest timestamp from DB: {e}")
        return None

@retry(
    stop=stop_after_attempt(MAX_API_RETRIES),
    wait=wait_exponential(multiplier=BASE_DELAY),
    retry=retry_if_exception_type((requests.exceptions.RequestException, pd.errors.EmptyDataError)),
    before_sleep=lambda retry_state: logger.warning(
        f"Retrying API call after failure: {retry_state.outcome.exception()}... "
        f"Attempt {retry_state.attempt_number}/{MAX_API_RETRIES}"
    )
)
def fetch_stock_data() -> Optional[pd.DataFrame]:
    logger.info(f"🌐 Fetching daily stock data for {SYMBOL}...")
    response = requests.get(url)
    response.raise_for_status()

    # Check for API rate limit message
    if "Thank you for using" in response.text or "Note" in response.text:
        logger.warning("API rate limit likely exceeded. Skipping fetch.")
        return pd.DataFrame()

    df = pd.read_csv(io.StringIO(response.text))

    if df.empty:
        raise pd.errors.EmptyDataError("No data returned from API")

    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")

    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    return df


def send_to_kafka_with_retry(producer, topic, message):
    for attempt in range(1, MAX_KAFKA_RETRIES + 1):
        try:
            future = producer.send(topic, message)
            future.get(timeout=10)
            return True
        except KafkaError as e:
            wait_time = BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(f"Kafka send attempt {attempt} failed. Retrying in {wait_time:.1f}s. Error: {e}")
            time.sleep(wait_time)
    logger.error(f"Failed to send message to Kafka after {MAX_KAFKA_RETRIES} attempts.")
    return False

def send_to_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        df = fetch_stock_data()
        if df is None or df.empty:
            logger.warning("No data fetched to send.")
            return
        print("DataFrame columns:", df.columns)

        #latest_db_ts = get_latest_timestamp_from_db()
        #if latest_db_ts:
            # Only keep rows with timestamp > latest timestamp in DB
           # df = df[df['timestamp'] > latest_db_ts]

        if df.empty:
            logger.info("No new data to send after filtering by latest DB timestamp.")
            return

        logger.info(f"Sending {len(df)} new records to Kafka topic '{KAFKA_TOPIC}'")

        success_count = 0
        failure_count = 0

        for _, row in df.iterrows():
            message = row.to_dict()
            if send_to_kafka_with_retry(producer, KAFKA_TOPIC, message):
                success_count += 1
                if success_count <= 3:
                    logger.info(f"✅ Sent: {message}")
            else:
                failure_count += 1

        producer.flush()
        logger.info(f"Kafka send results: {success_count} successful, {failure_count} failed.")

    except Exception as e:
        logger.error(f"Unexpected error in send_to_kafka: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    send_to_kafka()
