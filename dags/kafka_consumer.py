import os
import json
import logging
import psycopg2
import signal
import sys
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
POSTGRES_DB = 'stock_db'
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'stock-consumer-group') 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

def create_database_if_not_exists():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname='stock_db',
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_DB,))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'CREATE DATABASE {POSTGRES_DB}')
            logger.info(f"✅ Database '{POSTGRES_DB}' created.")
        else:
            logger.info(f"ℹ️ Database '{POSTGRES_DB}' already exists.")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Failed to check/create database: {e}")

def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )

def create_table_if_not_exists(conn, cursor):
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'stock_prices'
        );
    """)
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        cursor.execute("""
            CREATE TABLE stock_prices (
                timestamp TIMESTAMPTZ PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INT
            );
        """)
        conn.commit()
        logger.info("✅ Table 'stock_prices' created.")
    else:
        logger.info("ℹ️ Table 'stock_prices' already exists.")

def signal_handler(sig, frame):
    logger.info(f"📴 Signal {sig} received: Shutting down consumer gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def consume_data():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    create_database_if_not_exists()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    create_table_if_not_exists(conn, cursor)

    try:
        for message in consumer:
            data = message.value
            logger.info(f"📥 Received message: {data}")
            try:
                cursor.execute("""
                    INSERT INTO stock_prices (timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """, (
                    data.get('timestamp'),
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('volume')
                ))
                conn.commit()
                logger.info("✅ Data inserted/updated in database.")
            except Exception as e:
                logger.error(f"❌ DB insert failed for data {data}: {e}")
                conn.rollback()
    except Exception as e:
        logger.error(f"❌ Kafka consumer error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    consume_data()
