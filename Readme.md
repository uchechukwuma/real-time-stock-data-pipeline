# 📈 Real-Time Stock Data Pipeline with Apache Airflow, Kafka, PostgreSQL, and Redis

# Architecture Blueprint
graph TD
    subgraph Ingestion & Streaming
        A[Alpha Vantage API] -- Historical CSV Data --> B[Kafka Producer Microservice]
        B -- Publish Records --> C[Kafka Broker]
    end

    subgraph Persistence & Storage
        C -- Subscribe & Pull Streams --> D[Kafka Consumer Microservice]
        D -- Batched Writes --> E[(PostgreSQL Database)]
    end

    subgraph Orchestration & Workers
        F[Apache Airflow] -- Directs Workflows --> G[Redis Celery Broker]
        G -- Distributes Tasks --> H[Airflow Workers]
        F -. Monitors & Health Checks .-> B
        F -. Monitors & Health Checks .-> D
    end

    style A fill:#fff,stroke:#333,stroke-width:2px
    style C fill:#fff,stroke:#231F20,stroke-width:2px
    style E fill:#336791,stroke:#fff,stroke-width:1px,color:#fff
    style F fill:#017CEB,stroke:#fff,stroke-width:1px,color:#fff
    style G fill:#D82C20,stroke:#fff,stroke-width:1px,color:#fff

This project sets up a real-time data pipeline using:

- 🛰 **Apache Kafka** for streaming
- ⏰ **Apache Airflow** for orchestration
- 🐘 **PostgreSQL** as a database
- 🧠 **Redis** for message brokering
- 🐍 **Python** apps for Kafka producer & consumer
- 🐳 **Docker Compose** for containerization

## 📦 Features

- Fetches real-time stock data using the [Alpha Vantage API](https://www.alphavantage.co/)
- Streams data through Kafka
- Consumes and stores data in PostgreSQL
- DAGs for scheduling and monitoring with Airflow
- Redis broker used for CeleryExecutor

⚠️ Data Streaming Note (Simulated Streaming)
To simulate real-time data ingestion while avoiding API rate limits and financial costs, this project fetches historical stock data using the Alpha Vantage API (in CSV format). The Kafka producer sends this data to a Kafka topic record by record — simulating a real-time feed for demonstration purposes.

This setup mimics the structure and behavior of a production-ready streaming system and can be easily adapted to consume live tick data or event streams in a real deployment.
---

## 📁 Project Structure
---text
📁 Project Structure
├── dags/                     # Airflow Orchestration Layer
│   └── kafka_pipeline_dag.py # Monitors microservice statuses
├── services/
│   ├── producer/             # Decoupled Producer Microservice
│   │   ├── kafka_producer.py
│   │   └── Dockerfile.producer
│   └── consumer/             # Decoupled Consumer Microservice
│       ├── kafka_consumer.py
│       └── Dockerfile.consumer
├── Dockerfile.airflow        # Custom Airflow Build Configuration
├── docker-compose.yml        # Multi-container service infrastructure
├── requirements.txt          # Shared Python dependencies
├── .env.example              # Production safe template environment variables
└── README.md

---

## ⚙️ Prerequisites

- Docker and Docker Compose installed
- An [Alpha Vantage API Key](https://www.alphavantage.co/support/#api-key)

---

## 🔐 Environment Configuration

Create a `.env` file in the root of your project.

### Sample `.env` (with safe placeholders):

```env
# === Stock API ===
STOCK_SYMBOL=TSLA
ALPHAVANTAGE_API_KEY=your_alpha_vantage_key

# === PostgreSQL ===
POSTGRES_USER=airflow
POSTGRES_PASSWORD=your_pg_password
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# === Redis ===
REDIS_HOST=redis
REDIS_PORT=6379

# === Kafka ===
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=stock_prices_topic

# === Airflow ===
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true

# === Airflow UI Login ===
_AIRFLOW_WWW_USER_USERNAME= xxxxxx
_AIRFLOW_WWW_USER_PASSWORD= xxxxxx

# === pgAdmin ===
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD= xxxxxxx


🛡️ Make sure to add .env to your .gitignore:
echo ".env" >> .gitignore

🚀 Getting Started
1. Clone the Repo
git clone https://github.com/yourusername/yourproject.git
cd yourproject

2. Set Up .env
Create a .env file based on the sample above and insert your credentials.

3. Generate Fernet Key (Optional but Recommended)
If you're using encrypted connections or secrets in Airflow:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Paste it into the .env under AIRFLOW__CORE__FERNET_KEY.

4. Build & Start the Stack
docker-compose up --build

The first build might take a few minutes.

🌐 Access Services
| Service          | URL                                            |
| ---------------- | ---------------------------------------------- |
| Airflow UI       | [http://localhost:8080](http://localhost:8080) |
| pgAdmin          | [http://localhost:5050](http://localhost:5050) |
| Kafka (internal) | kafka:9092                                     |
| Redis (internal) | redis:6379                                     |

Login to Airflow with:
Username: xxxxxxxxx  -- 
Password: xxxxxxxxx


📊 Data Flow Summary
Kafka Producer fetches real-time stock data and sends it to stock_prices_topic

Kafka Consumer reads messages from the topic and writes them to PostgreSQL

Airflow DAG orchestrates, schedules, and monitors the process

📌 Notes
The .env file is used across all services (Airflow, Kafka, PostgreSQL, etc.)

Make sure ports 8080, 5432, 5050, and 6379 are not blocked on your machine.

DAGs are mounted from the dags/ folder.

🧹 Cleanup
To stop and remove containers, networks, and volumes:
docker-compose down -v

License
MIT License. Feel free to fork and modify.

🧠 Credits
Built with ❤️ using:

Apache Airflow

Apache Kafka

Redis

PostgreSQL

Docker
