import os

from dotenv import load_dotenv


load_dotenv()


# ---------------------------------------------------------
# GitHub
# ---------------------------------------------------------

GITHUB_EVENTS_URL = os.getenv(
    "GITHUB_EVENTS_URL",
    "https://api.github.com/events",
)

GITHUB_POLL_INTERVAL = int(
    os.getenv(
        "GITHUB_POLL_INTERVAL",
        "10",
    )
)

GITHUB_TOKEN = os.getenv(
    "GITHUB_TOKEN",
    "",
)


# ---------------------------------------------------------
# Kafka
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "github-events",
)

KAFKA_DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "github-events-dlq",
)


# ---------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost",
)

POSTGRES_PORT = int(
    os.getenv(
        "POSTGRES_PORT",
        "5432",
    )
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "github_streaming",
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "github_user",
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "github_password",
)