import json
import os
import time

import requests

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

from event_parser import parse_github_event


load_dotenv()


GITHUB_EVENTS_URL = os.getenv(
    "GITHUB_EVENTS_URL",
    "https://api.github.com/events",
)

GITHUB_TOKEN = os.getenv(
    "GITHUB_TOKEN",
)

GITHUB_POLL_INTERVAL = int(
    os.getenv(
        "GITHUB_POLL_INTERVAL",
        "10",
    )
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "github-events",
)


MAX_API_RETRIES = 5

INITIAL_RETRY_DELAY = 1

SEEN_EVENT_CACHE_SIZE = 5000


def create_http_session():

    session = requests.Session()

    session.headers.update(
        {
            "Accept": (
                "application/vnd.github+json"
            ),
            "X-GitHub-Api-Version": (
                "2022-11-28"
            ),
            "User-Agent": (
                "github-streaming-pipeline"
            ),
        }
    )

    if GITHUB_TOKEN:

        session.headers.update(
            {
                "Authorization": (
                    f"Bearer {GITHUB_TOKEN}"
                )
            }
        )

    return session


def fetch_github_events(
    session,
):

    delay = INITIAL_RETRY_DELAY

    for attempt in range(
        1,
        MAX_API_RETRIES + 1,
    ):

        try:

            response = session.get(
                GITHUB_EVENTS_URL,
                timeout=10,
            )

            if response.status_code == 200:

                return response.json()

            if (
                response.status_code == 429
                or 500
                <= response.status_code
                < 600
            ):

                print(
                    "[API RETRY] "
                    f"status="
                    f"{response.status_code} "
                    f"attempt="
                    f"{attempt}/"
                    f"{MAX_API_RETRIES}"
                )

                if (
                    attempt
                    == MAX_API_RETRIES
                ):
                    response.raise_for_status()

                time.sleep(
                    delay
                )

                delay *= 2

                continue

            response.raise_for_status()

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:

            print(
                "[API RETRY] "
                f"type="
                f"{type(error).__name__} "
                f"attempt="
                f"{attempt}/"
                f"{MAX_API_RETRIES}"
            )

            if (
                attempt
                == MAX_API_RETRIES
            ):
                raise

            time.sleep(
                delay
            )

            delay *= 2

    return []


def create_kafka_producer():

    return KafkaProducer(
        bootstrap_servers=(
            KAFKA_BOOTSTRAP_SERVERS
        ),
        key_serializer=lambda key: (
            key.encode("utf-8")
            if key
            else None
        ),
        value_serializer=lambda value: (
            json.dumps(
                value,
                ensure_ascii=False,
            ).encode(
                "utf-8"
            )
        ),
        acks="all",
        retries=5,
    )


def send_event(
    producer,
    event,
):

    event_key = (
        event.get("repo_name")
        or event.get("event_id")
    )

    future = producer.send(
        KAFKA_TOPIC,
        key=event_key,
        value=event,
    )

    try:

        metadata = future.get(
            timeout=10
        )

        print(
            "[KAFKA SENT] "
            f"event_id="
            f"{event.get('event_id')} "
            f"partition="
            f"{metadata.partition} "
            f"offset="
            f"{metadata.offset}"
        )

    except KafkaError:

        print(
            "[KAFKA ERROR] "
            f"event_id="
            f"{event.get('event_id')}"
        )

        raise


def main():

    session = (
        create_http_session()
    )

    producer = (
        create_kafka_producer()
    )

    seen_event_ids = set()

    try:

        while True:

            raw_events = (
                fetch_github_events(
                    session
                )
            )

            new_event_ids = []

            for raw_event in raw_events:

                event = (
                    parse_github_event(
                        raw_event
                    )
                )

                event_id = (
                    event.get(
                        "event_id"
                    )
                )

                if not event_id:
                    continue

                if (
                    event_id
                    in seen_event_ids
                ):
                    continue

                send_event(
                    producer,
                    event,
                )

                seen_event_ids.add(
                    event_id
                )

                new_event_ids.append(
                    event_id
                )

            if (
                len(seen_event_ids)
                > SEEN_EVENT_CACHE_SIZE
            ):

                seen_event_ids = set(
                    new_event_ids
                )

            producer.flush()

            print(
                "[POLL COMPLETE] "
                f"received="
                f"{len(raw_events)} "
                f"sent="
                f"{len(new_event_ids)}"
            )

            time.sleep(
                GITHUB_POLL_INTERVAL
            )

    except KeyboardInterrupt:

        print(
            "\nProducer stopped."
        )

    finally:

        producer.flush()

        producer.close()

        session.close()


if __name__ == "__main__":
    main()