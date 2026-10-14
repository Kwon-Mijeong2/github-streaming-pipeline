import os
import time

import psycopg2

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    struct,
    to_json,
    to_timestamp,
    when,
    window,
)

from schemas import GITHUB_EVENT_SCHEMA


# =========================================================
# Environment
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "github-events",
)

KAFKA_DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "github-events-dlq",
)


POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "postgres",
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


CHECKPOINT_BASE = (
    "/opt/spark/work-dir/checkpoints"
)


# =========================================================
# Spark
# =========================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName(
            "GitHubStreamingPipeline"
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )


# =========================================================
# Kafka Source
# =========================================================

def read_kafka_stream(
    spark,
):

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            KAFKA_TOPIC,
        )
        .option(
            "startingOffsets",
            "latest",
        )
        .load()
    )


def prepare_kafka_data(
    kafka_df,
):

    return kafka_df.select(

        col("key")
        .cast("string")
        .alias("message_key"),

        col("value")
        .cast("string")
        .alias("message_value"),

        col("topic")
        .alias("kafka_topic"),

        col("partition")
        .alias("kafka_partition"),

        col("offset")
        .alias("kafka_offset"),

        col("timestamp")
        .alias("kafka_timestamp"),
    )


# =========================================================
# Parse
# =========================================================

def parse_events(
    kafka_df,
):

    return (
        kafka_df
        .withColumn(
            "parsed_event",
            from_json(
                col("message_value"),
                GITHUB_EVENT_SCHEMA,
            ),
        )
        .select(
            "message_key",
            "message_value",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",

            col(
                "parsed_event.event_id"
            ).alias(
                "event_id"
            ),

            col(
                "parsed_event.event_type"
            ).alias(
                "event_type"
            ),

            col(
                "parsed_event.actor_id"
            ).alias(
                "actor_id"
            ),

            col(
                "parsed_event.actor_login"
            ).alias(
                "actor_login"
            ),

            col(
                "parsed_event.repo_id"
            ).alias(
                "repo_id"
            ),

            col(
                "parsed_event.repo_name"
            ).alias(
                "repo_name"
            ),

            col(
                "parsed_event.is_public"
            ).alias(
                "is_public"
            ),

            col(
                "parsed_event.created_at"
            ).alias(
                "created_at"
            ),
        )
        .withColumn(
            "event_time",
            to_timestamp(
                col("created_at")
            ),
        )
    )


# =========================================================
# Validation
# =========================================================

def validate_events(
    parsed_df,
):

    validated_df = (
        parsed_df
        .withColumn(
            "error_reason",

            when(
                col("event_id").isNull()
                | (col("event_id") == ""),
                lit(
                    "missing_required_field:event_id"
                ),
            )

            .when(
                col("event_type").isNull()
                | (col("event_type") == ""),
                lit(
                    "missing_required_field:event_type"
                ),
            )

            .when(
                col("repo_name").isNull()
                | (col("repo_name") == ""),
                lit(
                    "missing_required_field:repo_name"
                ),
            )

            .when(
                col("event_time").isNull(),
                lit(
                    "invalid_required_field:event_time"
                ),
            )

            .otherwise(
                lit(None)
            ),
        )
    )

    valid_df = (
        validated_df
        .filter(
            col("error_reason").isNull()
        )
        .drop(
            "error_reason"
        )
    )

    invalid_df = (
        validated_df
        .filter(
            col("error_reason").isNotNull()
        )
    )

    return (
        valid_df,
        invalid_df,
    )


# =========================================================
# Deduplication
# =========================================================

def deduplicate_events(
    valid_df,
):

    return (
        valid_df
        .withWatermark(
            "event_time",
            "10 minutes",
        )
        .dropDuplicatesWithinWatermark(
            ["event_id"]
        )
    )


# =========================================================
# Mart
# =========================================================

def create_window_aggregation(
    deduplicated_df,
):

    return (
        deduplicated_df
        .groupBy(
            window(
                col("event_time"),
                "5 minutes",
            ),
            col("repo_name"),
            col("event_type"),
        )
        .count()
        .select(
            col(
                "window.start"
            ).alias(
                "window_start"
            ),

            col(
                "window.end"
            ).alias(
                "window_end"
            ),

            col("repo_name"),

            col("event_type"),

            col("count").alias(
                "event_count"
            ),
        )
    )


# =========================================================
# DLQ
# =========================================================

def create_dlq_dataframe(
    invalid_df,
):

    return (
        invalid_df
        .select(
            col("event_id"),

            col("error_reason"),

            col(
                "kafka_topic"
            ).alias(
                "source_topic"
            ),

            col(
                "kafka_partition"
            ).alias(
                "source_partition"
            ),

            col(
                "kafka_offset"
            ).alias(
                "source_offset"
            ),

            col(
                "kafka_timestamp"
            ).alias(
                "source_kafka_timestamp"
            ),

            current_timestamp().alias(
                "failed_at"
            ),

            col(
                "message_value"
            ).alias(
                "original_message"
            ),
        )
    )


# =========================================================
# PostgreSQL
# =========================================================

def get_postgres_connection():

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )


# =========================================================
# Raw Sink
# =========================================================

def write_raw_batch(
    batch_df,
    batch_id,
):

    rows = batch_df.collect()

    if not rows:
        return

    connection = None

    try:

        connection = (
            get_postgres_connection()
        )

        cursor = (
            connection.cursor()
        )

        sql = """
        INSERT INTO github_events (
            event_id,
            event_type,
            actor_id,
            actor_login,
            repo_id,
            repo_name,
            is_public,
            event_time,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            kafka_timestamp
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (event_id)
        DO UPDATE SET
            event_type =
                EXCLUDED.event_type,
            actor_id =
                EXCLUDED.actor_id,
            actor_login =
                EXCLUDED.actor_login,
            repo_id =
                EXCLUDED.repo_id,
            repo_name =
                EXCLUDED.repo_name,
            is_public =
                EXCLUDED.is_public,
            event_time =
                EXCLUDED.event_time,
            kafka_topic =
                EXCLUDED.kafka_topic,
            kafka_partition =
                EXCLUDED.kafka_partition,
            kafka_offset =
                EXCLUDED.kafka_offset,
            kafka_timestamp =
                EXCLUDED.kafka_timestamp;
        """

        for row in rows:

            cursor.execute(
                sql,
                (
                    row.event_id,
                    row.event_type,
                    row.actor_id,
                    row.actor_login,
                    row.repo_id,
                    row.repo_name,
                    row.is_public,
                    row.event_time,
                    row.kafka_topic,
                    row.kafka_partition,
                    row.kafka_offset,
                    row.kafka_timestamp,
                ),
            )

        connection.commit()

        print(
            "[RAW SINK SUCCESS] "
            f"batch_id={batch_id} "
            f"rows={len(rows)}"
        )

    except Exception as error:

        if connection is not None:
            connection.rollback()

        print(
            "[RAW SINK FAILED] "
            f"batch_id={batch_id} "
            f"error={error}"
        )

        raise

    finally:

        if connection is not None:
            connection.close()


# =========================================================
# Mart Sink
# =========================================================

def write_mart_batch(
    batch_df,
    batch_id,
):

    rows = batch_df.collect()

    if not rows:
        return

    connection = None

    try:

        connection = (
            get_postgres_connection()
        )

        cursor = (
            connection.cursor()
        )

        sql = """
        INSERT INTO mart_github_activity_5m (
            window_start,
            window_end,
            repo_name,
            event_type,
            event_count,
            updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            NOW()
        )
        ON CONFLICT (
            window_start,
            repo_name,
            event_type
        )
        DO UPDATE SET
            window_end =
                EXCLUDED.window_end,
            event_count =
                EXCLUDED.event_count,
            updated_at =
                NOW();
        """

        for row in rows:

            cursor.execute(
                sql,
                (
                    row.window_start,
                    row.window_end,
                    row.repo_name,
                    row.event_type,
                    row.event_count,
                ),
            )

        connection.commit()

        print(
            "[MART SINK SUCCESS] "
            f"batch_id={batch_id} "
            f"rows={len(rows)}"
        )

    except Exception as error:

        if connection is not None:
            connection.rollback()

        print(
            "[MART SINK FAILED] "
            f"batch_id={batch_id} "
            f"error={error}"
        )

        raise

    finally:

        if connection is not None:
            connection.close()


# =========================================================
# Streaming Queries
# =========================================================

def start_raw_stream(
    deduplicated_df,
):

    return (
        deduplicated_df
        .writeStream
        .queryName(
            "github_raw_postgres"
        )
        .foreachBatch(
            write_raw_batch
        )
        .outputMode(
            "append"
        )
        .option(
            "checkpointLocation",
            (
                f"{CHECKPOINT_BASE}/"
                "github-events-postgres-v2"
            ),
        )
        .start()
    )


def start_mart_stream(
    mart_df,
):

    return (
        mart_df
        .writeStream
        .queryName(
            "github_mart_postgres"
        )
        .foreachBatch(
            write_mart_batch
        )
        .outputMode(
            "update"
        )
        .option(
            "checkpointLocation",
            (
                f"{CHECKPOINT_BASE}/"
                "github-mart-postgres-v2"
            ),
        )
        .start()
    )


def start_dlq_stream(
    dlq_df,
):

    kafka_dlq_df = (
        dlq_df
        .select(
            col("event_id")
            .cast("string")
            .alias("key"),

            to_json(
                struct("*")
            ).alias(
                "value"
            ),
        )
    )

    return (
        kafka_dlq_df
        .writeStream
        .queryName(
            "github_dlq"
        )
        .format(
            "kafka"
        )
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "topic",
            KAFKA_DLQ_TOPIC,
        )
        .option(
            "checkpointLocation",
            (
                f"{CHECKPOINT_BASE}/"
                "github-events-dlq-v5"
            ),
        )
        .start()
    )


# =========================================================
# Monitoring
# =========================================================

def print_query_progress(
    query,
):

    print(
        "\n"
        "========================================"
    )

    print(
        f"[QUERY] {query.name}"
    )

    print(
        f"[ID] {query.id}"
    )

    print(
        f"[ACTIVE] {query.isActive}"
    )

    progress = (
        query.lastProgress
    )

    if not progress:

        print(
            "[PROGRESS] waiting..."
        )

        return

    print(
        "[BATCH ID] "
        f"{progress.get('batchId')}"
    )

    print(
        "[INPUT ROWS] "
        f"{progress.get('numInputRows')}"
    )

    print(
        "[INPUT ROWS / SEC] "
        f"{progress.get('inputRowsPerSecond')}"
    )

    print(
        "[PROCESSED ROWS / SEC] "
        f"{progress.get('processedRowsPerSecond')}"
    )

    event_time = (
        progress.get(
            "eventTime",
            {},
        )
    )

    if event_time:

        print(
            "[EVENT TIME] "
            f"{event_time}"
        )

    sources = (
        progress.get(
            "sources",
            [],
        )
    )

    for source in sources:

        print(
            "[SOURCE] "
            f"startOffset="
            f"{source.get('startOffset')}"
        )

        print(
            "[SOURCE] "
            f"endOffset="
            f"{source.get('endOffset')}"
        )

        print(
            "[SOURCE] "
            f"numInputRows="
            f"{source.get('numInputRows')}"
        )


def monitor_queries(
    queries,
):

    while True:

        for query in queries:

            print_query_progress(
                query
            )

            if not query.isActive:

                exception = (
                    query.exception()
                )

                raise RuntimeError(
                    "Streaming Query failed: "
                    f"{query.name} "
                    f"{exception}"
                )

        print(
            "\n"
            "[MONITOR] "
            "next check in 30 seconds"
        )

        time.sleep(
            30
        )


# =========================================================
# Main
# =========================================================

def main():

    spark = (
        create_spark_session()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    kafka_df = (
        read_kafka_stream(
            spark
        )
    )

    prepared_df = (
        prepare_kafka_data(
            kafka_df
        )
    )

    parsed_df = (
        parse_events(
            prepared_df
        )
    )

    (
        valid_df,
        invalid_df,
    ) = validate_events(
        parsed_df
    )

    deduplicated_df = (
        deduplicate_events(
            valid_df
        )
    )

    mart_df = (
        create_window_aggregation(
            deduplicated_df
        )
    )

    dlq_df = (
        create_dlq_dataframe(
            invalid_df
        )
    )

    raw_query = (
        start_raw_stream(
            deduplicated_df
        )
    )

    mart_query = (
        start_mart_stream(
            mart_df
        )
    )

    dlq_query = (
        start_dlq_stream(
            dlq_df
        )
    )

    queries = [
        raw_query,
        mart_query,
        dlq_query,
    ]

    monitor_queries(
        queries
    )


if __name__ == "__main__":
    main()