# GitHub Real-Time Developer Activity Streaming Data Pipeline

GitHub Public Events를 실시간으로 수집하고 Kafka와 Spark Structured Streaming을 통해 처리한 뒤 PostgreSQL에 저장하는 Streaming Data Pipeline 프로젝트입니다.

단순한 실시간 데이터 수집을 넘어 다음 문제를 직접 구현하는 것을 목표로 했습니다.

- Streaming Data 수집
- Kafka 기반 Event 전달
- Schema Parsing
- Data Validation
- Event Time Processing
- Window Aggregation
- Watermark
- Deduplication
- Checkpoint Recovery
- PostgreSQL Idempotent Sink
- DLQ
- Retry / Error Handling
- Monitoring
- 장애 복구

---

# 1. Architecture

```text
GitHub Events API
        │
        ▼
Python Producer
        │
        │ Retry
        ▼
Apache Kafka
github-events
        │
        ▼
Spark Structured Streaming
        │
        ├── Schema Parsing
        │
        ├── Validation
        │
        ├── Event Time
        │
        ├── Watermark
        │
        ├── Deduplication
        │
        └── Window Aggregation
        │
        ├─────────────────────────────┐
        │                             │
        ▼                             ▼
Valid Events                    Invalid Events
        │                             │
        ▼                             ▼
PostgreSQL                     Kafka DLQ
        │                      github-events-dlq
        │
        ├── github_events
        │
        └── mart_github_activity_5m
```

---

# 2. Tech Stack

| Category | Technology |
|---|---|
| Language | Python |
| Source | GitHub Events API |
| Message Broker | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Database | PostgreSQL |
| Container | Docker / Docker Compose |
| Python DB Driver | psycopg2 |
| Data Format | JSON |
| Recovery | Spark Checkpoint |
| DQ | Validation / Deduplication |
| Error Handling | Retry / DLQ / Transaction Rollback |

---

# 3. Project Structure

```text
github-streaming-pipeline/
│
├── src/
│   ├── producer.py
│   └── event_parser.py
│
├── spark/
│   ├── Dockerfile
│   ├── schemas.py
│   └── stream_processor.py
│
├── sql/
│   ├── init.sql
│   └── monitoring.sql
│
├── checkpoints/
│
├── data/
│   └── .gitkeep
│
├── logs/
│   └── .gitkeep
│
├── .env
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# 4. Event Schema

GitHub Event를 다음 형태로 정규화합니다.

```text
event_id
event_type
actor_id
actor_login
repo_id
repo_name
is_public
created_at
```

Spark 처리 과정에서 다음 Metadata도 유지합니다.

```text
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
```

`created_at`은 Spark에서 `event_time`으로 변환합니다.

---

# 5. Kafka Topics

## Main Topic

```text
github-events
```

정상 처리 대상 GitHub Event가 저장됩니다.

## DLQ

```text
github-events-dlq
```

Validation을 통과하지 못한 Event가 저장됩니다.

---

# 6. Kafka Partition

Main Topic은 3개의 Partition을 사용합니다.

```text
github-events

Partition 0
Partition 1
Partition 2
```

Producer에서는 Repository 이름을 Message Key로 사용합니다.

```python
event_key = (
    event.get("repo_name")
    or event.get("event_id")
)
```

같은 Repository의 Event가 동일 Partition으로 전달될 가능성을 높여 Repository 단위 순서를 관리하기 쉽도록 구성했습니다.

---

# 7. Streaming Processing

전체 처리 흐름은 다음과 같습니다.

```text
Kafka
↓
JSON
↓
Explicit Schema
↓
Validation
↓
Event Time
↓
Watermark
↓
Deduplication
↓
Window Aggregation
↓
PostgreSQL
```

---

# 8. Data Validation

필수 필드:

```text
event_id
event_type
repo_name
event_time
```

Validation 실패 예:

```text
missing_required_field:event_id

missing_required_field:event_type

missing_required_field:repo_name

invalid_required_field:event_time
```

Invalid Event는 정상 Sink로 전달하지 않고 DLQ로 보냅니다.

---

# 9. DLQ

DLQ에는 다음 정보를 저장합니다.

```text
event_id
error_reason

source_topic
source_partition
source_offset
source_kafka_timestamp

failed_at

original_message
```

이를 통해 실패 데이터의 원본과 Kafka 위치를 추적할 수 있습니다.

---

# 10. Event Time

분석 기준 시간은 GitHub Event의:

```text
created_at
```

입니다.

Spark에서는:

```text
created_at
↓
event_time
```

으로 변환합니다.

다음 세 시간을 구분합니다.

```text
Event Time
Kafka Timestamp
Processing Time
```

---

# 11. Window

5분 Tumbling Window를 사용합니다.

```text
12:00 ~ 12:05
12:05 ~ 12:10
12:10 ~ 12:15
```

Mart Grain:

```text
window_start
+
repo_name
+
event_type
```

---

# 12. Watermark

Watermark:

```text
10 minutes
```

을 사용합니다.

목적:

```text
Late Event 처리
+
State 관리
```

Watermark는 단순한:

```text
현재 시스템 시간 - 10분
```

이 아니라 Stream에서 관찰되는 Event Time 진행을 기반으로 관리됩니다.

---

# 13. Deduplication

Business Event Identity:

```text
event_id
```

를 사용합니다.

```python
.withWatermark(
    "event_time",
    "10 minutes",
)
.dropDuplicatesWithinWatermark(
    ["event_id"]
)
```

Kafka Record Identity:

```text
topic
+
partition
+
offset
```

와 GitHub Business Event Identity:

```text
event_id
```

를 분리해서 관리합니다.

---

# 14. Checkpoint

Spark Checkpoint:

```text
checkpoints/

github-events-postgres-v2

github-mart-postgres-v2

github-events-dlq-v5
```

Checkpoint를 이용해 Streaming Query의 처리 진행 상태와 Stateful Processing 복구를 지원합니다.

`startingOffsets=latest`는 기존 Checkpoint가 없는 최초 실행에 주로 영향을 줍니다.

기존 Checkpoint가 있다면 해당 처리 상태를 기반으로 재시작합니다.

---

# 15. PostgreSQL

## Raw

```text
github_events
```

Grain:

```text
GitHub Event 1건
```

Primary Key:

```text
event_id
```

추가 Unique Constraint:

```text
kafka_topic
+
kafka_partition
+
kafka_offset
```

---

# 16. Mart

```text
mart_github_activity_5m
```

Grain:

```text
window_start
+
repo_name
+
event_type
```

저장 값:

```text
window_start
window_end
repo_name
event_type
event_count
updated_at
```

---

# 17. Idempotent Sink

Raw Sink:

```sql
ON CONFLICT (event_id)
DO UPDATE
```

Mart Sink:

```sql
ON CONFLICT (
    window_start,
    repo_name,
    event_type
)
DO UPDATE
```

Mart는 Spark `update` mode에서 현재 집계 상태를 전달하므로 기존 Count에 더하지 않고 최신 `event_count` 값으로 갱신합니다.

---

# 18. Failure Handling

오류를 다음과 같이 분리했습니다.

```text
Transient Error
→ Retry

Data Error
→ DLQ

Database Failure
→ Transaction Rollback
→ Query Failure
→ Recovery
→ Same Checkpoint Restart

Configuration / Code Error
→ Fail Fast
```

---

# 19. GitHub API Retry

다음 오류는 Retry 대상으로 처리합니다.

```text
Timeout

Connection Error

HTTP 429

HTTP 5xx
```

Exponential Backoff를 사용합니다.

```text
1 sec
2 sec
4 sec
8 sec
...
```

---

# 20. PostgreSQL Transaction

Micro Batch 저장 시:

```text
BEGIN
↓
INSERT / UPSERT
↓
COMMIT
```

실패 시:

```text
ROLLBACK
↓
Exception
↓
Streaming Query Failure
```

방식으로 처리합니다.

---

# 21. Exactly-Once에 대한 접근

이 프로젝트를 완벽한 End-to-End Exactly-Once Pipeline이라고 정의하지 않습니다.

대신 다음 장치를 조합해 재처리 시 중복 영향을 최소화했습니다.

```text
Kafka Event Log

+

Spark Checkpoint

+

event_id Deduplication

+

PostgreSQL Unique Constraints

+

Transaction

+

Idempotent UPSERT
```

---

# 22. Monitoring

Spark에서 다음 정보를 확인합니다.

```text
Query Name
Query Active Status

Batch ID

Input Rows

Input Rows / Sec

Processed Rows / Sec

Event Time

Watermark

Kafka Start Offset

Kafka End Offset
```

PostgreSQL에서는 다음 항목을 확인합니다.

```text
Raw Count

Mart Count

Latest Event Time

Freshness

Processing Delay

Required NULL

event_id Duplicate

Kafka Position Duplicate

Partition Distribution

Recent Ingestion
```

---

# 23. Environment

`.env` 예:

```env
GITHUB_EVENTS_URL=https://api.github.com/events
GITHUB_POLL_INTERVAL=10
GITHUB_TOKEN=

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=github-events
KAFKA_DLQ_TOPIC=github-events-dlq

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=github_streaming
POSTGRES_USER=github_user
POSTGRES_PASSWORD=github_password
```

실제 Secret 값이 포함된 `.env`는 Git에 Commit하지 않습니다.

---

# 24. Installation

Python 가상환경 생성:

```powershell
python -m venv venv
```

활성화:

```powershell
.\venv\Scripts\Activate.ps1
```

Dependency:

```powershell
pip install -r requirements.txt
```

---

# 25. Docker 실행

```powershell
docker compose up -d
```

상태 확인:

```powershell
docker compose ps
```

---

# 26. Kafka Topic 확인

```powershell
docker exec -it github_streaming_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

확인:

```text
github-events
github-events-dlq
```

---

# 27. Spark 실행

```powershell
docker exec -it github_streaming_spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /opt/spark/work-dir/spark/stream_processor.py
```

---

# 28. Producer 실행

새 PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

```powershell
python .\src\producer.py
```

흐름:

```text
GitHub
↓
Producer
↓
Kafka
↓
Spark
↓
PostgreSQL
```

---

# 29. PostgreSQL 확인

```powershell
docker exec -it github_streaming_postgres psql -U github_user -d github_streaming
```

Raw:

```sql
SELECT
    event_id,
    event_type,
    actor_login,
    repo_name,
    event_time
FROM github_events
ORDER BY inserted_at DESC
LIMIT 20;
```

Mart:

```sql
SELECT
    window_start,
    window_end,
    repo_name,
    event_type,
    event_count
FROM mart_github_activity_5m
ORDER BY window_start DESC
LIMIT 20;
```

---

# 30. Monitoring

```powershell
Get-Content .\sql\monitoring.sql | docker exec -i github_streaming_postgres psql -U github_user -d github_streaming
```

---

# 31. End-to-End Tests

## Test 1 — Normal Event

```text
Kafka
↓
Spark
↓
PostgreSQL
```

Expected:

```text
DB 1 row
```

## Test 2 — Duplicate Event

```text
Same event_id × 3
```

Expected:

```text
DB 1 row
```

## Test 3 — Invalid Event

```text
repo_name missing
```

Expected:

```text
DLQ
DB 0 row
```

## Test 4 — Malformed JSON

Expected:

```text
DLQ
```

## Test 5 — Spark Failure

```text
Spark Stop
↓
Kafka Event
↓
Spark Restart
↓
Same Checkpoint
↓
Event Processing
```

## Test 6 — PostgreSQL Failure

```text
PostgreSQL Stop
↓
Event
↓
Sink Failure
↓
PostgreSQL Recovery
↓
Spark Restart
↓
Same Checkpoint
↓
Event Stored
```

---

# 32. Monitoring Queries

Raw Count:

```sql
SELECT COUNT(*)
FROM github_events;
```

Duplicate:

```sql
SELECT
    event_id,
    COUNT(*)
FROM github_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

Required NULL:

```sql
SELECT COUNT(*)
FROM github_events
WHERE
    event_id IS NULL
    OR event_type IS NULL
    OR repo_name IS NULL
    OR event_time IS NULL;
```

Freshness:

```sql
SELECT
    NOW(),
    MAX(event_time),
    NOW() - MAX(event_time)
FROM github_events;
```

---

# 33. Key Engineering Points

이 프로젝트에서 구현한 핵심은 다음과 같습니다.

```text
Kafka Partition / Key

Kafka Offset

Structured Streaming

Explicit Schema

Validation

Event Time

Window

Watermark

Late Event

Stateful Processing

Deduplication

Checkpoint

Idempotency

PostgreSQL UPSERT

Transaction

DLQ

Retry

Failure Recovery

Monitoring
```

---

# 34. Limitations

현재 프로젝트는 로컬 포트폴리오 환경을 기준으로 구현했습니다.

현재 PostgreSQL Sink는:

```python
batch_df.collect()
```

방식으로 Driver에 Micro Batch 데이터를 수집합니다.

대규모 Production 환경에서는 다음 방식으로 개선할 수 있습니다.

```text
foreachPartition

JDBC Batch

Bulk Insert

Staging Table

Distributed Sink
```

또한 현재 Kafka는 Single Broker 환경입니다.

Production 환경에서는 Multi-Broker, Replication, Schema Registry, Metrics/Alerting 등의 추가 구성이 필요합니다.

---

# 35. Future Improvements

```text
Kafka Multi-Broker

Schema Registry

Prometheus

Grafana

Alerting

JDBC Batch Sink

DLQ Replay Pipeline

Cloud Deployment

Object Storage

Spark Cluster

CI/CD
```

---

# 36. What I Learned

이 프로젝트를 통해 Batch Pipeline과 Streaming Pipeline의 차이를 직접 구현하며 학습했습니다.

특히 다음 문제를 중점적으로 다뤘습니다.

```text
실시간 Event를 어떻게 전달하는가?

중복 Event를 어떻게 제거하는가?

늦게 도착한 Event를 어떻게 처리하는가?

Streaming State를 어떻게 관리하는가?

Processor 장애 후 어디서부터 다시 처리하는가?

Sink 장애 시 중복 없이 어떻게 재처리하는가?

잘못된 데이터와 시스템 장애를 어떻게 구분하는가?

Pipeline이 정상 동작하는지 무엇을 Monitoring해야 하는가?
```

단순히 데이터를 실시간으로 전달하는 것보다 장애와 재처리를 고려한 Streaming Pipeline 설계가 중요하다는 점을 확인했습니다.