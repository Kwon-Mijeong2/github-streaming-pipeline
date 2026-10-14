-- =========================================================
-- 1. Raw 전체 건수
-- =========================================================

SELECT
    COUNT(*) AS raw_event_count
FROM github_events;


-- =========================================================
-- 2. Mart 전체 건수
-- =========================================================

SELECT
    COUNT(*) AS mart_row_count
FROM mart_github_activity_5m;


-- =========================================================
-- 3. 최신 Event Time
-- =========================================================

SELECT
    MAX(event_time) AS latest_event_time
FROM github_events;


-- =========================================================
-- 4. Freshness
-- =========================================================

SELECT
    NOW() AS checked_at,
    MAX(event_time) AS latest_event_time,
    NOW() - MAX(event_time) AS freshness_delay
FROM github_events;


-- =========================================================
-- 5. 최신 DB 적재 시간
-- =========================================================

SELECT
    MAX(inserted_at) AS latest_inserted_at
FROM github_events;


-- =========================================================
-- 6. Event Type 분포
-- =========================================================

SELECT
    event_type,
    COUNT(*) AS event_count
FROM github_events
GROUP BY event_type
ORDER BY event_count DESC;


-- =========================================================
-- 7. Repository 활동량
-- =========================================================

SELECT
    repo_name,
    COUNT(*) AS event_count
FROM github_events
GROUP BY repo_name
ORDER BY event_count DESC
LIMIT 20;


-- =========================================================
-- 8. Required Field NULL 검사
-- =========================================================

SELECT
    COUNT(*) AS invalid_required_rows
FROM github_events
WHERE
    event_id IS NULL
    OR event_type IS NULL
    OR repo_name IS NULL
    OR event_time IS NULL;


-- =========================================================
-- 9. event_id 중복
-- =========================================================

SELECT
    event_id,
    COUNT(*) AS duplicate_count
FROM github_events
GROUP BY event_id
HAVING COUNT(*) > 1;


-- =========================================================
-- 10. Kafka Position 중복
-- =========================================================

SELECT
    kafka_topic,
    kafka_partition,
    kafka_offset,
    COUNT(*) AS duplicate_count
FROM github_events
GROUP BY
    kafka_topic,
    kafka_partition,
    kafka_offset
HAVING COUNT(*) > 1;


-- =========================================================
-- 11. Kafka → PostgreSQL Processing Delay
-- =========================================================

SELECT
    AVG(
        inserted_at
        - kafka_timestamp
    ) AS avg_processing_delay,

    MAX(
        inserted_at
        - kafka_timestamp
    ) AS max_processing_delay

FROM github_events
WHERE kafka_timestamp IS NOT NULL;


-- =========================================================
-- 12. Event Time → PostgreSQL Delay
-- =========================================================

SELECT
    AVG(
        inserted_at
        - event_time
    ) AS avg_event_delay,

    MAX(
        inserted_at
        - event_time
    ) AS max_event_delay

FROM github_events;


-- =========================================================
-- 13. Partition별 적재 상태
-- =========================================================

SELECT
    kafka_partition,
    COUNT(*) AS event_count,
    MIN(kafka_offset) AS min_offset,
    MAX(kafka_offset) AS max_offset
FROM github_events
GROUP BY kafka_partition
ORDER BY kafka_partition;


-- =========================================================
-- 14. 최근 Event
-- =========================================================

SELECT
    event_id,
    event_type,
    actor_login,
    repo_name,
    event_time,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    inserted_at
FROM github_events
ORDER BY inserted_at DESC
LIMIT 20;


-- =========================================================
-- 15. Mart 최신 Window
-- =========================================================

SELECT
    window_start,
    window_end,
    repo_name,
    event_type,
    event_count,
    updated_at
FROM mart_github_activity_5m
ORDER BY window_start DESC
LIMIT 30;


-- =========================================================
-- 16. Mart Grain 중복
-- =========================================================

SELECT
    window_start,
    repo_name,
    event_type,
    COUNT(*) AS duplicate_count
FROM mart_github_activity_5m
GROUP BY
    window_start,
    repo_name,
    event_type
HAVING COUNT(*) > 1;


-- =========================================================
-- 17. 최근 10분 적재량
-- =========================================================

SELECT
    COUNT(*) AS events_last_10_minutes
FROM github_events
WHERE inserted_at >=
    NOW() - INTERVAL '10 minutes';


-- =========================================================
-- 18. 최근 1분 적재량
-- =========================================================

SELECT
    COUNT(*) AS events_last_1_minute
FROM github_events
WHERE inserted_at >=
    NOW() - INTERVAL '1 minute';


-- =========================================================
-- 19. 최근 5분 Event Type
-- =========================================================

SELECT
    event_type,
    COUNT(*) AS event_count
FROM github_events
WHERE inserted_at >=
    NOW() - INTERVAL '5 minutes'
GROUP BY event_type
ORDER BY event_count DESC;


-- =========================================================
-- 20. Health Summary
-- =========================================================

SELECT
    COUNT(*) AS total_events,

    COUNT(
        DISTINCT event_id
    ) AS unique_events,

    MAX(event_time)
        AS latest_event_time,

    MAX(inserted_at)
        AS latest_inserted_at,

    NOW() - MAX(event_time)
        AS freshness_delay

FROM github_events;