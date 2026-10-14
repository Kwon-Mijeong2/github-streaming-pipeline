CREATE TABLE IF NOT EXISTS github_events (
    event_id TEXT PRIMARY KEY,

    event_type TEXT NOT NULL,

    actor_id BIGINT,

    actor_login TEXT,

    repo_id BIGINT,

    repo_name TEXT NOT NULL,

    is_public BOOLEAN,

    event_time TIMESTAMPTZ NOT NULL,

    kafka_topic TEXT NOT NULL,

    kafka_partition INTEGER NOT NULL,

    kafka_offset BIGINT NOT NULL,

    kafka_timestamp TIMESTAMPTZ,

    inserted_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),

    CONSTRAINT uq_github_events_kafka_position
        UNIQUE (
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
);


CREATE INDEX IF NOT EXISTS
    idx_github_events_event_time
ON github_events (
    event_time DESC
);


CREATE INDEX IF NOT EXISTS
    idx_github_events_repo_time
ON github_events (
    repo_name,
    event_time DESC
);


CREATE INDEX IF NOT EXISTS
    idx_github_events_event_type
ON github_events (
    event_type
);


CREATE TABLE IF NOT EXISTS
    mart_github_activity_5m (

    window_start TIMESTAMPTZ
        NOT NULL,

    window_end TIMESTAMPTZ
        NOT NULL,

    repo_name TEXT
        NOT NULL,

    event_type TEXT
        NOT NULL,

    event_count BIGINT
        NOT NULL
        CHECK (
            event_count >= 0
        ),

    updated_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),

    PRIMARY KEY (
        window_start,
        repo_name,
        event_type
    )
);


CREATE INDEX IF NOT EXISTS
    idx_mart_github_activity_repo_window
ON mart_github_activity_5m (
    repo_name,
    window_start DESC
);


CREATE INDEX IF NOT EXISTS
    idx_mart_github_activity_event_type
ON mart_github_activity_5m (
    event_type,
    window_start DESC
);


