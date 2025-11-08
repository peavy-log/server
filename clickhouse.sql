create database peavy;

create table peavy.events (
    timestamp DateTime('UTC') CODEC (DoubleDelta, ZSTD),

    type LowCardinality(String) CODEC (Delta(8), ZSTD),
    category LowCardinality(String) CODEC (ZSTD),
    name LowCardinality(String) CODEC (ZSTD),
    ident String CODEC (ZSTD),
    duration UInt32 CODEC (T64, ZSTD),
    result LowCardinality(String) CODEC (Delta(8), ZSTD),

    platform LowCardinality(String) CODEC (Delta(8), ZSTD),
    app_id LowCardinality(String) CODEC (Delta(8), ZSTD),
    app_version_code UInt64 CODEC (T64, ZSTD),
    session_id String CODEC (ZSTD),
    labels Map(String, String) CODEC (ZSTD)
)
ENGINE = MergeTree
ORDER BY (toStartOfHour(timestamp), type, category, name, app_id)
PARTITION BY (toYYYYMM(timestamp), type);
