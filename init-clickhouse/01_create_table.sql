CREATE TABLE IF NOT EXISTS analytics.enriched_events
(
  event_id            UInt64,
  content_id          String,
  user_id             String,
  event_type          LowCardinality(String),
  event_ts            DateTime64(3, 'UTC'),
  device              LowCardinality(String),
  duration_ms         Nullable(UInt32),
  content_title       String,
  content_type        LowCardinality(String),
  length_seconds      Nullable(UInt32),
  engagement_seconds  Nullable(Float64),
  engagement_pct      Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (event_ts, content_id);
