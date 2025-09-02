import json, os, time
from kafka import KafkaConsumer
import clickhouse_connect

KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC = os.getenv('KAFKA_TOPIC', 'enriched.clickhouse')
GROUP = os.getenv('KAFKA_GROUP', 'ch-sink')

CH_HOST = os.getenv('CH_HOST', 'clickhouse')
CH_PORT = int(os.getenv('CH_PORT', '8123'))
CH_DB   = os.getenv('CH_DB', 'analytics')
CH_TBL  = os.getenv('CH_TABLE', 'enriched_events')
CH_USER = os.getenv('CH_USER', 'app')
CH_PASSWORD = os.getenv('CH_PASSWORD', 'app123')

# --- CORRECTED LINE ---
# Pass the username and password to the client
client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD)

client.command(f'CREATE DATABASE IF NOT EXISTS {CH_DB}')
client.command(f'''
CREATE TABLE IF NOT EXISTS {CH_DB}.{CH_TBL} (
  event_id        UInt64,
  content_id      String,
  user_id         String,
  event_type      LowCardinality(String),
  event_ts        DateTime64(3, 'UTC'),
  device          LowCardinality(String),
  duration_ms     Nullable(UInt32),
  content_title   String,
  content_type    LowCardinality(String),
  length_seconds  Nullable(UInt32),
  engagement_seconds Nullable(Float64),
  engagement_pct     Nullable(Float64)
) ENGINE=MergeTree ORDER BY (event_ts, content_id);
''')

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA,
    group_id=GROUP,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8') if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

def row(v):
    # tolerant extraction; defaults for nullables
    def g(k, default=None): return v.get(k, default)
    def as_dt(s):
        # ClickHouse will parse 'YYYY-MM-DDTHH:MM:SS.sssZ' as DateTime64 if passed as string literal
        return s if s else None

    return (
        int(g('event_id', 0)),
        str(g('content_id', '')),
        str(g('user_id', '')),
        str(g('event_type', '')),
        as_dt(g('event_ts', None)),
        str(g('device', '')),
        g('duration_ms', None),
        str(g('content_title', '')),
        str(g('content_type', '')),
        g('length_seconds', None),
        g('engagement_seconds', None),
        g('engagement_pct', None),
    )

batch, BATCH = [], 500
for msg in consumer:
    v = msg.value
    try:
        batch.append(row(v))
        if len(batch) >= BATCH:
            client.insert(f'{CH_DB}.{CH_TBL}', batch, column_names=[
                'event_id','content_id','user_id','event_type','event_ts','device',
                'duration_ms','content_title','content_type','length_seconds',
                'engagement_seconds','engagement_pct'
            ])
            batch.clear()
    except Exception as e:
        print('CH insert error:', e)

# flush on shutdown
if batch:
    client.insert(f'{CH_DB}.{CH_TBL}', batch, column_names=[
        'event_id','content_id','user_id','event_type','event_ts','device',
        'duration_ms','content_title','content_type','length_seconds',
        'engagement_seconds','engagement_pct'
    ])