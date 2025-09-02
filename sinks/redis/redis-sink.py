import os
import json
import redis
import uuid
from kafka import KafkaConsumer

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").split(",")
redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

consumer = KafkaConsumer(
    "enriched.redis",
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: x.decode("utf-8") if x else None,
    group_id="redis-sink-group-1"
)

print("🚀 Redis sink started... waiting for messages")

for message in consumer:
    event = message.value

    key = f"event:{message.offset}"

    r.setex(key, 600, json.dumps(event)) # TTL = 600 ثانية (10 دقائق)

    print(f"✅ Stored in Redis: {key} -> {event}")
