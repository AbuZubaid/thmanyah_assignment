import os
import json
from kafka import KafkaConsumer

# إعداد Kafka consumer
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").split(",")
topic = os.getenv("KAFKA_TOPIC", "enriched.jsonl")

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="jsonl-sink-group"
)

output_path = "/data/enriched.jsonl"

print(f"📂 Writing enriched events to {output_path}")

with open(output_path, "a") as f:
    for message in consumer:
        json.dump(message.value, f, ensure_ascii=False)
        f.write("\n")
        f.flush()
        print(f"✅ Written event to file: {message.value}")
