// src/main/java/com/example/streams/EnrichmentApp.java
package com.example.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class EnrichmentApp {

    public static void main(String[] args) {
        // === Properties ===
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                System.getenv().getOrDefault("APPLICATION_ID", "enrichment-app"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));

        // Consume from the beginning on first start
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Exactly-once (safe in dev with single broker when replication factor = 1)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        // === Topic names ===
        final String CONTENT_TOPIC  = System.getenv().getOrDefault("CONTENT_TOPIC", "mydb.public.content");
        final String EVENTS_TOPIC   = System.getenv().getOrDefault("EVENTS_TOPIC", "mydb.public.engagement_events");
        final String OUT_CLICKHOUSE = System.getenv().getOrDefault("ENRICHED_CLICKHOUSE_TOPIC", "enriched.clickhouse");
        final String OUT_REDIS      = System.getenv().getOrDefault("ENRICHED_REDIS_TOPIC", "enriched.redis");
        final String OUT_JSONL      = System.getenv().getOrDefault("ENRICHED_JSONL_TOPIC", "enriched.jsonl");

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // ---------------------------
        // 1) Sources
        // ---------------------------
        // Global table of content keyed by content.id (Debezium SMT already unwrapped + key=id)
        GlobalKTable<String, String> contentTable = builder.globalTable(
                CONTENT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, JsonNode> events = builder.stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(v -> {
                try { return mapper.readTree(v); }
                catch (Exception e) {
                    System.out.println("⚠️ bad JSON in event: " + e.getMessage());
                    return null;
                }
            })
            .filter((k, v) -> {
                if (v == null) return false;
                boolean ok = v.hasNonNull("content_id");
                if (!ok) System.out.println("🔎 No content_id in event; skipping. Value: " + v);
                return ok;
            });

        // ---------------------------
        // 2) Join by foreign key (content_id)
        // ---------------------------
        KStream<String, String> enriched = events.join(
            contentTable,
            // Map each event to the lookup key (content_id string UUID)
            (eventKey, evt) -> evt.get("content_id").asText(),
            // ValueJoiner: combine event + content into a compact analytics record
            (evt, contentJson) -> {
                try {
                    JsonNode content = mapper.readTree(contentJson);

                    Long eventId = evt.hasNonNull("id") ? evt.get("id").asLong() : null;
                    String contentId = evt.get("content_id").asText();
                    String userId = evt.hasNonNull("user_id") ? evt.get("user_id").asText() : null;
                    String eventType = evt.hasNonNull("event_type") ? evt.get("event_type").asText() : null;
                    String eventTs = evt.hasNonNull("event_ts") ? evt.get("event_ts").asText() : null;
                    Long durationMs = (evt.hasNonNull("duration_ms") && !evt.get("duration_ms").isNull())
                            ? evt.get("duration_ms").asLong() : null;
                    String device = evt.hasNonNull("device") ? evt.get("device").asText() : null;

                    String contentType = content.hasNonNull("content_type") ? content.get("content_type").asText() : null;
                    String contentTitle = content.hasNonNull("title") ? content.get("title").asText() : null;
                    Integer lengthSeconds = content.hasNonNull("length_seconds") ? content.get("length_seconds").asInt() : null;

                    Double engagementSeconds = (durationMs != null) ? durationMs / 1000.0 : null;
                    Double engagementPct = (engagementSeconds != null && lengthSeconds != null && lengthSeconds > 0)
                            ? Math.round((engagementSeconds / lengthSeconds) * 10000.0) / 100.0
                            : null;

                    ObjectNode out = mapper.createObjectNode();
                    if (eventId != null) out.put("event_id", eventId);
                    out.put("content_id", contentId);
                    if (userId != null) out.put("user_id", userId);
                    if (eventType != null) out.put("event_type", eventType);
                    if (eventTs != null) out.put("event_ts", eventTs);
                    if (device != null) out.put("device", device);
                    if (durationMs != null) out.put("duration_ms", durationMs);

                    if (contentTitle != null) out.put("content_title", contentTitle);
                    if (contentType != null) out.put("content_type", contentType);
                    if (lengthSeconds != null) out.put("length_seconds", lengthSeconds);

                    if (engagementSeconds != null) out.put("engagement_seconds", engagementSeconds);
                    if (engagementPct != null) out.put("engagement_pct", engagementPct);

                    return mapper.writeValueAsString(out);
                } catch (Exception e) {
                    System.out.println("🔥 join serialization error: " + e.getMessage());
                    return null;
                }
            }
        ).filter((k, v) -> v != null);

        // ---------------------------
        // 3) Fan-out to sinks
        // ---------------------------
        enriched.to(OUT_CLICKHOUSE, Produced.with(Serdes.String(), Serdes.String())); // ch-sink consumes this
        enriched.to(OUT_REDIS,      Produced.with(Serdes.String(), Serdes.String()));
        enriched.to(OUT_JSONL,      Produced.with(Serdes.String(), Serdes.String()));

        // ---------------------------
        // 4) Start Streams
        // ---------------------------
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setStateListener((newState, oldState) ->
                System.out.println("🌊 Kafka Streams state: " + oldState + " -> " + newState));
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
