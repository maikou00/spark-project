package com.sziov.gacnev.datasource.kafka;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class KafkaConfig {
    private String bootstrapServers = "localhost:9092";
    private String groupId = "spark-datasource-group";
    private String startingOffsets = "latest";
    private int maxRetries = 3;

    public Map<String, String> toSparkOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("kafka.bootstrap.servers", bootstrapServers);
        if (groupId != null) {
            opts.put("group.id", groupId);
        }
        if (startingOffsets != null) {
            opts.put("startingOffsets", startingOffsets);
        }
        return opts;
    }
}
