package com.sziov.gacnev.datasource.kafka;

import com.sziov.gacnev.datasource.core.DataSourceConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Kafka 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class KafkaConfig extends DataSourceConfig {
    private String bootstrapServers;
    private String groupId = "spark-datasource-group";
    private String startingOffsets = "latest";

    public static KafkaConfig fromProps(java.util.Properties props) {
        KafkaConfig cfg = new KafkaConfig();
        cfg.setBootstrapServers(props.getProperty("datasource.kafka.bootstrap.servers", "localhost:9092"));
        cfg.setGroupId(props.getProperty("datasource.kafka.group.id", "spark-datasource-group"));
        return cfg;
    }
}
