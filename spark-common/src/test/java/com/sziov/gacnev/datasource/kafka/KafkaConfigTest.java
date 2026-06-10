package com.sziov.gacnev.datasource.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("KafkaConfig 配置测试")
class KafkaConfigTest {

    @Test
    @DisplayName("new_默认构造_返回默认值")
    void new_default_returnsDefaults() {
        KafkaConfig cfg = new KafkaConfig();
        assertThat(cfg.getBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(cfg.getGroupId()).isEqualTo("spark-datasource-group");
        assertThat(cfg.getStartingOffsets()).isEqualTo("latest");
    }

    @Test
    @DisplayName("toSparkOptions_默认配置_包含必要key")
    void toSparkOptions_default_containsRequiredKeys() {
        KafkaConfig cfg = new KafkaConfig();
        assertThat(cfg.toSparkOptions()).containsKey("kafka.bootstrap.servers");
    }
}
