package com.sziov.gacnev.datasource.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("KafkaConfig 配置测试")
class KafkaConfigTest {

    @Test
    @DisplayName("fromProps_默认值_返回默认配置")
    void fromProps_defaultValues_returnsDefaults() {
        Properties props = new Properties();
        KafkaConfig cfg = KafkaConfig.fromProps(props);
        assertThat(cfg.getBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(cfg.getGroupId()).isEqualTo("spark-datasource-group");
    }

    @Test
    @DisplayName("fromProps_自定义值_返回自定义配置")
    void fromProps_customValues_returnsCustomConfig() {
        Properties props = new Properties();
        props.setProperty("datasource.kafka.bootstrap.servers", "kafka1:9092");
        props.setProperty("datasource.kafka.group.id", "test.group");
        KafkaConfig cfg = KafkaConfig.fromProps(props);
        assertThat(cfg.getBootstrapServers()).isEqualTo("kafka1:9092");
        assertThat(cfg.getGroupId()).isEqualTo("test.group");
    }
}
