package com.sziov.gacnev.datasource.elasticsearch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ElasticsearchConfig 配置测试")
class ElasticsearchConfigTest {

    @Test
    @DisplayName("fromProps_默认值_返回默认配置")
    void fromProps_defaultValues_returnsDefaults() {
        Properties props = new Properties();
        ElasticsearchConfig cfg = ElasticsearchConfig.fromProps(props);
        assertThat(cfg.getHosts()).isEqualTo("localhost");
        assertThat(cfg.getPort()).isEqualTo(9200);
        assertThat(cfg.isIndexAutoCreate()).isTrue();
    }

    @Test
    @DisplayName("fromProps_自定义值_返回自定义配置")
    void fromProps_customValues_returnsCustomConfig() {
        Properties props = new Properties();
        props.setProperty("datasource.es.hosts", "es1:9200");
        props.setProperty("datasource.es.port", "9300");
        props.setProperty("datasource.es.index.auto.create", "false");
        ElasticsearchConfig cfg = ElasticsearchConfig.fromProps(props);
        assertThat(cfg.getHosts()).isEqualTo("es1:9200");
        assertThat(cfg.getPort()).isEqualTo(9300);
        assertThat(cfg.isIndexAutoCreate()).isFalse();
    }
}
