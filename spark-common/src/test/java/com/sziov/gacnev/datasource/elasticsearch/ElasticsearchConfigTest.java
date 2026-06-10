package com.sziov.gacnev.datasource.elasticsearch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ElasticsearchConfig 配置测试")
class ElasticsearchConfigTest {

    @Test
    @DisplayName("new_默认构造_返回默认值")
    void new_default_returnsDefaults() {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        assertThat(cfg.getHosts()).isEqualTo("localhost");
        assertThat(cfg.getPort()).isEqualTo(9200);
    }

    @Test
    @DisplayName("toSparkOptions_默认配置_包含必要key")
    void toSparkOptions_default_containsRequiredKeys() {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        assertThat(cfg.toSparkOptions()).containsKey("es.nodes");
    }
}
