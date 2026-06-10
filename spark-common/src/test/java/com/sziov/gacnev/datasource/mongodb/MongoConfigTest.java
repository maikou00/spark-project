package com.sziov.gacnev.datasource.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MongoConfig 配置测试")
class MongoConfigTest {

    @Test
    @DisplayName("new_默认构造_返回默认值")
    void new_default_returnsDefaults() {
        MongoConfig cfg = new MongoConfig();
        assertThat(cfg.getUri()).isEqualTo("mongodb://localhost:27017");
        assertThat(cfg.getDatabase()).isNull();
    }

    @Test
    @DisplayName("toSparkOptions_默认配置_包含必要key")
    void toSparkOptions_default_containsRequiredKeys() {
        MongoConfig cfg = new MongoConfig();
        assertThat(cfg.toSparkOptions()).containsKey("spark.mongodb.connection.uri");
    }
}
