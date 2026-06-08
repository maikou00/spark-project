package com.sziov.gacnev.datasource.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MongoConfig 配置测试")
class MongoConfigTest {

    @Test
    @DisplayName("fromProps_默认值_返回默认配置")
    void fromProps_defaultValues_returnsDefaults() {
        Properties props = new Properties();
        MongoConfig cfg = MongoConfig.fromProps(props);
        assertThat(cfg.getUri()).isEqualTo("mongodb://localhost:27017");
        assertThat(cfg.getDatabase()).isEmpty();
    }

    @Test
    @DisplayName("fromProps_自定义值_返回自定义配置")
    void fromProps_customValues_returnsCustomConfig() {
        Properties props = new Properties();
        props.setProperty("datasource.mongo.uri", "mongodb://mongo1:27017");
        props.setProperty("datasource.mongo.database", "testdb");
        MongoConfig cfg = MongoConfig.fromProps(props);
        assertThat(cfg.getUri()).isEqualTo("mongodb://mongo1:27017");
        assertThat(cfg.getDatabase()).isEqualTo("testdb");
    }
}
