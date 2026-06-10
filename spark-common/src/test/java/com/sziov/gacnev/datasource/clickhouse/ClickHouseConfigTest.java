package com.sziov.gacnev.datasource.clickhouse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ClickHouseConfig 配置测试")
class ClickHouseConfigTest {

    @Test
    @DisplayName("new_默认构造_返回默认值")
    void new_default_returnsDefaults() {
        ClickHouseConfig cfg = new ClickHouseConfig();
        assertThat(cfg.getUsername()).isEqualTo("default");
        assertThat(cfg.getPassword()).isEmpty();
        assertThat(cfg.getBatchSize()).isEqualTo(10000);
        assertThat(cfg.getMaxRetries()).isEqualTo(3);
    }

    @Test
    @DisplayName("setter_设置值_可正常获取")
    void setter_setValues_canRetrieve() {
        ClickHouseConfig cfg = new ClickHouseConfig();
        cfg.setJdbcUrl("jdbc:clickhouse://host:8123");
        cfg.setUsername("admin");
        assertThat(cfg.getJdbcUrl()).isEqualTo("jdbc:clickhouse://host:8123");
        assertThat(cfg.getUsername()).isEqualTo("admin");
    }
}
