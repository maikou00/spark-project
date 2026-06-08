package com.sziov.gacnev.datasource.clickhouse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {ClickHouseConfig} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("ClickHouseConfig 配置测试")
class ClickHouseConfigTest {

    @Test
    @DisplayName("fromProps_默认值_返回默认配置")
    void fromProps_defaultValues_returnsDefaults() {
        Properties props = new Properties();
        ClickHouseConfig cfg = ClickHouseConfig.fromProps(props);
        assertThat(cfg.getHosts()).isEqualTo("localhost:8123");
        assertThat(cfg.getUsername()).isEqualTo("default");
        assertThat(cfg.getPassword()).isEmpty();
        assertThat(cfg.getBatchSize()).isEqualTo(10000);
    }

    @Test
    @DisplayName("fromProps_自定义值_返回自定义配置")
    void fromProps_customValues_returnsCustomConfig() {
        Properties props = new Properties();
        props.setProperty("datasource.ck.hosts", "ck1:8123");
        props.setProperty("datasource.ck.username", "admin");
        props.setProperty("datasource.ck.password", "secret");
        props.setProperty("datasource.ck.batch.size", "5000");

        ClickHouseConfig cfg = ClickHouseConfig.fromProps(props);
        assertThat(cfg.getHosts()).isEqualTo("ck1:8123");
        assertThat(cfg.getUsername()).isEqualTo("admin");
        assertThat(cfg.getPassword()).isEqualTo("secret");
        assertThat(cfg.getBatchSize()).isEqualTo(5000);
    }
}
