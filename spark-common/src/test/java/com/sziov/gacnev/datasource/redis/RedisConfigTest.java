package com.sziov.gacnev.datasource.redis;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RedisConfig 配置测试")
class RedisConfigTest {

    @Test
    @DisplayName("fromProps_默认值_返回默认配置")
    void fromProps_defaultValues_returnsDefaults() {
        Properties props = new Properties();
        RedisConfig cfg = RedisConfig.fromProps(props);
        assertThat(cfg.getHost()).isEqualTo("localhost");
        assertThat(cfg.getPort()).isEqualTo(6379);
    }

    @Test
    @DisplayName("fromProps_自定义值_返回自定义配置")
    void fromProps_customValues_returnsCustomConfig() {
        Properties props = new Properties();
        props.setProperty("datasource.redis.host", "redis1");
        props.setProperty("datasource.redis.port", "6380");
        props.setProperty("datasource.redis.auth", "secret");
        props.setProperty("datasource.redis.db", "1");
        RedisConfig cfg = RedisConfig.fromProps(props);
        assertThat(cfg.getHost()).isEqualTo("redis1");
        assertThat(cfg.getPort()).isEqualTo(6380);
        assertThat(cfg.getAuth()).isEqualTo("secret");
        assertThat(cfg.getDb()).isEqualTo(1);
    }
}
