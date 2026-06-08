package com.sziov.gacnev.datasource.redis;

import com.sziov.gacnev.datasource.core.DataSourceConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Redis 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class RedisConfig extends DataSourceConfig {
    private String host = "localhost";
    private int port = 6379;
    private String auth;
    private int db = 0;

    public static RedisConfig fromProps(java.util.Properties props) {
        RedisConfig cfg = new RedisConfig();
        cfg.setHost(props.getProperty("datasource.redis.host", "localhost"));
        cfg.setPort(Integer.parseInt(props.getProperty("datasource.redis.port", "6379")));
        cfg.setAuth(props.getProperty("datasource.redis.auth"));
        cfg.setDb(Integer.parseInt(props.getProperty("datasource.redis.db", "0")));
        return cfg;
    }
}
