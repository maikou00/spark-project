package com.sziov.gacnev.datasource.redis;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class RedisConfig {
    private String host = "localhost";
    private int port = 6379;
    private String auth;
    private int db = 0;
    private int maxRetries = 3;

    public Map<String, String> toSparkOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("host", host);
        opts.put("port", String.valueOf(port));
        if (auth != null && !auth.isEmpty()) {
            opts.put("auth", auth);
        }
        opts.put("dbNum", String.valueOf(db));
        return opts;
    }
}
