package com.sziov.gacnev.datasource.clickhouse;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * ClickHouse 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class ClickHouseConfig {
    private String jdbcUrl;
    private String username = "default";
    private String password = "";
    private int batchSize = 10000;
    private int maxRetries = 3;

    public Map<String, String> toSparkOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("user", username);
        opts.put("password", password);
        return opts;
    }
}
