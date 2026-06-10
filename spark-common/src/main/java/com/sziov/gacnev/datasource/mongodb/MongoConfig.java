package com.sziov.gacnev.datasource.mongodb;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * MongoDB 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class MongoConfig {
    private String uri = "mongodb://localhost:27017";
    private String database;
    private int maxRetries = 3;

    public Map<String, String> toSparkOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("spark.mongodb.connection.uri", uri);
        if (database != null) {
            opts.put("spark.mongodb.database", database);
        }
        return opts;
    }
}
