package com.sziov.gacnev.datasource.elasticsearch;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Elasticsearch 配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Data
public class ElasticsearchConfig {
    private String hosts = "localhost";
    private int port = 9200;
    private boolean indexAutoCreate = true;
    private int batchSize = 1000;
    private int maxRetries = 3;

    public Map<String, String> toSparkOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("es.nodes", hosts);
        opts.put("es.port", String.valueOf(port));
        opts.put("es.index.auto.create", String.valueOf(indexAutoCreate));
        opts.put("es.nodes.wan.only", "true");
        if (batchSize > 0) {
            opts.put("es.batch.size.entries", String.valueOf(batchSize));
        }
        return opts;
    }
}
