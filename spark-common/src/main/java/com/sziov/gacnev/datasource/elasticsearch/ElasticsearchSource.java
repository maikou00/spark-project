package com.sziov.gacnev.datasource.elasticsearch;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.ReadOptions;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Elasticsearch 数据读取。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ElasticsearchSource implements DataSource {

    private final ElasticsearchConfig config;

    public ElasticsearchSource(DataSourceConfig config) {
        this.config = (ElasticsearchConfig) config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        Map<String, String> esOptions = new HashMap<>();
        esOptions.put("es.nodes", config.getHosts());
        esOptions.put("es.port", String.valueOf(config.getPort()));
        esOptions.put("es.index.auto.create", String.valueOf(config.isIndexAutoCreate()));
        esOptions.put("es.nodes.wan.only", "true");

        if (options.getQuery() != null && !options.getQuery().isEmpty()) {
            esOptions.put("es.query", options.getQuery());
        }

        return RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("从 ES 读取数据，索引: {}", options.getResource());
            return spark.read().format("org.elasticsearch.spark.sql")
                    .options(esOptions)
                    .load(options.getResource());
        });
    }
}
