package com.sziov.gacnev.datasource.elasticsearch;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Elasticsearch 数据写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ElasticsearchSink implements DataSink {

    private final ElasticsearchConfig config;

    public ElasticsearchSink(DataSourceConfig config) {
        this.config = (ElasticsearchConfig) config;
    }

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        Map<String, String> esOptions = new HashMap<>();
        esOptions.put("es.nodes", config.getHosts());
        esOptions.put("es.port", String.valueOf(config.getPort()));
        esOptions.put("es.index.auto.create", String.valueOf(config.isIndexAutoCreate()));
        esOptions.put("es.nodes.wan.only", "true");
        esOptions.put("es.batch.size.entries", String.valueOf(config.getBatchSize()));

        String writeMode = "append".equalsIgnoreCase(options.getWriteMode()) ? "append" : "overwrite";

        RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("写入 ES，索引: {}，模式: {}", options.getResource(), writeMode);
            df.write().format("org.elasticsearch.spark.sql")
                    .options(esOptions)
                    .mode(writeMode.equals("overwrite") ? "overwrite" : "append")
                    .save(options.getResource());
            return null;
        });
    }
}
