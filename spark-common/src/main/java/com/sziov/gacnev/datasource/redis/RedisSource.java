package com.sziov.gacnev.datasource.redis;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.ReadOptions;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Redis 数据读取（基于 spark-redis connector）。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class RedisSource implements DataSource {
    private final RedisConfig config;

    public RedisSource(DataSourceConfig config) {
        this.config = (RedisConfig) config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        return RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("从 Redis 读取数据，key pattern: {}", options.getResource());
            return spark.read().format("org.apache.spark.sql.redis")
                    .option("host", config.getHost())
                    .option("port", String.valueOf(config.getPort()))
                    .option("auth", config.getAuth())
                    .option("dbNum", String.valueOf(config.getDb()))
                    .option("keys.pattern", options.getResource() != null ? options.getResource() : "*")
                    .load();
        });
    }
}
