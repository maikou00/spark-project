package com.sziov.gacnev.datasource.mongodb;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.ReadOptions;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * MongoDB 数据读取。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class MongoDBSource implements DataSource {
    private final MongoConfig config;

    public MongoDBSource(DataSourceConfig config) {
        this.config = (MongoConfig) config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        return RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("从 MongoDB 读取数据，集合: {}", options.getResource());
            return spark.read().format("mongodb")
                    .option("spark.mongodb.connection.uri", config.getUri())
                    .option("spark.mongodb.database", config.getDatabase())
                    .option("spark.mongodb.collection", options.getResource())
                    .load();
        });
    }

    @Override
    public Dataset<Row> readStream(SparkSession spark, ReadOptions options) {
        return RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            return spark.readStream().format("mongodb")
                    .option("spark.mongodb.connection.uri", config.getUri())
                    .option("spark.mongodb.database", config.getDatabase())
                    .option("spark.mongodb.collection", options.getResource())
                    .load();
        });
    }
}
