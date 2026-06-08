package com.sziov.gacnev.datasource.kafka;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.ReadOptions;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Kafka 数据读取（基于 Spark 内置 spark-sql-kafka）。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class KafkaSource implements DataSource {
    private final KafkaConfig config;

    public KafkaSource(DataSourceConfig config) {
        this.config = (KafkaConfig) config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        return RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("从 Kafka 读取数据（批），topic: {}", options.getResource());
            return spark.read().format("kafka")
                    .option("kafka.bootstrap.servers", config.getBootstrapServers())
                    .option("subscribe", options.getResource())
                    .option("startingOffsets", "earliest")
                    .option("endingOffsets", "latest")
                    .load();
        });
    }

    @Override
    public Dataset<Row> readStream(SparkSession spark, ReadOptions options) {
        return spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", config.getBootstrapServers())
                .option("subscribe", options.getResource())
                .option("startingOffsets", config.getStartingOffsets())
                .option("group.id", config.getGroupId())
                .load();
    }
}
