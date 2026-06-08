package com.sziov.gacnev.datasource.kafka;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import com.sziov.gacnev.common.RetryUtils;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Kafka 数据写入（基于 Spark 内置 spark-sql-kafka）。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class KafkaSink implements DataSink {
    private final KafkaConfig config;

    public KafkaSink(DataSourceConfig config) {
        this.config = (KafkaConfig) config;
    }

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("写入 Kafka（批），topic: {}", options.getResource());
            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .write().format("kafka")
                    .option("kafka.bootstrap.servers", config.getBootstrapServers())
                    .option("topic", options.getResource())
                    .save();
            return null;
        });
    }

    @Override
    public void writeStream(Dataset<Row> df, WriteOptions options) {
        try {
            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .writeStream().format("kafka")
                    .option("kafka.bootstrap.servers", config.getBootstrapServers())
                    .option("topic", options.getResource())
                    .option("checkpointLocation", "/tmp/kafka-checkpoint-" + options.getResource())
                    .start();
        } catch (TimeoutException e) {
            log.error("Kafka 流式写入启动超时", e);
            throw new RuntimeException("Kafka 流式写入启动失败", e);
        }
    }
}
