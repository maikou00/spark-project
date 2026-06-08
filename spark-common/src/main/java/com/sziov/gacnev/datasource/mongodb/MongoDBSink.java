package com.sziov.gacnev.datasource.mongodb;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.concurrent.TimeoutException;

/**
 * MongoDB 数据写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class MongoDBSink implements DataSink {
    private final MongoConfig config;

    public MongoDBSink(DataSourceConfig config) {
        this.config = (MongoConfig) config;
    }

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        String mode = "overwrite".equalsIgnoreCase(options.getWriteMode()) ? "overwrite" : "append";
        RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("写入 MongoDB，集合: {}，模式: {}", options.getResource(), mode);
            df.write().format("mongodb").mode(mode)
                    .option("spark.mongodb.connection.uri", config.getUri())
                    .option("spark.mongodb.database", config.getDatabase())
                    .option("spark.mongodb.collection", options.getResource())
                    .save();
            return null;
        });
    }

    @Override
    public void writeStream(Dataset<Row> df, WriteOptions options) {
        try {
            df.writeStream().foreachBatch((batchDf, batchId) -> {
                WriteOptions batchOptions = WriteOptions.builder()
                        .resource(options.getResource()).writeMode("append").build();
                write(batchDf, batchOptions);
            }).start();
        } catch (TimeoutException e) {
            log.error("MongoDB 流式写入启动超时", e);
            throw new RuntimeException("MongoDB 流式写入启动失败", e);
        }
    }
}
