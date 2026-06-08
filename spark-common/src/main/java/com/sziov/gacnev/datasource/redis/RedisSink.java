package com.sziov.gacnev.datasource.redis;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import com.sziov.gacnev.common.RetryUtils;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Redis 数据写入（基于 spark-redis connector）。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class RedisSink implements DataSink {
    private final RedisConfig config;

    public RedisSink(DataSourceConfig config) {
        this.config = (RedisConfig) config;
    }

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        String mode = "overwrite".equalsIgnoreCase(options.getWriteMode()) ? "overwrite" : "append";
        RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("写入 Redis，key column: {}，模式: {}", options.getResource(), mode);
            df.write().format("org.apache.spark.sql.redis")
                    .option("host", config.getHost())
                    .option("port", String.valueOf(config.getPort()))
                    .option("auth", config.getAuth())
                    .option("dbNum", String.valueOf(config.getDb()))
                    .option("table", options.getResource())
                    .mode(mode)
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
            log.error("Redis 流式写入启动超时", e);
            throw new RuntimeException("Redis 流式写入启动失败", e);
        }
    }
}
