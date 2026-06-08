package com.sziov.gacnev.datasource.clickhouse;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import com.sziov.gacnev.common.JdbcUtils;
import java.util.concurrent.TimeoutException;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.util.Properties;

/**
 * ClickHouse 数据写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ClickHouseSink implements DataSink {
    private final ClickHouseConfig config;

    public ClickHouseSink(DataSourceConfig config) {
        this.config = (ClickHouseConfig) config;
    }

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", config.getUsername());
        jdbcProps.setProperty("password", config.getPassword());

        boolean overwrite = "overwrite".equalsIgnoreCase(options.getWriteMode());

        RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("写入 ClickHouse，表: {}，模式: {}", options.getResource(), options.getWriteMode());
            if (overwrite) {
                Connection conn = JdbcUtils.getConnection("com.clickhouse.jdbc.ClickHouseDriver",
                        config.getJdbcUrl(), config.getUsername(), config.getPassword());
                try {
                    conn.createStatement().execute("TRUNCATE TABLE " + options.getResource());
                } finally {
                    JdbcUtils.closeConnection(conn);
                }
            }
            df.write().mode("append").jdbc(config.getJdbcUrl(), options.getResource(), jdbcProps);
            return null;
        });
    }

    @Override
    public void writeStream(Dataset<Row> df, WriteOptions options) {
        try {
            df.writeStream().foreachBatch((batchDf, batchId) -> {
                WriteOptions batchOptions = WriteOptions.builder()
                        .resource(options.getResource())
                        .writeMode("append")
                        .build();
                write(batchDf, batchOptions);
            }).start();
        } catch (TimeoutException e) {
            log.error("ClickHouse 流式写入启动超时", e);
            throw new RuntimeException("ClickHouse 流式写入启动失败", e);
        }
    }
}
