package com.sziov.gacnev.datasource.clickhouse;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.ReadOptions;
import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * ClickHouse 数据读取。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ClickHouseSource implements DataSource {
    private final ClickHouseConfig config;

    public ClickHouseSource(DataSourceConfig config) {
        this.config = (ClickHouseConfig) config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", config.getUsername());
        jdbcProps.setProperty("password", config.getPassword());

        String tableOrQuery = options.getQuery() != null && !options.getQuery().isEmpty()
                ? "(" + options.getQuery() + ") t"
                : options.getResource();

        return RetryUtils.retry(config.getMaxRetries(), 1000L, () -> {
            log.info("从 ClickHouse 读取数据，表: {}", options.getResource());
            return spark.read().jdbc(config.getJdbcUrl(), tableOrQuery, jdbcProps);
        });
    }
}
