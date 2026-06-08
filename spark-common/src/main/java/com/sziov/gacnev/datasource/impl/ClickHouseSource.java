package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.RetryUtils;
import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.DataSourceProvider;
import com.sziov.gacnev.datasource.DataSourceType;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.ClickHouseOption;
import com.sziov.gacnev.spark.SparkParameterTool;
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
public class ClickHouseSource implements DataSource<ClickHouseOption>, DataSourceProvider {

    @Override
    public DataSourceType type() { return DataSourceType.CLICKHOUSE; }

    @Override
    public DataSource<?> createSource() { return this; }

    @Override
    public DataSink<?> createSink() { return new ClickHouseSink(); }

    private static final int DEFAULT_RETRIES = 3;

    @Override
    public Dataset<Row> read(SparkSession spark, ClickHouseOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String jdbcUrl = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_CK_HOSTS, null);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new WarehouseException("ClickHouse 连接地址未配置，请在 app.properties 中设置 datasource.ck.hosts");
        }
        String username = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_CK_USERNAME, "default");
        String password = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_CK_PASSWORD, "");

        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", username);
        jdbcProps.setProperty("password", password);

        String tableOrQuery = options.getQuery() != null && !options.getQuery().isEmpty()
                ? "(" + options.getQuery() + ") t"
                : options.getResource();

        return RetryUtils.retry(DEFAULT_RETRIES, 1000L, () -> {
            log.info("从 ClickHouse 读取数据，表: {}", options.getResource());
            return spark.read().jdbc(jdbcUrl, tableOrQuery, jdbcProps);
        });
    }
}
