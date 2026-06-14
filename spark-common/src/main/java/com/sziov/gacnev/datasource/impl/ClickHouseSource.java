package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.utils.WarehouseException;
import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.DataSourceProvider;
import com.sziov.gacnev.datasource.DataSourceType;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.ClickHouseOption;
import com.sziov.gacnev.utils.spark.SparkParameterTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * ClickHouse 数据读取。
 * 一致性语义：<b>至少一次</b>（Spark JDBC 读，无事务保证）。
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

        // JDBC 超时配置：防止网络抖动导致 Task 无限卡死
        int connectTimeout = SparkParameterTool.getInt(dsConfig,
                ParamsKeyConstant.DATASOURCE_JDBC_CONNECTION_TIMEOUT,
                ParamsDefaultValue.DATASOURCE_JDBC_CONNECTION_TIMEOUT);
        int socketTimeout = SparkParameterTool.getInt(dsConfig,
                ParamsKeyConstant.DATASOURCE_JDBC_SOCKET_TIMEOUT,
                ParamsDefaultValue.DATASOURCE_JDBC_SOCKET_TIMEOUT);
        int queryTimeout = SparkParameterTool.getInt(dsConfig,
                ParamsKeyConstant.DATASOURCE_JDBC_QUERY_TIMEOUT,
                ParamsDefaultValue.DATASOURCE_JDBC_QUERY_TIMEOUT);
        if (connectTimeout > 0) jdbcProps.setProperty("connectTimeout", String.valueOf(connectTimeout));
        if (socketTimeout > 0) jdbcProps.setProperty("socketTimeout", String.valueOf(socketTimeout));

        String tableOrQuery = options.getQuery() != null && !options.getQuery().isEmpty()
                ? "(" + options.getQuery() + ") t"
                : options.getResource();

        log.info("从 ClickHouse 读取数据，表: {}", options.getResource());
        return spark.read().option("queryTimeout", queryTimeout)
                .jdbc(jdbcUrl, tableOrQuery, jdbcProps);
    }
}
