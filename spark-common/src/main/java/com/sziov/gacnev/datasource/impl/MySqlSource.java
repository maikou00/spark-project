package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.DataSourceProvider;
import com.sziov.gacnev.datasource.DataSourceType;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.MySqlOption;
import com.sziov.gacnev.spark.SparkParameterTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;

/**
 * MySQL 数据读取。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Slf4j
public class MySqlSource implements DataSource<MySqlOption>, DataSourceProvider {

    @Override
    public DataSourceType type() { return DataSourceType.MYSQL; }

    @Override
    public DataSource<?> createSource() { return this; }

    @Override
    public DataSink<?> createSink() { return new MySqlSink(); }

    @Override
    public Dataset<Row> read(SparkSession spark, MySqlOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String jdbcUrl = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_MYSQL_URL, null);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new WarehouseException("MySQL 连接地址未配置，请在 app.properties 中设置 datasource.mysql.url");
        }
        String resource = options.getResource();
        if (resource != null && !resource.contains(".")) {
            throw new WarehouseException("表名必须为 database.table 格式，当前: " + resource);
        }
        String username = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_MYSQL_USERNAME, ParamsDefaultValue.DATASOURCE_MYSQL_USERNAME);
        String password = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_MYSQL_PASSWORD, ParamsDefaultValue.DATASOURCE_MYSQL_PASSWORD);
        String driver = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_MYSQL_DRIVER, ParamsDefaultValue.DATASOURCE_MYSQL_DRIVER);

        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", username);
        jdbcProps.setProperty("password", password);
        jdbcProps.setProperty("driver", driver);

        String tableOrQuery = options.getQuery() != null && !options.getQuery().isEmpty()
                ? "(" + options.getQuery() + ") t"
                : options.getResource();

        
    log.info("从 MySQL 读取数据，表: {}", options.getResource());
    List<String> predicates = options.getPredicates();
    if (predicates != null && !predicates.isEmpty()) {
        return spark.read().jdbc(jdbcUrl, tableOrQuery, predicates.toArray(new String[0]), jdbcProps);
    }
    String partitionColumn = options.getPartitionColumn();
    if (partitionColumn != null && !partitionColumn.isEmpty()) {
        long lower = options.getLowerBound() != null ? options.getLowerBound() : 0L;
        long upper = options.getUpperBound() != null ? options.getUpperBound() : Long.MAX_VALUE;
        int parts = options.getNumPartitions() != null ? options.getNumPartitions() : ParamsDefaultValue.DATASOURCE_MYSQL_NUM_PARTITIONS;
        return spark.read().jdbc(jdbcUrl, tableOrQuery, partitionColumn, lower, upper, parts, jdbcProps);
    }
    return spark.read().jdbc(jdbcUrl, tableOrQuery, jdbcProps);
        
    }
}
