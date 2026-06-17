package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.utils.WarehouseException;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.DorisOption;
import com.sziov.gacnev.utils.spark.SparkParameterTool;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.Connection;
import com.sziov.gacnev.utils.JdbcConnectionPool;
import java.sql.Statement;
import java.util.Properties;

/**
 * Doris 数据写入，通过 Spark-Doris-Connector Stream Load 批量导入。
 * <p>一致性语义：Stream Load 为 <b>至少一次</b>（通过 label 去重可实现幂等，配置 2PC 可提升一致性）。</p>
 *
 * @author maikou
 * @since 2026-06-12
 */
@Slf4j
public class DorisSink implements DataSink<DorisOption> {

    @Override
    public void write(Dataset<Row> df, DorisOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String fenodes = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_DORIS_FENODES, null);
        if (fenodes == null || fenodes.isEmpty()) {
            throw new WarehouseException("Doris FE 地址未配置，请在 app.properties 中设置 datasource.doris.fenodes");
        }
        String resource = options.getResource();
        if (resource != null && !resource.contains(".")) {
            throw new WarehouseException("表名必须为 database.table 格式，当前: " + resource);
        }
        String username = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_USERNAME, ParamsDefaultValue.DATASOURCE_DORIS_USERNAME);
        String password = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_PASSWORD, ParamsDefaultValue.DATASOURCE_DORIS_PASSWORD);

        SaveMode mode = options.getWriteMode() != null ? options.getWriteMode() : SaveMode.Append;

        String queryPort = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_QUERY_PORT, ParamsDefaultValue.DATASOURCE_DORIS_QUERY_PORT);
        String enable2pc = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_SINK_ENABLE_2PC,
                String.valueOf(ParamsDefaultValue.DATASOURCE_DORIS_SINK_ENABLE_2PC));
        String labelPrefix = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_SINK_LABEL_PREFIX,
                ParamsDefaultValue.DATASOURCE_DORIS_SINK_LABEL_PREFIX);
        String maxRetries = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_SINK_MAX_RETRIES,
                String.valueOf(ParamsDefaultValue.DATASOURCE_DORIS_SINK_MAX_RETRIES));
        String batchSize = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_SINK_BATCH_SIZE,
                String.valueOf(ParamsDefaultValue.DATASOURCE_DORIS_SINK_BATCH_SIZE));
        String batchInterval = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_SINK_BATCH_INTERVAL_MS,
                String.valueOf(ParamsDefaultValue.DATASOURCE_DORIS_SINK_BATCH_INTERVAL_MS));

        log.info("Stream Load 写入 Doris，表: {}，模式: {}，2PC: {}", resource, mode, enable2pc);
        DataFrameWriter writer = df.write()
                .format("doris")
                .option("doris.fenodes", fenodes)
                .option("doris.query.port", queryPort)
                .option("doris.sink.enable-2pc", enable2pc)
                .option("doris.sink.label-prefix", labelPrefix)
                .option("doris.sink.max-retries", maxRetries)
                .option("doris.sink.batch.size", batchSize)
                .option("doris.sink.batch.interval.ms", batchInterval)
                .option("user", username)
                .option("password", password == null ? "" : password)
                .option("doris.table.identifier", resource);

        if (SaveMode.Overwrite.equals(mode)) {
            writer = writer.mode("overwrite");
        } else {
            writer = writer.mode("append");
        }

        writer.save();
    }

    @Override
    public void execute(DorisOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String jdbcUrl = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_DORIS_URL, null);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new WarehouseException("Doris JDBC 地址未配置，请在 app.properties 中设置 datasource.doris.url");
        }
        String sql = options.getQuery();
        if (sql == null || sql.isEmpty()) {
            throw new WarehouseException("execute 必须通过 option.setQuery() 指定 SQL 语句");
        }
        Properties jdbcProps = buildJdbcProps(dsConfig);

        log.info("执行 Doris DDL/DML: {}", sql);
        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (java.sql.SQLException e) {
            log.error("Doris DDL/DML 执行失败: {}", sql, e);
            throw new WarehouseException("Doris DDL/DML 执行失败", e);
        }
    }

    private static Properties buildJdbcProps(Properties dsConfig) {
        Properties props = new Properties();
        props.setProperty("user", SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_USERNAME, ParamsDefaultValue.DATASOURCE_DORIS_USERNAME));
        String password = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_DORIS_PASSWORD, ParamsDefaultValue.DATASOURCE_DORIS_PASSWORD);
        if (password != null && !password.isEmpty()) {
            props.setProperty("password", password);
        }
        return props;
    }
}
