package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.WarehouseException;

import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.ClickHouseOption;
import com.sziov.gacnev.spark.SparkParameterTool;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.sziov.gacnev.common.JdbcConnectionPool;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

/**
 * ClickHouse 数据写入。
 *
 * <p>ClickHouse 为 OLAP 列存引擎，仅支持 Append/Overwrite/Execute。
 * 如需去重，请使用 ReplacingMergeTree 引擎配合纯 INSERT 写入。</p>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ClickHouseSink implements DataSink<ClickHouseOption> {

    @Override
    public void write(Dataset<Row> df, ClickHouseOption options) {
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
        SaveMode mode = options.getWriteMode() != null ? options.getWriteMode() : SaveMode.Append;

        log.info("写入 ClickHouse，表: {}，模式: {}", options.getResource(), mode);
        if (SaveMode.Overwrite.equals(mode)) {
            try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("TRUNCATE TABLE " + options.getResource());
            } catch (java.sql.SQLException e) {
                if (isTableNotExists(e)) {
                    log.info("TRUNCATE 跳过（表不存在）: {}", options.getResource());
                } else {
                    log.error("TRUNCATE 失败，表: {}，原因: {}", options.getResource(), e.getMessage());
                    throw new WarehouseException("Overwrite 模式下 TRUNCATE 失败", e);
                }
            }
        }
        df.write().mode("append").jdbc(jdbcUrl, options.getResource(), jdbcProps);
    }

    @Override
    public void execute(ClickHouseOption options) {
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

        String sql = options.getQuery();
        if (sql == null || sql.isEmpty()) {
            throw new WarehouseException("execute 必须通过 option.setQuery() 指定 SQL 语句");
        }

        log.info("执行 ClickHouse SQL: {}", sql);
        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (java.sql.SQLException e) {
            log.error("ClickHouse SQL 执行失败: {}", sql, e);
            throw new WarehouseException("ClickHouse SQL 执行失败", e);
        }
    }

    private static boolean isTableNotExists(java.sql.SQLException e) {
        String state = e.getSQLState();
        return "42S02".equals(state) || "60".equals(state);
    }
}
