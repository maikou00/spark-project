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
        String resource = options.getResource();

        log.info("写入 ClickHouse，表: {}，模式: {}", options.getResource(), mode);
        if (SaveMode.Overwrite.equals(mode)) {
            overwriteAtomic(jdbcUrl, jdbcProps, df, resource);
        } else {
            df.write().mode("append").jdbc(jdbcUrl, resource, jdbcProps);
        }
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

    /**
     * Overwrite 安全写入：先写临时表，再通过 RENAME TABLE 原子切换。
     */
    private void overwriteAtomic(String jdbcUrl, Properties jdbcProps,
                                  Dataset<Row> df, String resource) {
        String tmpTable = resource + "_tmp";
        String oldTable = resource + "_old";
        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
             Statement stmt = conn.createStatement()) {
            try {
                stmt.execute("DROP TABLE IF EXISTS " + tmpTable);
                stmt.execute("DROP TABLE IF EXISTS " + oldTable);
            } catch (java.sql.SQLException ignored) {
            }
        } catch (java.sql.SQLException e) {
            log.error("清理临时表失败: {} / {}", tmpTable, oldTable, e);
            throw new WarehouseException("Overwrite 清理临时表失败", e);
        }

        df.write().mode("append").jdbc(jdbcUrl, tmpTable, jdbcProps);
        log.info("Overwrite 临时表写入完成: {} → {}", tmpTable, resource);

        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
             Statement stmt = conn.createStatement()) {
            if (tableExists(conn, resource)) {
                stmt.execute("RENAME TABLE " + resource + " TO " + oldTable + ", "
                        + tmpTable + " TO " + resource);
            } else {
                stmt.execute("RENAME TABLE " + tmpTable + " TO " + resource);
            }
            log.info("Overwrite RENAME 原子切换完成: {}", resource);
        } catch (java.sql.SQLException e) {
            log.error("RENAME TABLE 失败，源表未受影响: {}", resource, e);
            throw new WarehouseException("Overwrite RENAME TABLE 失败，数据未丢失", e);
        }

        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + oldTable);
        } catch (java.sql.SQLException e) {
            log.warn("Overwrite 旧表清理失败（可手动删除）: {}", oldTable, e);
        }
    }

    private static boolean tableExists(Connection conn, String table) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1 FROM " + table + " WHERE 1=0");
            return true;
        } catch (java.sql.SQLException e) {
            return false;
        }
    }

    private static boolean isTableNotExists(java.sql.SQLException e) {
        String state = e.getSQLState();
        return "42S02".equals(state) || "60".equals(state);
    }
}
