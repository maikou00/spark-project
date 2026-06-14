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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import com.sziov.gacnev.common.JdbcConnectionPool;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

/**
 * ClickHouse 数据写入。
 *
 * <p>一致性语义：Append 写入为 <b>至少一次</b>，Overwrite（RENAME TABLE 原子切换）为 <b>精确一次</b>（同 JVM 内）。</p>
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
            ensureTable(jdbcUrl, jdbcProps, resource, df);
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
     * <p><b>注意：</b>tableExists() 与 RENAME TABLE 之间存在 TOCTOU 竞态窗口，
     * 生产环境须通过调度层保证同一表同时仅一个 Overwrite Job 运行。</p>
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

        // ClickHouse 24.x 要求显式指定 ENGINE + ORDER BY，Spark JDBC 自动建表不满足
        ensureTable(jdbcUrl, jdbcProps, tmpTable, df);
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

    /**
     * 确保目标表存在，不存在则按 DataFrame Schema 自动创建。
     * ClickHouse 24.x 要求 MergeTree 必须指定 ORDER BY，此处使用 {@code tuple()} 作为默认排序键。
     */
    private void ensureTable(String jdbcUrl, Properties jdbcProps, String table, Dataset<Row> df) {
        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps)) {
            if (tableExists(conn, table)) {
                return;
            }
            String ddl = buildCreateTableDDL(table, df.schema());
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
                log.info("ClickHouse 自动建表: {}", ddl);
            }
        } catch (java.sql.SQLException e) {
            log.error("ClickHouse 建表失败: {}", table, e);
            throw new WarehouseException("ClickHouse 建表失败: " + table, e);
        }
    }

    private String buildCreateTableDDL(String table, StructType schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");
        String[] fields = schema.fieldNames();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(fields[i]).append(" ").append(toClickHouseType(schema.apply(i).dataType()));
        }
        sb.append(") ENGINE = MergeTree() ORDER BY tuple()");
        return sb.toString();
    }

    /**
     * Spark 类型 → ClickHouse 类型映射。
     * <p><b>注意：</b>{@code timestamp} 映射为 {@code DateTime}（秒级精度），
     * {@code decimal} 统一映射为 {@code Decimal(38,18)}（忽略原始 precision/scale）。
     * 如需保留完整精度，请手动建表后使用 Append 模式写入。</p>
     */
    private static String toClickHouseType(DataType sparkType) {
        String typeName = sparkType.typeName();
        switch (typeName) {
            case "integer":   return "Int32";
            case "long":      return "Int64";
            case "float":     return "Float32";
            case "double":    return "Float64";
            case "string":    return "String";
            case "boolean":   return "UInt8";
            case "short":     return "Int16";
            case "byte":      return "Int8";
            case "date":      return "Date";
            case "timestamp": return "DateTime";
            case "decimal":   return "Decimal(38,18)";
            default:          return "String";
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
}
