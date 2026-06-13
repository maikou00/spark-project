package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.WarehouseException;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.MySqlOption;
import com.sziov.gacnev.spark.SparkParameterTool;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.Connection;
import com.sziov.gacnev.common.JdbcConnectionPool;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * MySQL 数据写入。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Slf4j
public class MySqlSink implements DataSink<MySqlOption> {

    @Override
    public void write(Dataset<Row> df, MySqlOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String jdbcUrl = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_MYSQL_URL, null);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new WarehouseException("MySQL 连接地址未配置，请在 app.properties 中设置 datasource.mysql.url");
        }
        String resource = options.getResource();
        if (resource != null && !resource.contains(".")) {
            throw new WarehouseException("表名必须为 database.table 格式，当前: " + resource);
        }
        Properties jdbcProps = buildJdbcProps(dsConfig);
        SaveMode mode = options.getWriteMode() != null ? options.getWriteMode() : SaveMode.Append;

        log.info("写入 MySQL，表: {}，模式: {}", options.getResource(), mode);
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
    public void upsert(Dataset<Row> df, MySqlOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String jdbcUrl = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_MYSQL_URL, null);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new WarehouseException("MySQL 连接地址未配置，请在 app.properties 中设置 datasource.mysql.url");
        }
        String resource = options.getResource();
        if (resource != null && !resource.contains(".")) {
            throw new WarehouseException("表名必须为 database.table 格式，当前: " + resource);
        }
        Properties jdbcProps = buildJdbcProps(dsConfig);
        int batchSize = Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_MYSQL_BATCH_SIZE, String.valueOf(ParamsDefaultValue.DATASOURCE_MYSQL_BATCH_SIZE)));

        List<String> upsertKeys = options.getUpsertKeys();
        if (upsertKeys == null || upsertKeys.isEmpty()) {
            throw new WarehouseException("UPSERT 必须指定 upsertKeys（唯一键列名）");
        }

        String table = options.getResource();
        String[] columns = df.columns();
        Set<String> keySet = new HashSet<>(upsertKeys);

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(quote(table)).append(" (");
        StringBuilder valuesPart = new StringBuilder(" VALUES (");
        List<String> nonKeyCols = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sql.append(", ");
                valuesPart.append(", ");
            }
            sql.append(quote(columns[i]));
            valuesPart.append("?");
            if (!keySet.contains(columns[i])) {
                nonKeyCols.add(columns[i]);
            }
        }
        sql.append(")");
        valuesPart.append(")");
        sql.append(valuesPart);

        if (!nonKeyCols.isEmpty()) {
            sql.append(" ON DUPLICATE KEY UPDATE ");
            for (int i = 0; i < nonKeyCols.size(); i++) {
                if (i > 0) sql.append(", ");
                String col = nonKeyCols.get(i);
                sql.append(quote(col)).append("=VALUES(").append(quote(col)).append(")");
            }
        }

        String upsertSql = sql.toString();
        log.info("UPSERT MySQL，表: {}，keys: {}，SQL: {}", table, upsertKeys, upsertSql);

        
    df.foreachPartition((ForeachPartitionFunction<Row>) iterator -> {
        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
             PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            int count = 0;
            while (iterator.hasNext()) {
                Row row = iterator.next();
                for (int i = 0; i < columns.length; i++) {
                    ps.setObject(i + 1, row.get(i));
                }
                ps.addBatch();
                count++;
                if (count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
    });
        
    }

    @Override
    public void execute(MySqlOption options) {
        Properties dsConfig = DataSources.getDsConfig();
        String jdbcUrl = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_MYSQL_URL, null);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new WarehouseException("MySQL 连接地址未配置，请在 app.properties 中设置 datasource.mysql.url");
        }
        Properties jdbcProps = buildJdbcProps(dsConfig);

        String sql = options.getQuery();
        if (sql == null || sql.isEmpty()) {
            throw new WarehouseException("execute 必须通过 option.setQuery() 指定 SQL 语句");
        }

        log.info("执行 MySQL SQL: {}", sql);
        try (Connection conn = JdbcConnectionPool.getConnection(jdbcUrl, jdbcProps);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (java.sql.SQLException e) {
            log.error("MySQL SQL 执行失败: {}", sql, e);
            throw new WarehouseException("MySQL SQL 执行失败", e);
        }
    }

    private static Properties buildJdbcProps(Properties dsConfig) {
        Properties props = new Properties();
        props.setProperty("user", SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_MYSQL_USERNAME, ParamsDefaultValue.DATASOURCE_MYSQL_USERNAME));
        props.setProperty("password", SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_MYSQL_PASSWORD, ParamsDefaultValue.DATASOURCE_MYSQL_PASSWORD));
        props.setProperty("driver", SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_MYSQL_DRIVER, ParamsDefaultValue.DATASOURCE_MYSQL_DRIVER));
        return props;
    }

    private static String quote(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) sb.append(".");
                sb.append("`").append(parts[i]).append("`");
            }
            return sb.toString();
        }
        return "`" + identifier + "`";
    }

    private static boolean isTableNotExists(java.sql.SQLException e) {
        String state = e.getSQLState();
        // MySQL: 42S02, ClickHouse: 42S02 or 60
        return "42S02".equals(state) || "60".equals(state);
    }
}
