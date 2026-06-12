package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.RetryUtils;
import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.datasource.option.ClickHouseOption;
import com.sziov.gacnev.spark.SparkParameterTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ClickHouse 数据写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ClickHouseSink implements DataSink<ClickHouseOption> {

    private static final int DEFAULT_RETRIES = 3;

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

        RetryUtils.retry(DEFAULT_RETRIES, 1000L, () -> {
            log.info("写入 ClickHouse，表: {}，模式: {}", options.getResource(), mode);
            if (SaveMode.Overwrite.equals(mode)) {
                try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl, jdbcProps);
                     java.sql.Statement stmt = conn.createStatement()) {
                    try {
                        stmt.execute("TRUNCATE TABLE " + options.getResource());
                    } catch (java.sql.SQLException ignored) {
                    }
                }
            }
            df.write().mode("append").jdbc(jdbcUrl, options.getResource(), jdbcProps);
            return null;
        });
    }

    @Override
    public void upsert(Dataset<Row> df, ClickHouseOption options) {
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

        List<String> upsertKeys = options.getUpsertKeys();
        if (upsertKeys == null || upsertKeys.isEmpty()) {
            throw new WarehouseException("UPSERT 必须指定 upsertKeys（唯一键列名）");
        }

        String table = options.getResource();
        String[] columns = df.columns();
        Set<String> keySet = new HashSet<>(upsertKeys);
        List<Integer> keyIndexes = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            if (keySet.contains(columns[i])) {
                keyIndexes.add(i);
            }
        }

        int batchSize = Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_CK_BATCH_SIZE, String.valueOf(ParamsDefaultValue.DATASOURCE_CK_BATCH_SIZE)));

        String upsertSql = buildUpsertSql(table, columns);

        log.info("UPSERT ClickHouse，表: {}，keys: {}", table, upsertKeys);

        RetryUtils.retry(DEFAULT_RETRIES, 1000L, () -> {
            df.foreachPartition((ForeachPartitionFunction<Row>) iterator -> {
                try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcProps)) {
                    List<Row> batch = new ArrayList<>();
                    while (iterator.hasNext()) {
                        batch.add(iterator.next());
                        if (batch.size() >= batchSize) {
                            deleteAndInsert(conn, batch, table, keyIndexes, columns, upsertSql);
                            batch.clear();
                        }
                    }
                    if (!batch.isEmpty()) {
                        deleteAndInsert(conn, batch, table, keyIndexes, columns, upsertSql);
                    }
                }
            });
            return null;
        });
    }

    private static void deleteAndInsert(Connection conn, List<Row> batch, String table,
                                         List<Integer> keyIndexes, String[] columns, String upsertSql) {
        try (Statement stmt = conn.createStatement()) {
            for (Row row : batch) {
                StringBuilder cond = new StringBuilder();
                for (int i = 0; i < keyIndexes.size(); i++) {
                    if (i > 0) cond.append(" AND ");
                    int idx = keyIndexes.get(i);
                    Object val = row.get(idx);
                    cond.append(columns[idx]).append("=").append(formatValue(val));
                }
                stmt.execute("ALTER TABLE " + table + " DELETE WHERE " + cond);
            }
        } catch (Exception e) {
            throw new RuntimeException("UPSERT 删除阶段失败", e);
        }

        try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            for (Row row : batch) {
                for (int i = 0; i < columns.length; i++) {
                    ps.setObject(i + 1, row.get(i));
                }
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            throw new RuntimeException("UPSERT 插入阶段失败", e);
        }
    }

    private static String formatValue(Object val) {
        if (val == null) return "NULL";
        if (val instanceof Number) return val.toString();
        return "'" + val.toString().replace("'", "\\'") + "'";
    }

    private static String buildUpsertSql(String table, String[] columns) {
        String cols = String.join(", ", columns);
        String placeholders = java.util.Arrays.stream(columns).map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")";
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
        RetryUtils.retry(DEFAULT_RETRIES, 1000L, () -> {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcProps);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
            return null;
        });
    }
}
