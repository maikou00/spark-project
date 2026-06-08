package com.sziov.gacnev.meta;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Hive 元数据工具类，提供库/表/分区等元数据查询功能。
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class HiveMetaUtils {

    private HiveMetaUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 获取所有数据库列表。
     *
     * @param spark SparkSession
     * @return 数据库名列表（可能为空）
     */
    public static List<String> getDatabaseList(SparkSession spark) {
        Objects.requireNonNull(spark, "spark must not be null");
        try {
            Dataset<Row> df = spark.sql("SHOW DATABASES");
            return extractStringColumn(df);
        } catch (Exception e) {
            if (e instanceof org.apache.spark.sql.AnalysisException) {
                log.warn("获取数据库列表失败: {}", e.getMessage());
            } else {
                log.error("获取数据库列表异常", e);
            }
            return Collections.emptyList();
        }
    }

    /**
     * 获取指定数据库下的所有表。
     *
     * @param spark  SparkSession
     * @param dbName 数据库名称
     * @return 表名列表
     */
    public static List<String> getTableList(SparkSession spark, String dbName) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(dbName, "dbName must not be null");
        try {
            Dataset<Row> df = spark.sql(String.format("SHOW TABLES IN %s", dbName));
            // SHOW TABLES 返回: namespace, tableName, isTemporary
            return df.select("tableName").javaRDD().map(row -> row.getString(0)).take(5000);
        } catch (Exception e) {
            if (e instanceof org.apache.spark.sql.AnalysisException) {
                log.warn("数据库 {} 不存在: {}", dbName, e.getMessage());
            } else {
                log.error("获取数据库 {} 的表列表异常", dbName, e);
            }
            return Collections.emptyList();
        }
    }

    /**
     * 获取表的详细信息（DESCRIBE EXTENDED）。
     *
     * @param spark     SparkSession
     * @param tableName 表名称
     * @return 表详细信息
     */
    public static Dataset<Row> getTableDetail(SparkSession spark, String tableName) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        return spark.sql(String.format("DESCRIBE EXTENDED %s", tableName));
    }

    /**
     * 获取表的列信息。
     *
     * @param spark     SparkSession
     * @param tableName 表名称
     * @return 列信息
     */
    public static Dataset<Row> getTableColumns(SparkSession spark, String tableName) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        return spark.sql(String.format("DESCRIBE %s", tableName));
    }

    /**
     * 获取表的建表语句。
     *
     * @param spark     SparkSession
     * @param tableName 表名称
     * @return 建表语句
     */
    public static String getCreateTableDDL(SparkSession spark, String tableName) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        try {
            Dataset<Row> df = spark.sql(String.format("SHOW CREATE TABLE %s", tableName));
            return df.javaRDD().map(row -> row.getString(0)).take(1).stream()
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            if (e instanceof org.apache.spark.sql.AnalysisException) {
                log.warn("表 {} 不存在: {}", tableName, e.getMessage());
            } else {
                log.error("获取表 {} 的建表语句异常", tableName, e);
            }
            return "";
        }
    }

    /**
     * 判断数据库是否存在。
     *
     * @param spark  SparkSession
     * @param dbName 数据库名称
     * @return 是否存在
     */
    public static boolean databaseExists(SparkSession spark, String dbName) {
        return getDatabaseList(spark).contains(dbName);
    }

    /**
     * 判断表是否存在。
     *
     * @param spark     SparkSession
     * @param tableName 表名称
     * @return 是否存在
     */
    public static boolean tableExists(SparkSession spark, String tableName) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        try {
            return spark.catalog().tableExists(tableName);
        } catch (Exception e) {
            if (!(e instanceof org.apache.spark.sql.AnalysisException)) {
                log.error("检查表 {} 是否存在异常", tableName, e);
            }
            return false;
        }
    }

    /**
     * 获取表的属性信息。
     *
     * @param spark     SparkSession
     * @param tableName 表名称
     * @return 属性信息
     */
    public static Dataset<Row> getTableProperties(SparkSession spark, String tableName) {
        Objects.requireNonNull(spark, "spark must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        return spark.sql(String.format("SHOW TBLPROPERTIES %s", tableName));
    }

    /**
     * 打印表的详细信息。
     *
     * @param spark     SparkSession
     * @param tableName 表名称
     */
    public static void printTableDetail(SparkSession spark, String tableName) {
        Dataset<Row> detail = getTableDetail(spark, tableName);
        detail.show(100, false);
    }

    /**
     * 打印数据库列表。
     *
     * @param spark SparkSession
     */
    public static void printDatabaseList(SparkSession spark) {
        List<String> databases = getDatabaseList(spark);
        log.info("数据库列表: {}", databases);
    }

    /**
     * 打印表列表。
     *
     * @param spark  SparkSession
     * @param dbName 数据库名称
     */
    public static void printTableList(SparkSession spark, String dbName) {
        List<String> tables = getTableList(spark, dbName);
        log.info("数据库 {} 中的表: {}", dbName, tables);
    }

    /**
     * 从 SHOW 类型查询中提取第一列字符串结果，防止 OOM。
     */
    private static List<String> extractStringColumn(Dataset<Row> df) {
        // 使用 take(5000) 代替 collect()，防止大表 OOM
        return df.javaRDD().map(row -> row.getString(0)).take(5000);
    }
}
