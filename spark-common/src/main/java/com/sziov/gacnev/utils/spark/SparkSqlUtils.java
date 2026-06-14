package com.sziov.gacnev.utils.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Objects;

/**
 * Spark SQL工具类
 * 提供SQL执行、表操作等功能
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class SparkSqlUtils {


    private SparkSqlUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 执行SQL查询
     *
     * @param spark SparkSession对象
     * @param sql   SQL语句
     * @return DataFrame
     */
    public static Dataset<Row> executeQuery(SparkSession spark, String sql) {
        long startTime = System.currentTimeMillis();
        try {
            Dataset<Row> df = spark.sql(sql);
            long endTime = System.currentTimeMillis();
            log.info("SQL executed successfully, elapsed time: {} ms, SQL: {}", endTime - startTime, sql);
            return df;
        } catch (Exception e) {
            log.error("Failed to execute SQL: {}", sql, e);
            throw new RuntimeException("Failed to execute SQL", e);
        }
    }

    /**
     * 执行SQL更新（CREATE、INSERT、DROP等）
     *
     * @param spark SparkSession对象
     * @param sql   SQL语句
     */
    public static void executeUpdate(SparkSession spark, String sql) {
        long startTime = System.currentTimeMillis();
        try {
            spark.sql(sql);
            long endTime = System.currentTimeMillis();
            log.info("SQL update executed successfully, elapsed time: {} ms, SQL: {}", endTime - startTime, sql);
        } catch (Exception e) {
            log.error("Failed to execute SQL update: {}", sql, e);
            throw new RuntimeException("Failed to execute SQL update", e);
        }
    }

    /**
     * 创建临时视图
     *
     * @param df       DataFrame
     * @param viewName 视图名称
     */
    public static void createTempView(Dataset<Row> df, String viewName) {
        df.createOrReplaceTempView(viewName);
        log.info("Temporary view created: {}", viewName);
    }

    /**
     * 创建全局临时视图
     *
     * @param df       DataFrame
     * @param viewName 视图名称
     */
    public static void createGlobalTempView(Dataset<Row> df, String viewName) {
        df.createOrReplaceGlobalTempView(viewName);
        log.info("Global temporary view created: {}", viewName);
    }

    /**
     * 删除临时视图
     *
     * @param spark    SparkSession对象
     * @param viewName 视图名称
     */
    public static void dropTempView(SparkSession spark, String viewName) {
        spark.catalog().dropTempView(viewName);
        log.info("Temporary view dropped: {}", viewName);
    }

    /**
     * 删除全局临时视图
     *
     * @param spark    SparkSession对象
     * @param viewName 视图名称
     */
    public static void dropGlobalTempView(SparkSession spark, String viewName) {
        spark.catalog().dropGlobalTempView(viewName);
        log.info("Global temporary view dropped: {}", viewName);
    }

    /**
     * 创建数据库
     *
     * @param spark  SparkSession对象
     * @param dbName 数据库名称
     */
    public static void createDatabase(SparkSession spark, String dbName) {
        String sql = String.format("CREATE DATABASE IF NOT EXISTS %s", dbName);
        executeUpdate(spark, sql);
        log.info("Database created: {}", dbName);
    }

    /**
     * 删除数据库
     *
     * @param spark    SparkSession对象
     * @param dbName   数据库名称
     * @param cascade  是否级联删除
     */
    public static void dropDatabase(SparkSession spark, String dbName, boolean cascade) {
        String sql = cascade 
            ? String.format("DROP DATABASE IF EXISTS %s CASCADE", dbName)
            : String.format("DROP DATABASE IF EXISTS %s", dbName);
        executeUpdate(spark, sql);
        log.info("Database dropped: {}, cascade: {}", dbName, cascade);
    }

    /**
     * 删除表
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void dropTable(SparkSession spark, String tableName) {
        String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
        executeUpdate(spark, sql);
        log.info("Table dropped: {}", tableName);
    }

    /**
     * 清空表
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void truncateTable(SparkSession spark, String tableName) {
        String sql = String.format("TRUNCATE TABLE %s", tableName);
        executeUpdate(spark, sql);
        log.info("Table truncated: {}", tableName);
    }

    /**
     * 缓存表
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void cacheTable(SparkSession spark, String tableName) {
        spark.catalog().cacheTable(tableName);
        log.info("Table cached: {}", tableName);
    }

    /**
     * 取消缓存表
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void uncacheTable(SparkSession spark, String tableName) {
        spark.catalog().uncacheTable(tableName);
        log.info("Table uncached: {}", tableName);
    }

    /**
     * 刷新表
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void refreshTable(SparkSession spark, String tableName) {
        spark.catalog().refreshTable(tableName);
        log.info("Table refreshed: {}", tableName);
    }

    /**
     * 设置当前数据库
     *
     * @param spark  SparkSession对象
     * @param dbName 数据库名称
     */
    public static void setCurrentDatabase(SparkSession spark, String dbName) {
        spark.catalog().setCurrentDatabase(dbName);
        log.info("Current database set to: {}", dbName);
    }

    /**
     * 获取表Schema
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     * @return Schema
     */
    public static StructType getTableSchema(SparkSession spark, String tableName) {
        Dataset<Row> df = spark.table(tableName);
        return df.schema();
    }

    /**
     * 打印表Schema
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void printTableSchema(SparkSession spark, String tableName) {
        StructType schema = getTableSchema(spark, tableName);
        if (Objects.nonNull(schema)) {
            log.info("Schema for table {}: \n{}", tableName, schema.treeString());
        }
    }

    /**
     * 获取执行计划
     *
     * @param df DataFrame
     * @return 执行计划
     */
    public static String getExecutionPlan(Dataset<Row> df) {
        return df.queryExecution().toString();
    }

    /**
     * 打印执行计划
     *
     * @param df DataFrame
     */
    public static void printExecutionPlan(Dataset<Row> df) {
        String plan = getExecutionPlan(df);
        log.info("Execution plan: \n{}", plan);
    }


}
