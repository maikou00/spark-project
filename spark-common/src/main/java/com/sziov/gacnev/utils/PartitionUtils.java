package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 分区管理工具类
 * 提供Hive表分区管理、DataFrame分区操作等功能
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class PartitionUtils {

    private PartitionUtils() {}

    /**
     * 添加分区
     *
     * @param spark        SparkSession对象
     * @param tableName    表名称
     * @param partitionSpec 分区规格（如：dt='2026-05-17'）
     */
    public static void addPartition(SparkSession spark, String tableName, String partitionSpec) {
        String sql = String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s)", tableName, partitionSpec);
        SparkSqlUtils.executeUpdate(spark, sql);
        log.info("Partition added: {}, spec: {}", tableName, partitionSpec);
    }

    /**
     * 删除分区
     *
     * @param spark        SparkSession对象
     * @param tableName    表名称
     * @param partitionSpec 分区规格（如：dt='2026-05-17'）
     */
    public static void dropPartition(SparkSession spark, String tableName, String partitionSpec) {
        String sql = String.format("ALTER TABLE %s DROP IF EXISTS PARTITION (%s)", tableName, partitionSpec);
        SparkSqlUtils.executeUpdate(spark, sql);
        log.info("Partition dropped: {}, spec: {}", tableName, partitionSpec);
    }

    /**
     * 修复分区（刷新分区元数据）
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void repairPartition(SparkSession spark, String tableName) {
        String sql = String.format("MSCK REPAIR TABLE %s", tableName);
        SparkSqlUtils.executeUpdate(spark, sql);
        log.info("Partition repaired: {}", tableName);
    }

    /**
     * 获取分区列表
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     * @return 分区列表
     */
    public static List<String> getPartitionList(SparkSession spark, String tableName) {
        try {
            Dataset<Row> df = spark.sql(String.format("SHOW PARTITIONS %s", tableName));
            // 使用 take(5000) 替代 collect()，防止大表 OOM
            List<Row> rows = df.takeAsList(5000);
            List<String> partitionList = new ArrayList<>();
            for (Row row : rows) {
                partitionList.add(row.getString(0));
            }
            return partitionList;
        } catch (Exception e) {
            log.error("Failed to get partition list: {}", tableName, e);
            return new ArrayList<>();
        }
    }

    /**
     * 获取分区数量
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     * @return 分区数量
     */
    public static long getPartitionCount(SparkSession spark, String tableName) {
        List<String> partitions = getPartitionList(spark, tableName);
        return partitions.size();
    }

    /**
     * 判断分区是否存在
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     * @param partitionSpec 分区规格（如：dt='2026-05-17'）
     * @return 是否存在
     */
    public static boolean partitionExists(SparkSession spark, String tableName, String partitionSpec) {
        List<String> partitions = getPartitionList(spark, tableName);
        return partitions.stream().anyMatch(p -> p.contains(partitionSpec));
    }

    /**
     * 设置DataFrame分区数
     *
     * @param df        DataFrame
     * @param numPartitions 分区数
     * @return 分区后的DataFrame
     */
    public static Dataset<Row> repartition(Dataset<Row> df, int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Num partitions must be positive");
        }
        
        Dataset<Row> result = df.repartition(numPartitions);
        log.info("DataFrame repartitioned to {} partitions", numPartitions);
        return result;
    }

    /**
     * 设置DataFrame分区数（按列分区）
     *
     * @param df        DataFrame
     * @param numPartitions 分区数
     * @param columnNames 列名数组
     * @return 分区后的DataFrame
     */
    public static Dataset<Row> repartition(Dataset<Row> df, int numPartitions, String... columnNames) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Num partitions must be positive");
        }
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException("Column names cannot be empty");
        }
        
        // 将所有列名转为 Column[]，传入 repartition
        org.apache.spark.sql.Column[] cols = new org.apache.spark.sql.Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            cols[i] = org.apache.spark.sql.functions.col(columnNames[i]);
        }
        Dataset<Row> result = df.repartition(numPartitions, cols);
        log.info("DataFrame repartitioned to {} partitions by columns: {}", numPartitions, Arrays.toString(columnNames));
        return result;
    }

    /**
     * 合并DataFrame分区（减少分区数）
     *
     * @param df        DataFrame
     * @param numPartitions 分区数
     * @return 合并后的DataFrame
     */
    public static Dataset<Row> coalesce(Dataset<Row> df, int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Num partitions must be positive");
        }
        
        Dataset<Row> result = df.coalesce(numPartitions);
        log.info("DataFrame coalesced to {} partitions", numPartitions);
        return result;
    }

    /**
     * 获取DataFrame当前分区数
     *
     * @param df DataFrame
     * @return 分区数
     */
    public static int getCurrentPartitionNum(Dataset<Row> df) {
        return df.rdd().getNumPartitions();
    }

    /**
     * 打印DataFrame分区信息
     *
     * @param df DataFrame
     */
    public static void printPartitionInfo(Dataset<Row> df) {
        int partitionNum = getCurrentPartitionNum(df);
        log.info("Current partition number: {}", partitionNum);
    }
}
