package com.sziov.gacnev.utils.meta;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import com.sziov.gacnev.utils.spark.SparkSqlUtils;
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

    private PartitionUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

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

}
