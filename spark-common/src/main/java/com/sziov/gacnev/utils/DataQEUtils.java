package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


/**
 * 数据质量校验工具类
 * 提供数据质量检查功能
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class DataQEUtils {

    private DataQEUtils() {}

    /**
     * 检查表是否存在
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     * @return 是否存在
     */
    public static boolean tableExists(SparkSession spark, String tableName) {
        return spark.catalog().tableExists(tableName);
    }

    /**
     * 检查表是否为空
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     * @return 是否为空
     */
    public static boolean isTableEmpty(SparkSession spark, String tableName) {
        Dataset<Row> df = spark.table(tableName);
        return df.isEmpty();
    }

    /**
     * 获取表行数
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     * @return 行数
     */
    public static long getTableRowCount(SparkSession spark, String tableName) {
        Dataset<Row> df = spark.table(tableName);
        return df.count();
    }

    /**
     * 检查空值比例
     *
     * @param df        DataFrame
     * @param columnName 列名
     * @return 空值比例（0.0-1.0）
     */
    public static double getNullRatio(Dataset<Row> df, String columnName) {
        long totalCount = df.count();
        if (totalCount == 0) {
            return 0.0;
        }
        
        long nullCount = df.filter(functions.col(columnName).isNull()).count();
        return (double) nullCount / totalCount;
    }

    /**
     * 检查重复行数
     *
     * @param df DataFrame
     * @return 重复行数
     */
    public static long getDuplicateCount(Dataset<Row> df) {
        long totalCount = df.count();
        long distinctCount = df.distinct().count();
        return totalCount - distinctCount;
    }

    /**
     * 检查列是否存在
     *
     * @param df         DataFrame
     * @param columnName 列名
     * @return 是否存在
     */
    public static boolean columnExists(Dataset<Row> df, String columnName) {
        String[] columns = df.columns();
        for (String col : columns) {
            if (col.equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取列的唯一值数量
     *
     * @param df         DataFrame
     * @param columnName 列名
     * @return 唯一值数量
     */
    public static long getDistinctCount(Dataset<Row> df, String columnName) {
        return df.select(columnName).distinct().count();
    }

    /**
     * 数据质量报告
     *
     * @param spark     SparkSession对象
     * @param tableName 表名称
     */
    public static void printQualityReport(SparkSession spark, String tableName) {
        log.info("========== Data Quality Report for {} ==========", tableName);
        
        Dataset<Row> df = spark.table(tableName);
        long totalCount = df.count();
        log.info("Total Record Count: {}", totalCount);
        
        String[] columns = df.columns();
        log.info("Total Column Count: {}", columns.length);
        
        for (String column : columns) {
            long nullCount = df.filter(functions.col(column).isNull()).count();
            double nullRatio = totalCount > 0 ? (double) nullCount / totalCount * 100 : 0.0;
            long distinctCount = getDistinctCount(df, column);

            log.info("Column: {}, Null Count: {}, Null Ratio: {}%, Distinct Count: {}",
                    column, nullCount, nullRatio, distinctCount);
        }
        
        long duplicateCount = getDuplicateCount(df);
        log.info("Duplicate Row Count: {}", duplicateCount);
        
        log.info("================================================");
    }

    /**
     * 检查数据完整性（非空检查）
     *
     * @param df          DataFrame
     * @param columnNames 列名数组
     * @return 是否完整（无空值）
     */
    public static boolean checkCompleteness(Dataset<Row> df, String... columnNames) {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException("Column names cannot be empty");
        }
        
        for (String columnName : columnNames) {
            double nullRatio = getNullRatio(df, columnName);
            if (nullRatio > 0) {
                log.warn("Column {} has null values, null ratio: {}%", columnName, nullRatio * 100);
                return false;
            }
        }
        return true;
    }

    /**
     * 检查数据唯一性
     *
     * @param df          DataFrame
     * @param columnNames 列名数组（作为唯一键）
     * @return 是否唯一
     */
    public static boolean checkUniqueness(Dataset<Row> df, String... columnNames) {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException("Column names cannot be empty");
        }
        
        long totalCount = df.count();
        long distinctCount = df.selectExpr(columnNames).distinct().count();
        boolean isUnique = totalCount == distinctCount;
        
        if (!isUnique) {
            log.warn("Data is not unique on columns: {}, total: {}, distinct: {}", 
                String.join(", ", columnNames), totalCount, distinctCount);
        }
        
        return isUnique;
    }

    /**
     * 检查数据范围
     *
     * @param df         DataFrame
     * @param columnName 列名
     * @param minValue   最小值
     * @param maxValue   最大值
     * @return 是否在范围内
     */
    public static boolean checkRange(Dataset<Row> df, String columnName, long minValue, long maxValue) {
        long outOfRangeCount = df.filter(
            functions.col(columnName).lt(minValue).or(functions.col(columnName).gt(maxValue))
        ).count();
        
        boolean isInRange = outOfRangeCount == 0;
        if (!isInRange) {
            log.warn("Column {} has {} records out of range [{}, {}]", 
                columnName, outOfRangeCount, minValue, maxValue);
        }
        
        return isInRange;
    }

    /**
     * 检查数据一致性（两个表的行数是否一致）
     *
     * @param df1 DataFrame1
     * @param df2 DataFrame2
     * @return 是否一致
     */
    public static boolean checkConsistency(Dataset<Row> df1, Dataset<Row> df2) {
        long count1 = df1.count();
        long count2 = df2.count();
        boolean isConsistent = count1 == count2;
        
        if (!isConsistent) {
            log.warn("Data is not consistent, df1 count: {}, df2 count: {}", count1, count2);
        }
        
        return isConsistent;
    }
}
