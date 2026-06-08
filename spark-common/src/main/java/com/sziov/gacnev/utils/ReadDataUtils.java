package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Spark数据读取工具类
 * <p>禁用自动schema推断，强制自定义Schema</p>
 * 
 * @author maikou
 * @since 2026-05-18
 */
@Slf4j
public final class ReadDataUtils {

    /**
     * 默认编码格式
     */
    private static final String DEFAULT_ENCODING = "UTF-8";

    /**
     * 默认多行JSON解析（默认关闭，支持JSON Lines格式）
     * 
     * <p>修改原因：修复JSON Lines格式文件读取问题，默认关闭multiline以支持每行一个JSON对象的格式</p>
     */
    private static final String MULTILINE_DISABLED = "false";

    /**
     * 默认Schema合并
     */
    private static final String MERGE_SCHEMA_FALSE = "false";

    /**
     * 默认CSV分隔符
     */
    private static final String DEFAULT_DELIMITER = ",";

    /**
     * 默认CSV无头
     */
    private static final String HEADER_FALSE = "false";

    /**
     * 文本列名
     */
    private static final String TEXT_COLUMN_NAME = "raw_text";

    /**
     * 私有构造方法，防止实例化
     */
    private ReadDataUtils() {
        throw new AssertionError("工具类禁止实例化");
    }


    /**
     * 读取Hive表数据（带分区过滤）
     * 
     * <p>修改原因：添加参数校验和日志记录</p>
     *
     * @param spark SparkSession对象
     * @param database 数据库名
     * @param table 表名
     * @param partitionFilter 分区过滤条件（如：dt='2023-01-01'）
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readHiveTable(SparkSession spark,
                                             String database,
                                             String table,
                                             String partitionFilter) {
        String sql = String.format("SELECT * FROM %s.%s WHERE %s", database, table, partitionFilter);
        log.info("读取Hive表SQL-带分区过滤: {}", sql);
        return spark.sql(sql);
    }


    /**
     * 读取Hive表数据（无分区过滤）
     * 
     * <p>修改原因：新增无分区过滤的读取方法</p>
     *
     * @param spark SparkSession对象
     * @param database 数据库名
     * @param table 表名
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readHiveTable(SparkSession spark,
                                             String database,
                                             String table) {
        String sql = String.format("SELECT * FROM %s.%s", database, table);
        log.info("读取Hive表SQL-无分区过滤: {}", sql);
        return spark.sql(sql);
    }


    /**
     * 读取JSON文件（自定义Schema）
     *
     * @param spark SparkSession对象
     * @param path 文件路径
     * @param customSchema 自定义Schema
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readJsonWithSchema(SparkSession spark,
                                                  String path,
                                                  StructType customSchema) {
        log.info("读取JSON文件: {}, Schema字段数: {}", path, customSchema.fields().length);
        return spark.read()
                .schema(customSchema)
                .option("multiline", MULTILINE_DISABLED)
                .option("encoding", DEFAULT_ENCODING)
                .option("mergeSchema", MERGE_SCHEMA_FALSE)
                .json(path);
    }


    /**
     * 读取CSV文件（自定义Schema）
     *
     * @param spark SparkSession对象
     * @param path 文件路径
     * @param delimiter 分隔符
     * @param customSchema 自定义Schema
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readCsvWithSchema(SparkSession spark,
                                                 String path,
                                                 String delimiter,
                                                 StructType customSchema) {
        String actualDelimiter = delimiter == null ? DEFAULT_DELIMITER : delimiter;
        log.info("读取CSV文件: {}, 分隔符: {}, Schema字段数: {}", path, actualDelimiter, customSchema.fields().length);
        return spark.read()
                .schema(customSchema)
                .option("delimiter", actualDelimiter)
                .option("header", HEADER_FALSE)
                .option("encoding", DEFAULT_ENCODING)
                .csv(path);
    }


    /**
     * 读取Parquet文件
     *
     * @param spark SparkSession对象
     * @param path 文件路径
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readParquet(SparkSession spark, String path) {
        log.info("读取Parquet文件: {}", path);
        return spark.read().option("mergeSchema", MERGE_SCHEMA_FALSE).parquet(path);
    }

    /**
     * 读取ORC文件
     *
     * @param spark SparkSession对象
     * @param path 文件路径
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readOrc(SparkSession spark, String path) {
        log.info("读取ORC文件: {}", path);
        return spark.read().orc(path);
    }


    /**
     * 读取文本文件
     *
     * @param spark SparkSession对象
     * @param path 文件路径
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readText(SparkSession spark, String path) {
        log.info("读取文本文件: {}", path);
        return spark.read().textFile(path).toDF(TEXT_COLUMN_NAME);
    }

    /**
     * 读取文本文件（自定义列名）
     *
     * @param spark SparkSession对象
     * @param path 文件路径
     * @param columnName 列名
     * @return Dataset数据集
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static Dataset<Row> readText(SparkSession spark, String path, String columnName) {
        String actualColumnName = columnName == null ? TEXT_COLUMN_NAME : columnName;
        log.info("读取文本文件: {}, 列名: {}", path, actualColumnName);
        return spark.read().textFile(path).toDF(actualColumnName);
    }
}
