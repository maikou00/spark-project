package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

/**
 * Spark数据写入工具类
 * 
 * <p>统一数据落地工具类，支持Hive分区、小文件合并、压缩等</p>
 * 
 * @author maikou
 * @since 2026-05-18
 */
@Slf4j
public final class WriteDataUtils {

    /**
     * Snappy压缩编解码器
     */
    private static final String SNAPPY_COMPRESS = "snappy";

    /**
     * Hive动态分区配置键
     */
    private static final String HIVE_DYNAMIC_PARTITION = "hive.exec.dynamic.partition";

    /**
     * Hive动态分区模式配置键
     */
    private static final String HIVE_DYNAMIC_PARTITION_MODE = "hive.exec.dynamic.partition.mode";

    /**
     * Parquet压缩编解码器配置键
     */
    private static final String PARQUET_COMPRESS_CODEC = "spark.sql.parquet.compression.codec";

    /**
     * ORC压缩编解码器配置键
     */
    private static final String ORC_COMPRESS_CODEC = "spark.sql.orc.compression.codec";

    /**
     * 动态分区启用值
     */
    private static final String DYNAMIC_PARTITION_TRUE = "true";

    /**
     * 动态分区非严格模式值
     */
    private static final String DYNAMIC_PARTITION_NONSTRICT = "nonstrict";

    /**
     * 临时视图名称
     */
    private static final String TEMP_VIEW_NAME = "tmp_write_data";

    /**
     * 私有构造方法，防止实例化
     */
    private WriteDataUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }



    /**
     * Hive静态分区覆盖写入（日跑离线主流）
     * 
     * <p>修改原因：添加参数校验和日志记录</p>
     *
     * @param df Dataset数据集
     * @param database 数据库名
     * @param table 表名
     * @param partitionKey 分区字段名
     * @param partitionVal 分区值
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static void writeHiveStaticPartition(Dataset<Row> df,
                                                String database,
                                                String table,
                                                String partitionKey,
                                                String partitionVal) {
        Objects.requireNonNull(df, "Dataset不能为空");
        Objects.requireNonNull(database, "数据库名不能为空");
        Objects.requireNonNull(table, "表名不能为空");
        Objects.requireNonNull(partitionKey, "分区字段名不能为空");
        Objects.requireNonNull(partitionVal, "分区值不能为空");

        SparkSession spark = df.sparkSession();
        initHiveConfig(spark);
        df.createOrReplaceTempView(TEMP_VIEW_NAME);
        String sql = String.format(
                "INSERT OVERWRITE TABLE %s.%s PARTITION (%s='%s') SELECT * FROM %s",
                database, table, partitionKey, partitionVal, TEMP_VIEW_NAME
        );
        log.info("执行Hive静态分区写入SQL: {}", sql);
        spark.sql(sql);
        log.info("Hive静态分区写入完成，数据库: {}，表: {}，分区: {}={}", database, table, partitionKey, partitionVal);
    }


    /**
     * Hive动态分区写入
     *
     * @param df Dataset数据集
     * @param database 数据库名
     * @param table 表名
     * @param partitionKey 分区字段名
     * @param saveMode 写入模式
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static void writeHiveDynamicPartition(Dataset<Row> df,
                                                 String database,
                                                 String table,
                                                 String partitionKey,
                                                 SaveMode saveMode) {
        Objects.requireNonNull(df, "Dataset不能为空");
        Objects.requireNonNull(database, "数据库名不能为空");
        Objects.requireNonNull(table, "表名不能为空");
        Objects.requireNonNull(partitionKey, "分区字段名不能为空");
        Objects.requireNonNull(saveMode, "写入模式不能为空");

        SparkSession spark = df.sparkSession();
        initHiveConfig(spark);
        String tableName = database + "." + table;
        log.info("执行Hive动态分区写入-单分区字段，表: {}，分区字段: {}，写入模式: {}", tableName, partitionKey, saveMode);
        df.write()
                .mode(saveMode)
                .partitionBy(partitionKey)
                .saveAsTable(tableName);
    }

    /**
     * Hive动态分区写入（多分区字段）
     * 
     * <p>修改原因：新增多分区字段写入方法</p>
     *
     * @param df Dataset数据集
     * @param database 数据库名
     * @param table 表名
     * @param partitionKeys 分区字段名数组
     * @param saveMode 写入模式
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static void writeHiveDynamicPartition(Dataset<Row> df,
                                                 String database,
                                                 String table,
                                                 String[] partitionKeys,
                                                 SaveMode saveMode) {
        Objects.requireNonNull(df, "Dataset不能为空");
        Objects.requireNonNull(database, "数据库名不能为空");
        Objects.requireNonNull(table, "表名不能为空");
        Objects.requireNonNull(partitionKeys, "分区字段名数组不能为空");
        Objects.requireNonNull(saveMode, "写入模式不能为空");

        SparkSession spark = df.sparkSession();
        initHiveConfig(spark);
        String tableName = database + "." + table;
        log.info("执行Hive动态分区写入-多分区字段，表: {}，分区字段: {}，写入模式: {}", tableName, partitionKeys, saveMode);
        df.write()
                .mode(saveMode)
                .partitionBy(partitionKeys)
                .saveAsTable(tableName);
    }


    /**
     * 增量追加Hive表
     *
     * @param df Dataset数据集
     * @param database 数据库名
     * @param table 表名
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static void appendHiveTable(Dataset<Row> df, String database, String table) {
        Objects.requireNonNull(df, "Dataset不能为空");
        Objects.requireNonNull(database, "数据库名不能为空");
        Objects.requireNonNull(table, "表名不能为空");

        String tableName = database + "." + table;
        log.info("执行Hive追加写入，表: {}", tableName);
        df.write().mode(SaveMode.Append).insertInto(tableName);
    }


    /**
     * 写入HDFS Parquet文件（自动合并小文件）
     *
     * @param df Dataset数据集
     * @param hdfsPath HDFS路径
     * @param repartitionNum 重分区数
     * @param saveMode 写入模式
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static void writeParquetToHdfs(Dataset<Row> df,
                                          String hdfsPath,
                                          int repartitionNum,
                                          SaveMode saveMode) {
        Objects.requireNonNull(df, "Dataset不能为空");
        Objects.requireNonNull(hdfsPath, "HDFS路径不能为空");
        Objects.requireNonNull(saveMode, "写入模式不能为空");

        if (repartitionNum <= 0) {
            throw new IllegalArgumentException("重分区数必须大于0");
        }

        df.sparkSession().conf().set(PARQUET_COMPRESS_CODEC, SNAPPY_COMPRESS);
        log.info("写入Parquet文件到HDFS: {}，重分区数: {}，写入模式: {}", hdfsPath, repartitionNum, saveMode);
        df.repartition(repartitionNum)
                .write()
                .mode(saveMode)
                .parquet(hdfsPath);
    }

    /**
     * 写入HDFS ORC文件（自动合并小文件）
     *
     * @param df Dataset数据集
     * @param hdfsPath HDFS路径
     * @param repartitionNum 重分区数
     * @param saveMode 写入模式
     * @throws IllegalArgumentException 参数为空时抛出
     */
    public static void writeOrcToHdfs(Dataset<Row> df,
                                       String hdfsPath,
                                       int repartitionNum,
                                       SaveMode saveMode) {
        Objects.requireNonNull(df, "Dataset不能为空");
        Objects.requireNonNull(hdfsPath, "HDFS路径不能为空");
        Objects.requireNonNull(saveMode, "写入模式不能为空");

        if (repartitionNum <= 0) {
            throw new IllegalArgumentException("重分区数必须大于0");
        }

        df.sparkSession().conf().set(ORC_COMPRESS_CODEC, SNAPPY_COMPRESS);
        log.info("写入ORC文件到HDFS: {}，重分区数: {}，写入模式: {}", hdfsPath, repartitionNum, saveMode);
        df.repartition(repartitionNum)
                .write()
                .mode(saveMode)
                .orc(hdfsPath);
    }

    /**
     * 初始化Hive动态分区配置
     * 
     * <p>修改原因：添加日志记录</p>
     *
     * @param spark SparkSession对象
     */
    private static void initHiveConfig(SparkSession spark) {
        Objects.requireNonNull(spark, "SparkSession不能为空");

        spark.conf().set(HIVE_DYNAMIC_PARTITION, DYNAMIC_PARTITION_TRUE);
        spark.conf().set(HIVE_DYNAMIC_PARTITION_MODE, DYNAMIC_PARTITION_NONSTRICT);
        spark.conf().set(PARQUET_COMPRESS_CODEC, SNAPPY_COMPRESS);
        spark.conf().set(ORC_COMPRESS_CODEC, SNAPPY_COMPRESS);
        log.info("Hive动态分区配置已初始化，压缩编解码器: {}", SNAPPY_COMPRESS);
    }
}
