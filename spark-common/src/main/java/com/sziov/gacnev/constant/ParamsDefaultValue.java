package com.sziov.gacnev.constant;

/**
 * 配置参数默认值常量类
 * 定义所有Spark配置参数的默认值
 * 
 * @author maikou
 * @date 2026-05-16
 */
public final class ParamsDefaultValue {

    /**
     * 私有构造函数，防止实例化
     */
    private ParamsDefaultValue() {
        throw new UnsupportedOperationException("常量类不允许实例化");
    }


    // ==================== Spark基础配置默认值 ====================
    /**
     * 默认应用名称
     */
    public static final String SPARK_APP_NAME = "SparkApp";

    /**
     * 默认本地模式
     */
    public static final boolean SPARK_LOCAL = true;

    /**
     * 默认启用Hive
     */
    public static final boolean SPARK_HIVE_ENABLED = false;

    /**
     * 默认Driver主机
     */
    public static final String SPARK_DRIVER_HOST = "localhost";

    /**
     * 默认Driver端口
     */
    public static final int SPARK_DRIVER_PORT = 0;

    /**
     * 默认启用UI
     */
    public static final boolean SPARK_UI_ENABLED = true;

    /**
     * 默认UI端口
     */
    public static final int SPARK_UI_PORT = 4040;


    // ==================== 序列化配置默认值 ====================
    /**
     * 默认使用kryo序列化器
     */
    public static final String SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
    /**
     * 默认Kryo最大缓冲区大小
     */
    public static final String SPARK_KRYO_BUFFER_MAX = "512m";

    /**
     * 默认Kryo缓冲区大小
     */
    public static final String SPARK_KRYO_BUFFER = "64k";


    // ==================== 内存配置默认值 ====================
    /**
     * 默认内存比例
     */
    public static final double SPARK_MEMORY_FRACTION = 0.6;

    /**
     * 默认存储内存比例
     */
    public static final double SPARK_MEMORY_STORAGE_FRACTION = 0.5;


    // ==================== Shuffle配置默认值 ====================
    /**
     * 默认压缩Shuffle输出
     */
    public static final boolean SPARK_SHUFFLE_COMPRESS = true;

    /**
     * 默认压缩Shuffle溢写
     */
    public static final boolean SPARK_SHUFFLE_SPILL_COMPRESS = true;

    /**
     * 默认IO压缩编解码器
     */
    public static final String SPARK_IO_COMPRESSION_CODEC = "snappy";


    // ==================== 网络配置默认值 ====================
    /**
     * 默认网络超时时间
     */
    public static final String SPARK_NETWORK_TIMEOUT = "600s";

    /**
     * 默认Executor心跳间隔（毫秒）
     */
    public static final String SPARK_EXECUTOR_HEARTBEAT_INTERVAL = "10s";


    // ==================== 动态资源分配配置默认值 ====================
    /**
     * 默认启用动态资源分配
     */
    public static final boolean SPARK_DYNAMIC_ALLOCATION_ENABLED = false;

    /**
     * 默认最小Executor数量
     */
    public static final int SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = 1;

    /**
     * 默认最大Executor数量
     */
    public static final int SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = 10;

    /**
     * 默认初始Executor数量
     */
    public static final int SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = 2;

    /**
     * 默认Executor空闲超时时间
     */
    public static final String SPARK_DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT = "60s";


    // ==================== 内存配置默认值（集群/本地） ====================
    /**
     * 默认Driver内存（集群模式）
     */
    public static final String SPARK_DRIVER_MEMORY = "4g";

    /**
     * 默认Driver核心数（集群模式）
     */
    public static final int SPARK_DRIVER_CORES = 2;

    /**
     * 默认Driver堆外内存
     */
    public static final String SPARK_DRIVER_MEMORY_OVERHEAD = "512m";

    /**
     * 默认Executor内存（集群模式）
     */
    public static final String SPARK_EXECUTOR_MEMORY = "6g";

    /**
     * 默认Executor实例数（集群模式）
     */
    public static final int SPARK_EXECUTOR_INSTANCES = 2;

    /**
     * 默认Executor核心数（集群模式）
     */
    public static final int SPARK_EXECUTOR_CORES_INT = 2;

    /**
     * 默认Executor堆外内存
     */
    public static final String SPARK_EXECUTOR_MEMORY_OVERHEAD = "1g";


    // ==================== 并行度配置默认值 ====================
    /**
     * 默认并行度（集群模式）
     */
    public static final String SPARK_DEFAULT_PARALLELISM = "200";

    /**
     * 默认SQL Shuffle分区数（集群模式）
     */
    public static final String SPARK_SQL_SHUFFLE_PARTITIONS = "200";

    /**
     * 默认并行度（本地模式）
     */
    public static final String SPARK_DEFAULT_PARALLELISM_LOCAL = "4";

    /**
     * 默认SQL Shuffle分区数（本地模式）
     */
    public static final String SPARK_SQL_SHUFFLE_PARTITIONS_LOCAL = "4";


    // ==================== Hive配置默认值 ====================
    /**
     * 默认Spark SQL Catalog实现
     */
    public static final String SPARK_SQL_CATALOG_IMPLEMENTATION = "hive";

    /**
     * 默认启用事件日志
     */
    public static final String SPARK_EVENTLOG_ENABLED = "true";

    /**
     * 默认事件日志目录
     */
    public static final String SPARK_EVENTLOG_DIR = "/tmp/spark-events";


    // ==================== 日志配置默认值 ====================
    /**
     * 默认日志级别
     */
    public static final String SPARK_LOG_LEVEL = "WARN";


    // ==================== 性能优化配置默认值 ====================
    /**
     * 默认启用推测执行
     */
    public static final boolean SPARK_SPECULATION = true;

    /**
     * 默认推测执行量化系数
     */
    public static final double SPARK_SPECULATION_QUANTILE = 0.75;

    /**
     * 默认任务最大失败次数
     */
    public static final int SPARK_TASK_MAX_FAILURES = 4;

    /**
     * 默认启用RDD压缩
     */
    public static final boolean SPARK_RDD_COMPRESS = true;

    /**
     * 默认启用广播变量压缩
     */
    public static final boolean SPARK_BROADCAST_COMPRESS = true;


    // ==================== 自适应查询执行配置默认值（Spark 3.0+） ====================
    /**
     * 默认启用自适应查询执行
     */
    public static final boolean SPARK_SQL_ADAPTIVE_ENABLED = true;

    /**
     * 默认启用自适应合并分区
     */
    public static final boolean SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED = true;

    /**
     * 默认自适应合并分区初始分区数
     */
    public static final int SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_INITIAL_PARTITION_NUM = 200;


    // ==================== JVM配置默认值 ====================
    /**
     * 默认Driver额外JVM选项
     */
    public static final String SPARK_DRIVER_EXTRA_JAVA_OPTIONS = "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError";

    /**
     * 默认Executor额外JVM选项
     */
    public static final String SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError";
}