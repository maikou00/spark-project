package com.sziov.gacnev.constant;

/**
 * 配置key常量
 *
 * @author maikou
 * @date 2026/05/16 01:00
 **/
public final class ParamsKeyConstant {

    private ParamsKeyConstant() {
        throw new UnsupportedOperationException("常量类不允许实例化");
    }


    // ==================== Spark配置参数 ====================
    /**
     * Spark应用名称
     */
    public static final String SPARK_APP_NAME = "spark.app.name";

    /**
     * Spark Master地址
     */
    public static final String SPARK_MASTER = "spark.master";

    /**
     * 是否启用Hive支持
     */
    public static final String SPARK_HIVE_ENABLED = "spark.hive.enabled";

    /**
     * 是否本地模式
     */
    public static final String SPARK_LOCAL = "spark.local";

    /**
     * Driver内存
     */
    public static final String SPARK_DRIVER_MEMORY = "spark.driver.memory";

    /**
     * Executor内存
     */
    public static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";

    /**
     * Executor核心数
     */
    public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";

    /**
     * 默认并行度
     */
    public static final String SPARK_DEFAULT_PARALLELISM = "spark.default.parallelism";

    /**
     * Shuffle分区数
     */
    public static final String SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";

    /**
     * 序列化器
     */
    public static final String SPARK_SERIALIZER = "spark.serializer";

    /**
     * Kryo注册类
     */
    public static final String SPARK_KRYO_REGISTRATOR = "spark.kryo.registrator";

    /**
     * 网络超时时间
     */
    public static final String SPARK_NETWORK_TIMEOUT = "spark.network.timeout";

    /**
     * 动态资源分配
     */
    public static final String SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled";

    /**
     * Executor心跳间隔
     */
    public static final String SPARK_EXECUTOR_HEARTBEAT_INTERVAL = "spark.executor.heartbeatInterval";

    /**
     * 动态资源分配最小Executor数
     */
    public static final String SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = "spark.dynamicAllocation.minExecutors";

    /**
     * 动态资源分配最大Executor数
     */
    public static final String SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = "spark.dynamicAllocation.maxExecutors";

    /**
     * 动态资源分配初始Executor数
     */
    public static final String SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = "spark.dynamicAllocation.initialExecutors";

    /**
     * 动态资源分配Executor空闲超时时间
     */
    public static final String SPARK_DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT = "spark.dynamicAllocation.executorIdleTimeout";

    /**
     * 事件日志是否启用
     */
    public static final String SPARK_EVENTLOG_ENABLED = "spark.eventLog.enabled";

    /**
     * 事件日志目录
     */
    public static final String SPARK_EVENTLOG_DIR = "spark.eventLog.dir";

    /**
     * 日志级别
     */
    public static final String SPARK_LOG_LEVEL = "spark.log.level";


    // ==================== 环境配置参数 ====================
    /**
     * 运行环境类型：local/cluster
     */
    public static final String ENV_TYPE = "env.type";

    /**
     * 配置文件路径参数
     */
    public static final String CONFIG_FILE = "config";

    /**
     * 配置文件路径参数（备用）
     */
    public static final String CONFIG_FILE_ALT = "config-file";


    // ==================== 基础配置参数 ====================
    /**
     * Driver主机地址
     */
    public static final String SPARK_DRIVER_HOST = "spark.driver.host";

    /**
     * Driver端口
     */
    public static final String SPARK_DRIVER_PORT = "spark.driver.port";

    /**
     * UI是否启用
     */
    public static final String SPARK_UI_ENABLED = "spark.ui.enabled";

    /**
     * UI端口
     */
    public static final String SPARK_UI_PORT = "spark.ui.port";


    // ==================== 序列化配置参数 ====================
    /**
     * Kryo缓冲区最大值
     */
    public static final String SPARK_KRYO_BUFFER_MAX = "spark.kryoserializer.buffer.max";

    /**
     * Kryo缓冲区大小
     */
    public static final String SPARK_KRYO_BUFFER = "spark.kryoserializer.buffer";


    // ==================== 内存配置参数 ====================
    /**
     * 内存比例
     */
    public static final String SPARK_MEMORY_FRACTION = "spark.memory.fraction";

    /**
     * 存储内存比例
     */
    public static final String SPARK_MEMORY_STORAGE_FRACTION = "spark.memory.storageFraction";


    // ==================== Shuffle配置参数 ====================
    /**
     * Shuffle压缩
     */
    public static final String SPARK_SHUFFLE_COMPRESS = "spark.shuffle.compress";

    /**
     * Shuffle溢写压缩
     */
    public static final String SPARK_SHUFFLE_SPILL_COMPRESS = "spark.shuffle.spill.compress";

    /**
     * IO压缩编解码器
     */
    public static final String SPARK_IO_COMPRESSION_CODEC = "spark.io.compression.codec";


    // ==================== Hive配置参数 ====================
    /**
     * Spark SQL Catalog实现
     */
    public static final String SPARK_SQL_CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation";

    /**
     * Hive Metastore URIs
     */
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    /**
     * Hive Warehouse路径
     */
    public static final String SPARK_SQL_WAREHOUSE_DIR = "spark.sql.warehouse.dir";


    // ==================== Executor配置参数 ====================
    /**
     * Executor实例数
     */
    public static final String SPARK_EXECUTOR_INSTANCES = "spark.executor.instances";

    /**
     * Driver核心数
     */
    public static final String SPARK_DRIVER_CORES = "spark.driver.cores";

    /**
     * Executor堆外内存
     */
    public static final String SPARK_EXECUTOR_MEMORY_OVERHEAD = "spark.executor.memoryOverhead";

    /**
     * Driver堆外内存
     */
    public static final String SPARK_DRIVER_MEMORY_OVERHEAD = "spark.driver.memoryOverhead";


    // ==================== 性能优化配置参数 ====================
    /**
     * 推测执行
     */
    public static final String SPARK_SPECULATION = "spark.speculation";

    /**
     * 推测执行量化系数
     */
    public static final String SPARK_SPECULATION_QUANTILE = "spark.speculation.quantile";

    /**
     * 任务最大失败次数
     */
    public static final String SPARK_TASK_MAX_FAILURES = "spark.task.maxFailures";

    /**
     * RDD压缩
     */
    public static final String SPARK_RDD_COMPRESS = "spark.rdd.compress";

    /**
     * 广播变量压缩
     */
    public static final String SPARK_BROADCAST_COMPRESS = "spark.broadcast.compress";


    // ==================== 自适应查询执行配置参数（Spark 3.0+） ====================
    /**
     * 自适应查询执行开关
     */
    public static final String SPARK_SQL_ADAPTIVE_ENABLED = "spark.sql.adaptive.enabled";

    /**
     * 自适应合并分区开关
     */
    public static final String SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED = "spark.sql.adaptive.coalescePartitions.enabled";

    /**
     * 自适应合并分区初始分区数
     */
    public static final String SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_INITIAL_PARTITION_NUM = "spark.sql.adaptive.coalescePartitions.initialPartitionNum";


    // ==================== JVM配置参数 ====================
    /**
     * Driver额外JVM选项
     */
    public static final String SPARK_DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";

    /**
     * Executor额外JVM选项
     */
    public static final String SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
}