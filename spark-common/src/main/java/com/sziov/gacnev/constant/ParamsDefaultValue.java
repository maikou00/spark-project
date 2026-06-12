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

    // ==================== 数据源通用默认值 ====================
    /** 数据源默认连接地址 */
    public static final String DATASOURCE_DEFAULT_HOSTS = "localhost";
    /** 数据源连接超时（毫秒） */
    public static final int DATASOURCE_DEFAULT_TIMEOUT = 30000;
    /** 数据源默认重试次数 */
    public static final int DATASOURCE_DEFAULT_RETRIES = 3;
    // Elasticsearch
    /** Elasticsearch 默认连接地址 */
    public static final String DATASOURCE_ES_HOSTS = "localhost";
    /** Elasticsearch 默认端口 */
    public static final String DATASOURCE_ES_PORT = "9200";
    /** Elasticsearch 是否自动创建索引 */
    public static final boolean DATASOURCE_ES_INDEX_AUTO_CREATE = true;
    // ClickHouse
    /** ClickHouse 默认连接地址 */
    public static final String DATASOURCE_CK_HOSTS = "localhost:8123";
    /** ClickHouse 默认批量大小 */
    public static final String DATASOURCE_CK_BATCH_SIZE = "10000";
    // MongoDB
    /** MongoDB 默认连接 URI */
    public static final String DATASOURCE_MONGO_URI = "mongodb://localhost:27017";
    // Redis
    /** Redis 默认主机 */
    public static final String DATASOURCE_REDIS_HOST = "localhost";
    /** Redis 默认端口 */
    public static final String DATASOURCE_REDIS_PORT = "6379";
    /** Redis 默认数据库编号 */
    public static final String DATASOURCE_REDIS_DB = "0";
    /** Redis 连接池默认最大连接数 */
    public static final String REDIS_POOL_MAX_TOTAL = "8";
    /** Redis 连接池默认最大空闲连接数 */
    public static final String REDIS_POOL_MAX_IDLE = "8";
    /** Redis 连接池默认最小空闲连接数 */
    public static final String REDIS_POOL_MIN_IDLE = "2";
    /** Redis 连接池默认获取连接最大等待时间（毫秒） */
    public static final String REDIS_POOL_MAX_WAIT_MS = "2000";
    /** Redis 连接池默认借出时是否验证连接 */
    public static final String REDIS_POOL_TEST_ON_BORROW = "true";
    /** Redis SCAN 默认转大规模路径的 key 数阈值 */
    public static final String REDIS_SCAN_THRESHOLD = "10000";
    // Kafka
    /** Kafka 默认 Bootstrap Servers */
    // MySQL
    /** MySQL 默认 JDBC 连接地址 */
    public static final String DATASOURCE_MYSQL_URL = "jdbc:mysql://localhost:3306/spark_test?rewriteBatchedStatements=true\u0026useSSL=false\u0026allowPublicKeyRetrieval=true";
    /** MySQL 默认用户名 */
    public static final String DATASOURCE_MYSQL_USERNAME = "root";
    /** MySQL 默认密码 */
    public static final String DATASOURCE_MYSQL_PASSWORD = "";
    /** MySQL 默认 JDBC 驱动类 */
    public static final String DATASOURCE_MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    /** MySQL 读取默认分区数 */
    public static final int DATASOURCE_MYSQL_NUM_PARTITIONS = 10;
    /** MySQL 写入默认批量大小 */
    public static final int DATASOURCE_MYSQL_BATCH_SIZE = 5000;
    
    /** Doris 默认连接地址 */
    public static final String DATASOURCE_DORIS_URL = "jdbc:mysql://localhost:9030";
    /** Doris 默认用户名 */
    public static final String DATASOURCE_DORIS_USERNAME = "root";
    /** Doris 默认密码 */
    public static final String DATASOURCE_DORIS_PASSWORD = "";
    /** Doris 默认驱动 */
    public static final String DATASOURCE_DORIS_DRIVER = "com.mysql.cj.jdbc.Driver";
    /** Doris 默认 FE HTTP 地址 */
    public static final String DATASOURCE_DORIS_FENODES = "localhost:8030";
    /** Doris 默认批量写入大小 */
    public static final int DATASOURCE_DORIS_BATCH_SIZE = 10000;
    /** Doris 默认 MySQL 协议查询端口 */
    public static final String DATASOURCE_DORIS_QUERY_PORT = "9030";
    /** Doris 默认两阶段提交 */
    public static final boolean DATASOURCE_DORIS_SINK_ENABLE_2PC = true;
    /** Doris 默认 Stream Load 标签前缀 */
    public static final String DATASOURCE_DORIS_SINK_LABEL_PREFIX = "spark_doris";
    /** Doris 默认 Stream Load 最大重试次数 */
    public static final int DATASOURCE_DORIS_SINK_MAX_RETRIES = 3;
    /** Doris 默认写入批大小 */
    public static final int DATASOURCE_DORIS_SINK_BATCH_SIZE = 10000;
    /** Doris 默认写入批间隔（毫秒） */
    public static final int DATASOURCE_DORIS_SINK_BATCH_INTERVAL_MS = 3000;
    /** Doris 默认攒批模式 */
    public static final String DATASOURCE_DORIS_SINK_GROUP_COMMIT = "sync_mode";
    /** Doris 默认 BE 请求重试次数 */
    public static final int DATASOURCE_DORIS_REQUEST_RETRIES = 3;
    /** Doris 默认 BE 连接超时（毫秒） */
    public static final int DATASOURCE_DORIS_REQUEST_CONNECT_TIMEOUT_MS = 30000;
    /** Doris 默认 BE 读取超时（毫秒） */
    public static final int DATASOURCE_DORIS_REQUEST_READ_TIMEOUT_MS = 60000;

    public static final String DATASOURCE_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    /** Kafka 默认消费者组 ID */
    public static final String DATASOURCE_KAFKA_GROUP_ID = "spark-datasource-group";
}