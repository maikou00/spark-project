package com.sziov.gacnev.utils;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

import java.util.Objects;
import java.util.Properties;

/**
 * Spark 环境工具类，负责 {@link SparkSession} 和 {@link JavaSparkContext} 的创建及统一配置管理。
 *
 * <p><b>配置加载优先级</b>（高 → 低）：
 * <ol>
 *   <li>命令行参数 ({@code --key value})</li>
 *   <li>外部配置文件 ({@code --config /path/to/app.properties})</li>
 *   <li>classpath 默认配置 ({@code app.properties})</li>
 *   <li>代码硬编码默认值</li>
 * </ol>
 *
 * @author maikou
 * @since 2026-05-16
 */
@Slf4j
public final class SparkEnvUtils {

    private SparkEnvUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    // ==================== Public API ====================

    /**
     * 初始化 {@link SparkSession}。
     *
     * @param args           命令行参数
     * @param defaultAppName 默认应用名称
     * @return SparkSession 实例
     * @throws IllegalArgumentException 如果参数为 null
     */
    public static SparkSession prepare(String[] args, String defaultAppName) {
        Objects.requireNonNull(args, "args must not be null");

        Properties mergedProps = loadAndMergeConfig(args);

        String appName = resolveAppName(mergedProps, defaultAppName);
        boolean isLocal = SparkParameterTool.getBoolean(mergedProps,
                ParamsKeyConstant.SPARK_LOCAL, ParamsDefaultValue.SPARK_LOCAL);
        boolean enableHive = SparkParameterTool.getBoolean(mergedProps,
                ParamsKeyConstant.SPARK_HIVE_ENABLED, ParamsDefaultValue.SPARK_HIVE_ENABLED);

        log.info("初始化 SparkSession - appName: {}, isLocal: {}, enableHive: {}", appName, isLocal, enableHive);

        SparkSession sparkSession = createSparkSession(appName, isLocal, enableHive, mergedProps);
        setGlobalJobParameters(sparkSession, mergedProps);
        setLogLevel(sparkSession, mergedProps);

        // 打印配置信息
        logConfigInfo(sparkSession);

        return sparkSession;
    }

    /**
     * 使用默认应用名初始化 {@link SparkSession}。
     *
     * @param args 命令行参数
     * @return SparkSession 实例
     */
    public static SparkSession prepare(String[] args) {
        return prepare(args, null);
    }

    /**
     * 初始化 {@link JavaSparkContext}。
     *
     * @param args           命令行参数
     * @param defaultAppName 默认应用名称
     * @return JavaSparkContext 实例
     */
    public static JavaSparkContext prepareContext(String[] args, String defaultAppName) {
        Objects.requireNonNull(args, "args must not be null");

        Properties mergedProps = loadAndMergeConfig(args);

        String appName = resolveAppName(mergedProps, defaultAppName);
        boolean isLocal = SparkParameterTool.getBoolean(mergedProps,
                ParamsKeyConstant.SPARK_LOCAL, ParamsDefaultValue.SPARK_LOCAL);
        boolean enableHive = SparkParameterTool.getBoolean(mergedProps,
                ParamsKeyConstant.SPARK_HIVE_ENABLED, ParamsDefaultValue.SPARK_HIVE_ENABLED);

        log.info("初始化 JavaSparkContext - appName: {}, isLocal: {}, enableHive: {}", appName, isLocal, enableHive);

        JavaSparkContext jsc = createJavaSparkContext(appName, isLocal, enableHive, mergedProps);
        setGlobalJobParameters(jsc, mergedProps);

        return jsc;
    }

    /**
     * 使用默认应用名初始化 {@link JavaSparkContext}。
     *
     * @param args 命令行参数
     * @return JavaSparkContext 实例
     */
    public static JavaSparkContext prepareContext(String[] args) {
        return prepareContext(args, null);
    }

    /**
     * 获取所有生效的配置并打印日志。
     *
     * @param spark SparkSession 实例
     */
    public static void getAllConfigMsg(SparkSession spark) {
        Objects.requireNonNull(spark, "spark must not be null");
        log.info("========== Spark 配置信息（只读） ==========");
        scala.collection.immutable.Map<String, String> confMap = spark.conf().getAll();
        Iterator<String> iter = confMap.keys().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            String value = confMap.get(key).get();
            log.info("  {} = {}", key, maskSensitiveConfig(key, value));
        }
        log.info("==========================================");
    }

    // ==================== Internal: 配置加载 ====================

    /**
     * 加载并合并配置（文件配置 + 命令行参数）。
     */
    private static Properties loadAndMergeConfig(String[] args) {
        Properties argsProps = SparkParameterTool.fromArgs(args);
        String configFilePath = SparkParameterTool.getConfigFilePath(args);
        Properties fileProps;

        log.info("配置文件路径：{}", configFilePath != null ? configFilePath : "classpath:app.properties");

        if (configFilePath != null) {
            fileProps = SparkParameterTool.fromPropertiesFile(configFilePath);
        } else {
            fileProps = SparkParameterTool.fromClasspath("app.properties");
        }

        return SparkParameterTool.merge(fileProps, argsProps);
    }

    /**
     * 解析应用名称。
     */
    private static String resolveAppName(Properties props, String defaultAppName) {
        return SparkParameterTool.get(props, ParamsKeyConstant.SPARK_APP_NAME,
                defaultAppName != null ? defaultAppName : ParamsDefaultValue.SPARK_APP_NAME);
    }

    // ==================== Internal: SparkSession 创建 ====================

    /**
     * 创建并配置 {@link SparkSession}。
     */
    private static SparkSession createSparkSession(String appName, boolean isLocal,
                                                    boolean enableHive, Properties properties) {
        SparkConf conf = new SparkConf();

        // 基础配置
        conf.setAppName(appName);
        if (isLocal) {
            conf.setMaster("local[*]");
            setLocalConfig(conf, properties);
        } else {
            setClusterConfig(conf, properties);
        }

        // 通用配置
        setSerializerConfig(conf, properties);
        setMemoryConfig(conf, properties);
        setShuffleConfig(conf, properties);
        setNetworkConfig(conf, properties);
        setDynamicAllocationConfig(conf, properties);
        setHiveConfig(conf, properties);
        setPerformanceConfig(conf, properties);
        setAdaptiveQueryConfig(conf, properties);
        setJvmConfig(conf, properties);

        // 构建 SparkSession
        SparkSession.Builder builder = SparkSession.builder()
                .config(conf)
                .config("spark.sql.session.timeZone", "Asia/Shanghai");

        if (enableHive) {
            builder.enableHiveSupport();
        }

        return builder.getOrCreate();
    }

    /**
     * 创建并配置 {@link JavaSparkContext}。
     */
    private static JavaSparkContext createJavaSparkContext(String appName, boolean isLocal,
                                                           boolean enableHive, Properties properties) {
        SparkSession spark = createSparkSession(appName, isLocal, enableHive, properties);
        return JavaSparkContext.fromSparkContext(spark.sparkContext());
    }

    // ==================== Internal: 配置项设置 ====================

    private static void setLocalConfig(SparkConf conf, Properties props) {
        conf.set("spark.default.parallelism",
                SparkParameterTool.get(props, ParamsKeyConstant.SPARK_DEFAULT_PARALLELISM,
                        ParamsDefaultValue.SPARK_DEFAULT_PARALLELISM_LOCAL));
        conf.set("spark.sql.shuffle.partitions",
                SparkParameterTool.get(props, ParamsKeyConstant.SPARK_SQL_SHUFFLE_PARTITIONS,
                        ParamsDefaultValue.SPARK_SQL_SHUFFLE_PARTITIONS_LOCAL));
        conf.set("spark.ui.enabled", "false");
        log.warn("本地模式配置已应用，并行度={}, Shuffle分区数={}",
                ParamsDefaultValue.SPARK_DEFAULT_PARALLELISM_LOCAL,
                ParamsDefaultValue.SPARK_SQL_SHUFFLE_PARTITIONS_LOCAL);
    }

    private static void setClusterConfig(SparkConf conf, Properties props) {
        String master = SparkParameterTool.get(props, ParamsKeyConstant.SPARK_MASTER, "yarn");
        conf.set("spark.master", master);
        conf.set("spark.default.parallelism",
                SparkParameterTool.get(props, ParamsKeyConstant.SPARK_DEFAULT_PARALLELISM,
                        ParamsDefaultValue.SPARK_DEFAULT_PARALLELISM));
        conf.set("spark.sql.shuffle.partitions",
                SparkParameterTool.get(props, ParamsKeyConstant.SPARK_SQL_SHUFFLE_PARTITIONS,
                        ParamsDefaultValue.SPARK_SQL_SHUFFLE_PARTITIONS));

        // Driver 配置
        conf.set("spark.driver.memory",
                SparkParameterTool.get(props, ParamsKeyConstant.SPARK_DRIVER_MEMORY,
                        ParamsDefaultValue.SPARK_DRIVER_MEMORY));
        conf.set("spark.driver.cores",
                String.valueOf(SparkParameterTool.getInt(props, ParamsKeyConstant.SPARK_DRIVER_CORES,
                        ParamsDefaultValue.SPARK_DRIVER_CORES)));

        // Executor 配置
        conf.set("spark.executor.memory",
                SparkParameterTool.get(props, ParamsKeyConstant.SPARK_EXECUTOR_MEMORY,
                        ParamsDefaultValue.SPARK_EXECUTOR_MEMORY));
        conf.set("spark.executor.cores",
                String.valueOf(SparkParameterTool.getInt(props, ParamsKeyConstant.SPARK_EXECUTOR_CORES,
                        ParamsDefaultValue.SPARK_EXECUTOR_CORES_INT)));
        conf.set("spark.executor.instances",
                String.valueOf(SparkParameterTool.getInt(props, ParamsKeyConstant.SPARK_EXECUTOR_INSTANCES,
                        ParamsDefaultValue.SPARK_EXECUTOR_INSTANCES)));

        log.warn("集群模式配置已应用, master={}", master);
    }

    private static void setSerializerConfig(SparkConf conf, Properties properties) {
        String serializer = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_SERIALIZER, ParamsDefaultValue.SPARK_SERIALIZER);
        String kryoBufferMax = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_KRYO_BUFFER_MAX, ParamsDefaultValue.SPARK_KRYO_BUFFER_MAX);
        String kryoBuffer = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_KRYO_BUFFER, ParamsDefaultValue.SPARK_KRYO_BUFFER);

        conf.set("spark.serializer", serializer);
        conf.set("spark.kryoserializer.buffer.max", kryoBufferMax);
        conf.set("spark.kryoserializer.buffer", kryoBuffer);

        log.debug("序列化配置: serializer={}", serializer);
    }

    private static void setMemoryConfig(SparkConf conf, Properties properties) {
        double fraction = SparkParameterTool.getDouble(properties,
                ParamsKeyConstant.SPARK_MEMORY_FRACTION, ParamsDefaultValue.SPARK_MEMORY_FRACTION);
        double storageFraction = SparkParameterTool.getDouble(properties,
                ParamsKeyConstant.SPARK_MEMORY_STORAGE_FRACTION, ParamsDefaultValue.SPARK_MEMORY_STORAGE_FRACTION);

        conf.set("spark.memory.fraction", String.valueOf(fraction));
        conf.set("spark.memory.storageFraction", String.valueOf(storageFraction));

        log.debug("内存配置: fraction={}, storageFraction={}", fraction, storageFraction);
    }

    private static void setShuffleConfig(SparkConf conf, Properties properties) {
        boolean shuffleCompress = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_SHUFFLE_COMPRESS, ParamsDefaultValue.SPARK_SHUFFLE_COMPRESS);
        boolean spillCompress = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_SHUFFLE_SPILL_COMPRESS, ParamsDefaultValue.SPARK_SHUFFLE_SPILL_COMPRESS);
        String ioCodec = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_IO_COMPRESSION_CODEC, ParamsDefaultValue.SPARK_IO_COMPRESSION_CODEC);

        conf.set("spark.shuffle.compress", String.valueOf(shuffleCompress));
        conf.set("spark.shuffle.spill.compress", String.valueOf(spillCompress));
        conf.set("spark.io.compression.codec", ioCodec);
    }

    private static void setNetworkConfig(SparkConf conf, Properties properties) {
        String timeout = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_NETWORK_TIMEOUT, ParamsDefaultValue.SPARK_NETWORK_TIMEOUT);
        String heartbeatInterval = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_EXECUTOR_HEARTBEAT_INTERVAL, ParamsDefaultValue.SPARK_EXECUTOR_HEARTBEAT_INTERVAL);

        conf.set("spark.network.timeout", timeout);
        conf.set("spark.executor.heartbeatInterval", heartbeatInterval);
    }

    private static void setDynamicAllocationConfig(SparkConf conf, Properties properties) {
        boolean enabled = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_DYNAMIC_ALLOCATION_ENABLED, ParamsDefaultValue.SPARK_DYNAMIC_ALLOCATION_ENABLED);
        if (!enabled) {
            conf.set("spark.dynamicAllocation.enabled", "false");
            return;
        }

        conf.set("spark.dynamicAllocation.enabled", "true");
        conf.set("spark.dynamicAllocation.minExecutors",
                String.valueOf(SparkParameterTool.getInt(properties,
                        ParamsKeyConstant.SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS,
                        ParamsDefaultValue.SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS)));
        conf.set("spark.dynamicAllocation.maxExecutors",
                String.valueOf(SparkParameterTool.getInt(properties,
                        ParamsKeyConstant.SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS,
                        ParamsDefaultValue.SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS)));
        conf.set("spark.dynamicAllocation.initialExecutors",
                String.valueOf(SparkParameterTool.getInt(properties,
                        ParamsKeyConstant.SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS,
                        ParamsDefaultValue.SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS)));
        conf.set("spark.dynamicAllocation.executorIdleTimeout",
                SparkParameterTool.get(properties,
                        ParamsKeyConstant.SPARK_DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT,
                        ParamsDefaultValue.SPARK_DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT));
    }

    private static void setHiveConfig(SparkConf conf, Properties properties) {
        boolean enableHive = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_HIVE_ENABLED, ParamsDefaultValue.SPARK_HIVE_ENABLED);
        if (!enableHive) {
            return;
        }

        String warehouseDir = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_SQL_WAREHOUSE_DIR, "/user/hive/warehouse");
        String catalogImpl = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_SQL_CATALOG_IMPLEMENTATION, ParamsDefaultValue.SPARK_SQL_CATALOG_IMPLEMENTATION);
        String metastoreUris = properties.getProperty(ParamsKeyConstant.HIVE_METASTORE_URIS);

        conf.set("spark.sql.warehouse.dir", warehouseDir);
        conf.set("spark.sql.catalogImplementation", catalogImpl);
        if (metastoreUris != null) {
            conf.set("hive.metastore.uris", metastoreUris);
            log.info("Hive Metastore 地址: {}", metastoreUris);
        }

        log.info("Hive 配置已加载: warehouseDir={}, catalogImpl={}", warehouseDir, catalogImpl);
    }

    private static void setPerformanceConfig(SparkConf conf, Properties properties) {
        boolean speculation = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_SPECULATION, ParamsDefaultValue.SPARK_SPECULATION);
        double speculationQuantile = SparkParameterTool.getDouble(properties,
                ParamsKeyConstant.SPARK_SPECULATION_QUANTILE, ParamsDefaultValue.SPARK_SPECULATION_QUANTILE);
        int taskMaxFailures = SparkParameterTool.getInt(properties,
                ParamsKeyConstant.SPARK_TASK_MAX_FAILURES, ParamsDefaultValue.SPARK_TASK_MAX_FAILURES);
        boolean rddCompress = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_RDD_COMPRESS, ParamsDefaultValue.SPARK_RDD_COMPRESS);
        boolean broadcastCompress = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_BROADCAST_COMPRESS, ParamsDefaultValue.SPARK_BROADCAST_COMPRESS);

        conf.set("spark.speculation", String.valueOf(speculation));
        conf.set("spark.speculation.quantile", String.valueOf(speculationQuantile));
        conf.set("spark.task.maxFailures", String.valueOf(taskMaxFailures));
        conf.set("spark.rdd.compress", String.valueOf(rddCompress));
        conf.set("spark.broadcast.compress", String.valueOf(broadcastCompress));

        log.debug("性能配置: speculation={}, taskMaxFailures={}", speculation, taskMaxFailures);
    }

    private static void setAdaptiveQueryConfig(SparkConf conf, Properties properties) {
        boolean adaptiveEnabled = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_SQL_ADAPTIVE_ENABLED, ParamsDefaultValue.SPARK_SQL_ADAPTIVE_ENABLED);
        boolean coalesceEnabled = SparkParameterTool.getBoolean(properties,
                ParamsKeyConstant.SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED,
                ParamsDefaultValue.SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED);
        int initialPartitionNum = SparkParameterTool.getInt(properties,
                ParamsKeyConstant.SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_INITIAL_PARTITION_NUM,
                ParamsDefaultValue.SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_INITIAL_PARTITION_NUM);

        conf.set("spark.sql.adaptive.enabled", String.valueOf(adaptiveEnabled));
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", String.valueOf(coalesceEnabled));
        conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", String.valueOf(initialPartitionNum));

        log.debug("AQE 配置: enabled={}, coalesce={}, initialPartitions={}",
                adaptiveEnabled, coalesceEnabled, initialPartitionNum);
    }

    private static void setJvmConfig(SparkConf conf, Properties properties) {
        String driverJavaOptions = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_DRIVER_EXTRA_JAVA_OPTIONS, "");
        String executorJavaOptions = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS, "");

        if (driverJavaOptions != null && !driverJavaOptions.isEmpty()) {
            conf.set("spark.driver.extraJavaOptions", driverJavaOptions);
        }
        if (executorJavaOptions != null && !executorJavaOptions.isEmpty()) {
            conf.set("spark.executor.extraJavaOptions", executorJavaOptions);
        }

        log.debug("JVM 配置已加载");
    }

    // ==================== Internal: 其他 ====================

    private static void setLogLevel(SparkSession sparkSession, Properties properties) {
        String logLevel = SparkParameterTool.get(properties,
                ParamsKeyConstant.SPARK_LOG_LEVEL, ParamsDefaultValue.SPARK_LOG_LEVEL);
        sparkSession.sparkContext().setLogLevel(logLevel);
        log.info("Spark 日志级别已设置为: {}", logLevel);
    }

    private static void setGlobalJobParameters(SparkSession sparkSession, Properties properties) {
        sparkSession.sparkContext().setLocalProperties(properties);
    }

    private static void setGlobalJobParameters(JavaSparkContext jsc, Properties properties) {
        jsc.sc().setLocalProperties(properties);
    }

    /**
     * 日志打印生效的配置（对敏感信息脱敏）。
     */
    private static void logConfigInfo(SparkSession spark) {
        log.info("========== Spark 生效配置 ==========");
        Map<String, String> confMap = spark.conf().getAll();
        Iterator<String> iter = confMap.keys().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            String value = confMap.get(key).get();
            log.info("  {} = {}", key, maskSensitiveConfig(key, value));
        }
        log.info("===================================");
    }

    /**
     * 对敏感配置项值进行脱敏处理（如密码、密钥等）。
     */
    private static String maskSensitiveConfig(String key, String value) {
        if (value == null) {
            return null;
        }
        String lowerKey = key.toLowerCase();
        if (lowerKey.contains("password") || lowerKey.contains("secret")
                || lowerKey.contains("token") || lowerKey.contains("credential")) {
            if (value.length() <= 4) {
                return "****";
            }
            return value.substring(0, 2) + "****" + value.substring(value.length() - 2);
        }
        return value;
    }
}
