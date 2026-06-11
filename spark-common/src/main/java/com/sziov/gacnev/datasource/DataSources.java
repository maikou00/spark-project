package com.sziov.gacnev.datasource;

import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.impl.FileSink;
import com.sziov.gacnev.datasource.impl.FileSource;
import com.sziov.gacnev.datasource.option.ClickHouseOption;
import com.sziov.gacnev.datasource.option.FileOption;
import com.sziov.gacnev.datasource.option.HiveOption;
import com.sziov.gacnev.datasource.option.RedisOption;
import com.sziov.gacnev.datasource.option.MySqlOption;
import com.sziov.gacnev.spark.SparkParameterTool;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据源统一入口，提供所有数据源类型的工厂方法。
 * 通过 {@link #init(Properties)} 从配置文件加载连接信息并校验必填项。
 * 内置类型（文件格式：CSV、JSON、Parquet、ORC、Text）静态注册，
 * 扩展类型（Hive、ClickHouse、Redis）通过 {@link ServiceLoader} 自动发现。
 *
 * <pre>{@code
 *   DataSources.hive().option(o -> o.database("ods").partitionFilter("dt='2026-06-10'")).read(spark, "orders");
 *   DataSources.csv().option(o -> o.writeMode(SaveMode.Overwrite)).write(df, "/data/orders.csv");
 *   DataSources.redis().option(o -> o.keyColumn("id").setRedisModel(RedisModel.HASH)).read(spark, "users");
 * }</pre>
 *
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public final class DataSources {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final String FORMAT_CSV = "csv";
    private static final String FORMAT_JSON = "json";
    private static final String FORMAT_PARQUET = "parquet";
    private static final String FORMAT_ORC = "orc";
    private static final String FORMAT_TEXT = "text";

    private static volatile Properties dsConfig;
    private static final Map<DataSourceType, DataSource<?>> SOURCES = new ConcurrentHashMap<>();
    private static final Map<DataSourceType, DataSink<?>> SINKS = new ConcurrentHashMap<>();

    static {
        initBuiltinSources();
        discoverProviders();
    }

    private DataSources() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 初始化数据源配置。
     * 校验各数据源必填配置项，失败抛 {@link WarehouseException}。
     *
     * @param props 合并后的配置
     */
    public static void init(Properties props) {
        if (props == null) {
            props = new Properties();
        }
        dsConfig = props;
        log.info("DataSources 配置已加载，共 {} 项", dsConfig.size());
    }

    static void ensureInitialized() {
        if (dsConfig == null) {
            log.info("DataSources 未显式初始化，从 classpath:app.properties 加载");
            Properties fallback = SparkParameterTool.fromClasspath("app.properties");
            init(fallback == null ? new Properties() : fallback);
        }
    }

    static void requireConfig(DataSourceType type) {
        String configKey = getRequiredConfigKey(type);
        if (configKey == null) {
            return;
        }
        String value = SparkParameterTool.get(dsConfig, configKey, null);
        if (value == null || value.isEmpty()) {
            throw new WarehouseException(
                    "数据源 [" + type + "] 未配置 " + configKey + "，请在 app.properties 中设置");
        }
    }

    private static String getRequiredConfigKey(DataSourceType type) {
        switch (type) {
            case MYSQL:      return ParamsKeyConstant.DATASOURCE_MYSQL_URL;
            case REDIS:      return ParamsKeyConstant.DATASOURCE_REDIS_HOST;
            case CLICKHOUSE: return ParamsKeyConstant.DATASOURCE_CK_HOSTS;
            default:         return null;
        }
    }

    // ==================== 工厂方法 ====================

    /** Hive 数据源 */
    public static DataSourceApi<HiveOption> hive() {
        return new DataSourceApi<>(DataSourceType.HIVE, new HiveOption());
    }

    /** ClickHouse 数据源 */
    public static DataSourceApi<ClickHouseOption> clickhouse() {
        return new DataSourceApi<>(DataSourceType.CLICKHOUSE, new ClickHouseOption());
    }

    /** Redis 数据源 */
    public static DataSourceApi<RedisOption> redis() {
        return new DataSourceApi<>(DataSourceType.REDIS, new RedisOption());
    }

    /** MySQL 数据源 */
    public static DataSourceApi<MySqlOption> mysql() {
        return new DataSourceApi<>(DataSourceType.MYSQL, new MySqlOption());
    }

    /** CSV 文件数据源 */
    public static DataSourceApi<FileOption> csv() {
        return new DataSourceApi<>(DataSourceType.CSV, new FileOption());
    }

    /** JSON 文件数据源 */
    public static DataSourceApi<FileOption> json() {
        return new DataSourceApi<>(DataSourceType.JSON, new FileOption());
    }

    /** Parquet 文件数据源 */
    public static DataSourceApi<FileOption> parquet() {
        return new DataSourceApi<>(DataSourceType.PARQUET, new FileOption());
    }

    /** ORC 文件数据源 */
    public static DataSourceApi<FileOption> orc() {
        return new DataSourceApi<>(DataSourceType.ORC, new FileOption());
    }

    /** Text 文件数据源 */
    public static DataSourceApi<FileOption> text() {
        return new DataSourceApi<>(DataSourceType.TEXT, new FileOption());
    }

    static DataSource<?> getSource(DataSourceType type) {
        requireConfig(type);
        DataSource<?> src = SOURCES.get(type);
        if (src == null) {
            throw new WarehouseException("未注册的数据源类型: " + type);
        }
        return src;
    }

    static DataSink<?> getSink(DataSourceType type) {
        requireConfig(type);
        DataSink<?> sink = SINKS.get(type);
        if (sink == null) {
            throw new WarehouseException("未注册的数据源类型: " + type);
        }
        return sink;
    }

    public static Properties getDsConfig() {
        ensureInitialized();
        return dsConfig;
    }

    // ==================== 内置数据源静态注册 ====================

    private static void initBuiltinSources() {
        SOURCES.put(DataSourceType.CSV, new FileSource(FORMAT_CSV, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SINKS.put(DataSourceType.CSV, new FileSink(FORMAT_CSV, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SOURCES.put(DataSourceType.JSON, new FileSource(FORMAT_JSON, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SINKS.put(DataSourceType.JSON, new FileSink(FORMAT_JSON, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SOURCES.put(DataSourceType.PARQUET, new FileSource(FORMAT_PARQUET, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SINKS.put(DataSourceType.PARQUET, new FileSink(FORMAT_PARQUET, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SOURCES.put(DataSourceType.ORC, new FileSource(FORMAT_ORC, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SINKS.put(DataSourceType.ORC, new FileSink(FORMAT_ORC, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SOURCES.put(DataSourceType.TEXT, new FileSource(FORMAT_TEXT, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
        SINKS.put(DataSourceType.TEXT, new FileSink(FORMAT_TEXT, Collections.emptyMap(), null, DEFAULT_MAX_RETRIES));
    }

    // ==================== SPI 自动发现 ====================

    private static void discoverProviders() {
        ServiceLoader<DataSourceProvider> loader = ServiceLoader.load(DataSourceProvider.class);
        for (DataSourceProvider provider : loader) {
            DataSourceType type = provider.type();
            SOURCES.put(type, provider.createSource());
            SINKS.put(type, provider.createSink());
            log.info("SPI 注册数据源: {}", type);
        }
    }
}
