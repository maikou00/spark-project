package com.sziov.gacnev.datasource.core;

import com.sziov.gacnev.datasource.clickhouse.ClickHouseConfig;
import com.sziov.gacnev.datasource.clickhouse.ClickHouseSink;
import com.sziov.gacnev.datasource.clickhouse.ClickHouseSource;
import com.sziov.gacnev.datasource.elasticsearch.ElasticsearchConfig;
import com.sziov.gacnev.datasource.file.FileConfig;
import com.sziov.gacnev.datasource.hive.HiveConfig;
import com.sziov.gacnev.datasource.hive.HiveSink;
import com.sziov.gacnev.datasource.hive.HiveSource;
import com.sziov.gacnev.datasource.kafka.KafkaConfig;
import com.sziov.gacnev.datasource.mongodb.MongoConfig;
import com.sziov.gacnev.datasource.redis.RedisConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据源统一入口，提供所有数据源类型的工厂方法。
 *
 * <pre>{@code
 *   // 读
 *   Dataset<Row> df = DataSources.hive().read(spark, "orders");
 *   Dataset<Row> df = DataSources.csv().read(spark, "/data/orders.csv");
 *
 *   // 读（带选项）
 *   Dataset<Row> df = DataSources.clickhouse()
 *       .options(o -> o.database("dw").query("SELECT * FROM orders"))
 *       .read(spark, "orders");
 *
 *   // 写
 *   DataSources.hive().write(df, "dw.dws_orders");
 *   DataSources.csv().write(df, "/output/orders");
 * }</pre>
 *
 * @author maikou
 * @since 2026-06-10
 */
public final class DataSources {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final String FORMAT_CSV = "csv";
    private static final String FORMAT_JSON = "json";
    private static final String FORMAT_PARQUET = "parquet";
    private static final String FORMAT_ORC = "orc";
    private static final String FORMAT_TEXT = "text";
    private static final String FORMAT_KAFKA = "kafka";
    private static final String FORMAT_REDIS = "org.apache.spark.sql.redis";
    private static final String FORMAT_MONGODB = "mongodb";
    private static final String FORMAT_ES = "org.elasticsearch.spark.sql";

    static {
        initFileSources();
        initKafkaSource();
        initRedisSource();
        initMongoDBSource();
        initElasticsearchSource();
        initClickHouseSource();
        initHiveSource();
    }

    private static final FileConfig FILE_CONFIG = new FileConfig();

    private DataSources() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    public static DataSourceApi hive() {
        return new DataSourceApi(DataSourceType.HIVE);
    }

    public static DataSourceApi csv() {
        return new DataSourceApi(DataSourceType.CSV);
    }

    public static DataSourceApi json() {
        return new DataSourceApi(DataSourceType.JSON);
    }

    public static DataSourceApi parquet() {
        return new DataSourceApi(DataSourceType.PARQUET);
    }

    public static DataSourceApi orc() {
        return new DataSourceApi(DataSourceType.ORC);
    }

    public static DataSourceApi text() {
        return new DataSourceApi(DataSourceType.TEXT);
    }

    public static DataSourceApi kafka() {
        return new DataSourceApi(DataSourceType.KAFKA);
    }

    public static DataSourceApi redis() {
        return new DataSourceApi(DataSourceType.REDIS);
    }

    public static DataSourceApi mongodb() {
        return new DataSourceApi(DataSourceType.MONGODB);
    }

    public static DataSourceApi elasticsearch() {
        return new DataSourceApi(DataSourceType.ELASTICSEARCH);
    }

    public static DataSourceApi clickhouse() {
        return new DataSourceApi(DataSourceType.CLICKHOUSE);
    }

    // ==================== 注册 ====================

    private static void initFileSources() {
        Map<String, String> emptyOpts = Collections.emptyMap();
        registerFileSource(DataSourceType.CSV, FORMAT_CSV, emptyOpts);
        registerFileSource(DataSourceType.JSON, FORMAT_JSON, emptyOpts);
        registerFileSource(DataSourceType.PARQUET, FORMAT_PARQUET, emptyOpts);
        registerFileSource(DataSourceType.ORC, FORMAT_ORC, emptyOpts);
        registerFileSource(DataSourceType.TEXT, FORMAT_TEXT, emptyOpts);
    }

    private static void registerFileSource(DataSourceType type, String format, Map<String, String> opts) {
        DataSourceRegistry.register(type, FileConfig.class,
                () -> new GenericSource<>(format, FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, DEFAULT_MAX_RETRIES),
                () -> new GenericSink<>(format, FILE_CONFIG, FILE_CONFIG::toSparkOptions, null, DEFAULT_MAX_RETRIES));
    }

    private static void initKafkaSource() {
        KafkaConfig cfg = new KafkaConfig();
        DataSourceRegistry.register(DataSourceType.KAFKA, KafkaConfig.class,
                () -> new GenericSource<>(FORMAT_KAFKA, cfg, cfg::toSparkOptions, "subscribe", cfg.getMaxRetries()),
                () -> new GenericSink<>(FORMAT_KAFKA, cfg, cfg::toSparkOptions, "topic", cfg.getMaxRetries()));
    }

    private static void initRedisSource() {
        RedisConfig cfg = new RedisConfig();
        DataSourceRegistry.register(DataSourceType.REDIS, RedisConfig.class,
                () -> new GenericSource<>(FORMAT_REDIS, cfg, cfg::toSparkOptions, "keys.pattern", cfg.getMaxRetries()),
                () -> new GenericSink<>(FORMAT_REDIS, cfg, cfg::toSparkOptions, null, cfg.getMaxRetries()));
    }

    private static void initMongoDBSource() {
        MongoConfig cfg = new MongoConfig();
        DataSourceRegistry.register(DataSourceType.MONGODB, MongoConfig.class,
                () -> new GenericSource<>(FORMAT_MONGODB, cfg, cfg::toSparkOptions, "spark.mongodb.collection", cfg.getMaxRetries()),
                () -> new GenericSink<>(FORMAT_MONGODB, cfg, cfg::toSparkOptions, "spark.mongodb.collection", cfg.getMaxRetries()));
    }

    private static void initElasticsearchSource() {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        DataSourceRegistry.register(DataSourceType.ELASTICSEARCH, ElasticsearchConfig.class,
                () -> new GenericSource<>(FORMAT_ES, cfg, cfg::toSparkOptions, null, cfg.getMaxRetries()),
                () -> new GenericSink<>(FORMAT_ES, cfg, cfg::toSparkOptions, null, cfg.getMaxRetries()));
    }

    private static void initClickHouseSource() {
        ClickHouseConfig cfg = new ClickHouseConfig();
        DataSourceRegistry.register(DataSourceType.CLICKHOUSE, ClickHouseConfig.class,
                () -> new ClickHouseSource(cfg),
                () -> new ClickHouseSink(cfg));
    }

    private static void initHiveSource() {
        HiveConfig cfg = new HiveConfig();
        DataSourceRegistry.register(DataSourceType.HIVE, HiveConfig.class,
                () -> new HiveSource(cfg),
                () -> new HiveSink(cfg));
    }
}
