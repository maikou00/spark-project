package com.sziov.gacnev.datasource.core;

import com.sziov.gacnev.datasource.clickhouse.ClickHouseConfig;
import com.sziov.gacnev.datasource.clickhouse.ClickHouseSink;
import com.sziov.gacnev.datasource.clickhouse.ClickHouseSource;
import com.sziov.gacnev.datasource.elasticsearch.ElasticsearchConfig;
import com.sziov.gacnev.datasource.elasticsearch.ElasticsearchSink;
import com.sziov.gacnev.datasource.elasticsearch.ElasticsearchSource;
import com.sziov.gacnev.datasource.file.*;
import com.sziov.gacnev.datasource.hive.HiveSink;
import com.sziov.gacnev.datasource.hive.HiveSource;
import com.sziov.gacnev.datasource.kafka.KafkaConfig;
import com.sziov.gacnev.datasource.kafka.KafkaSink;
import com.sziov.gacnev.datasource.kafka.KafkaSource;
import com.sziov.gacnev.datasource.mongodb.MongoConfig;
import com.sziov.gacnev.datasource.mongodb.MongoDBSink;
import com.sziov.gacnev.datasource.mongodb.MongoDBSource;
import com.sziov.gacnev.datasource.redis.RedisConfig;
import com.sziov.gacnev.datasource.redis.RedisSink;
import com.sziov.gacnev.datasource.redis.RedisSource;

/**
 * 数据源工厂，统一创建 Source / Sink 实例。
 *
 * @author maikou
 * @since 2026-06-09
 */
public final class DataSourceFactory {

    private DataSourceFactory() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    public static DataSource createSource(DataSourceType type, DataSourceConfig config) {
        switch (type) {
            case HIVE:               return new HiveSource(config);
            case FILE_CSV:           return new CsvSource(config);
            case FILE_JSON:          return new JsonSource(config);
            case FILE_PARQUET:       return new ParquetSource(config);
            case FILE_ORC:           return new OrcSource(config);
            case FILE_TEXT:          return new TextSource(config);
            case CLICKHOUSE:         return new ClickHouseSource(config);
            case ELASTICSEARCH:      return new ElasticsearchSource(config);
            case MONGODB:            return new MongoDBSource(config);
            case REDIS:              return new RedisSource(config);
            case KAFKA:              return new KafkaSource(config);
            default: throw new IllegalArgumentException("不支持的数据源类型: " + type);
        }
    }

    public static DataSink createSink(DataSourceType type, DataSourceConfig config) {
        switch (type) {
            case HIVE:               return new HiveSink(config);
            case FILE_CSV:           return new CsvSink(config);
            case FILE_JSON:          return new JsonSink(config);
            case FILE_PARQUET:       return new ParquetSink(config);
            case FILE_ORC:           return new OrcSink(config);
            case FILE_TEXT:          return new TextSink(config);
            case CLICKHOUSE:         return new ClickHouseSink(config);
            case ELASTICSEARCH:      return new ElasticsearchSink(config);
            case MONGODB:            return new MongoDBSink(config);
            case REDIS:              return new RedisSink(config);
            case KAFKA:              return new KafkaSink(config);
            default: throw new IllegalArgumentException("不支持的数据源类型: " + type);
        }
    }
}
