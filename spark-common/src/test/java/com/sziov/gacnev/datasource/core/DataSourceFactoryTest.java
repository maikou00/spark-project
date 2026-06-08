package com.sziov.gacnev.datasource.core;

import com.sziov.gacnev.datasource.clickhouse.ClickHouseConfig;
import com.sziov.gacnev.datasource.elasticsearch.ElasticsearchConfig;
import com.sziov.gacnev.datasource.kafka.KafkaConfig;
import com.sziov.gacnev.datasource.mongodb.MongoConfig;
import com.sziov.gacnev.datasource.redis.RedisConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DataSourceFactory} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("DataSourceFactory 工厂测试")
class DataSourceFactoryTest {

    @Test
    @DisplayName("createSource_ES类型_返回非空实例")
    void createSource_elasticsearchType_returnsNonNull() {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        cfg.setHosts("localhost");
        DataSource source = DataSourceFactory.createSource(DataSourceType.ELASTICSEARCH, cfg);
        assertThat(source).isNotNull();
    }

    @Test
    @DisplayName("createSink_ES类型_返回非空实例")
    void createSink_elasticsearchType_returnsNonNull() {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        cfg.setHosts("localhost");
        DataSink sink = DataSourceFactory.createSink(DataSourceType.ELASTICSEARCH, cfg);
        assertThat(sink).isNotNull();
    }

    @Test
    @DisplayName("createSource_所有类型_全部非空")
    void createSource_allTypes_allNonNull() {
        for (DataSourceType type : DataSourceType.values()) {
            DataSource source = DataSourceFactory.createSource(type, configFor(type));
            assertThat(source).as("createSource for " + type).isNotNull();
        }
    }

    @Test
    @DisplayName("createSink_所有类型_全部非空")
    void createSink_allTypes_allNonNull() {
        for (DataSourceType type : DataSourceType.values()) {
            DataSink sink = DataSourceFactory.createSink(type, configFor(type));
            assertThat(sink).as("createSink for " + type).isNotNull();
        }
    }

    @Test
    @DisplayName("readStream_ES类型_抛出UnsupportedOperationException")
    void readStream_elasticsearch_throwsUnsupportedOperation() {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        DataSource source = DataSourceFactory.createSource(DataSourceType.ELASTICSEARCH, cfg);
        assertThatThrownBy(() -> source.readStream(null, null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("writeStream_ES类型_抛出UnsupportedOperationException")
    void writeStream_elasticsearch_throwsUnsupportedOperation() {
        ElasticsearchConfig cfg = new ElasticsearchConfig();
        DataSink sink = DataSourceFactory.createSink(DataSourceType.ELASTICSEARCH, cfg);
        assertThatThrownBy(() -> sink.writeStream(null, null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private DataSourceConfig configFor(DataSourceType type) {
        switch (type) {
            case ELASTICSEARCH: return new ElasticsearchConfig();
            case CLICKHOUSE: return new ClickHouseConfig();
            case MONGODB: return new MongoConfig();
            case REDIS: return new RedisConfig();
            case KAFKA: return new KafkaConfig();
            default: return new DataSourceConfig();
        }
    }
}
