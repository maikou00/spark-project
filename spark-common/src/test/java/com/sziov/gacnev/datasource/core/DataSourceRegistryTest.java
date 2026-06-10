package com.sziov.gacnev.datasource.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DataSourceRegistry} 测试用例。
 *
 * @author maikou
 * @since 2026-06-10
 */
@DisplayName("DataSourceRegistry 注册中心测试")
class DataSourceRegistryTest {

    @BeforeAll
    static void initRegistry() {
        DataSources.hive();
    }

    @Test
    @DisplayName("createSource_所有类型_全部非空")
    void createSource_allTypes_allNonNull() {
        for (DataSourceType type : DataSourceType.values()) {
            DataSource<?> source = DataSourceRegistry.createSource(type);
            assertThat(source).as("createSource for " + type).isNotNull();
        }
    }

    @Test
    @DisplayName("createSink_所有类型_全部非空")
    void createSink_allTypes_allNonNull() {
        for (DataSourceType type : DataSourceType.values()) {
            DataSink<?> sink = DataSourceRegistry.createSink(type);
            assertThat(sink).as("createSink for " + type).isNotNull();
        }
    }

    @Test
    @DisplayName("readStream_Hive类型_抛出UnsupportedOperationException")
    void readStream_hive_throwsUnsupportedOperation() {
        DataSource<?> source = DataSourceRegistry.createSource(DataSourceType.HIVE);
        assertThatThrownBy(() -> source.readStream(null, null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("writeStream_Hive类型_抛出UnsupportedOperationException")
    void writeStream_hive_throwsUnsupportedOperation() {
        DataSink<?> sink = DataSourceRegistry.createSink(DataSourceType.HIVE);
        assertThatThrownBy(() -> sink.writeStream(null, null))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
