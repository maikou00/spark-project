package com.sziov.gacnev.datasource;

/**
 * SPI 接口：数据源提供者，通过 {@link java.util.ServiceLoader} 自动发现。
 * 每个需要自动注册的数据源 Source 实现类必须实现本接口。
 *
 * @author maikou
 * @since 2026-06-10
 */
public interface DataSourceProvider {

    DataSourceType type();

    DataSource<?> createSource();

    DataSink<?> createSink();
}
