package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.DataSourceProvider;
import com.sziov.gacnev.datasource.DataSourceType;

/**
 * Doris 数据源注册工厂。
 *
 * @author maikou
 * @since 2026-06-12
 */
public class DorisSourceProvider implements DataSourceProvider {

    @Override
    public DataSourceType type() {
        return DataSourceType.DORIS;
    }

    @Override
    public DataSource<?> createSource() {
        return new DorisSource();
    }

    @Override
    public DataSink<?> createSink() {
        return new DorisSink();
    }
}
