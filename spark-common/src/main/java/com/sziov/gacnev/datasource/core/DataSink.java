package com.sziov.gacnev.datasource.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 数据源写入接口。
 *
 * @author maikou
 * @since 2026-06-09
 */
public interface DataSink {

    void write(Dataset<Row> df, WriteOptions options);

    default void writeStream(Dataset<Row> df, WriteOptions options) {
        throw new UnsupportedOperationException("流式写入不支持");
    }
}
