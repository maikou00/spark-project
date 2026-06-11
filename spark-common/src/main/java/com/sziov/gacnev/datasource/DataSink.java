package com.sziov.gacnev.datasource;

import com.sziov.gacnev.datasource.option.DataSourceOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 数据源写入接口。
 *
 * @param <O> 数据源 Option 类型
 * @author maikou
 * @since 2026-06-09
 */
public interface DataSink<O extends DataSourceOption<O>> {

    void write(Dataset<Row> df, O options);

    default void writeStream(Dataset<Row> df, O options) {
        throw new UnsupportedOperationException("流式写入不支持");
    }

    default void upsert(Dataset<Row> df, O options) {
        throw new UnsupportedOperationException("UPSERT不支持");
    }

    default void execute(O options) {
        throw new UnsupportedOperationException("直接执行SQL不支持");
    }
}
