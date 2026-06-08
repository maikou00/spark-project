package com.sziov.gacnev.datasource.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 数据源读取接口。
 *
 * @author maikou
 * @since 2026-06-09
 */
public interface DataSource {

    Dataset<Row> read(SparkSession spark, ReadOptions options);

    default Dataset<Row> readStream(SparkSession spark, ReadOptions options) {
        throw new UnsupportedOperationException("流式读取不支持");
    }
}
