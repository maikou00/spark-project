package com.sziov.gacnev.datasource;

import com.sziov.gacnev.datasource.option.DataSourceOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 数据源读取接口。
 *
 * @param <O> 数据源 Option 类型
 * @author maikou
 * @since 2026-06-09
 */
public interface DataSource<O extends DataSourceOption<O>> {

    Dataset<Row> read(SparkSession spark, O options);

}
