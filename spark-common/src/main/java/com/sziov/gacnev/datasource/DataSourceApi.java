package com.sziov.gacnev.datasource;

import com.sziov.gacnev.datasource.option.DataSourceOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Consumer;

/**
 * 数据源统一 API，提供链式调用风格。
 *
 * @param <O> 数据源 Option 类型
 * @author maikou
 * @since 2026-06-10
 */
public class DataSourceApi<O extends DataSourceOption<O>> {

    private final DataSourceType type;
    private final O option;

    DataSourceApi(DataSourceType type, O option) {
        this.type = type;
        this.option = option;
    }

    /**
     * 链式配置 Option。
     *
     * @param configurer Option 配置回调
     * @return this
     */
    public DataSourceApi<O> option(Consumer<O> configurer) {
        configurer.accept(option);
        return this;
    }

    /**
     * 读取数据。
     *
     * @param spark    SparkSession
     * @param resource 资源标识
     * @return Dataset
     */
    @SuppressWarnings("unchecked")
    public Dataset<Row> read(SparkSession spark, String resource) {
        DataSources.ensureInitialized();
        option.setResource(resource);
        DataSource<?> source = DataSources.getSource(type);
        return ((DataSource<O>) source).read(spark, option);
    }

    /**
     * 写入数据。
     *
     * @param df       Dataset
     * @param resource 资源标识
     */
    @SuppressWarnings("unchecked")
    public void write(Dataset<Row> df, String resource) {
        DataSources.ensureInitialized();
        option.setResource(resource);
        DataSink<?> sink = DataSources.getSink(type);
        ((DataSink<O>) sink).write(df, option);
    }

    /**
     * 写入数据（带额外 Option 配置）。
     *
     * @param df         Dataset
     * @param configurer 额外 Option 配置回调
     */
    @SuppressWarnings("unchecked")
    public void write(Dataset<Row> df, Consumer<O> configurer) {
        DataSources.ensureInitialized();
        configurer.accept(option);
        DataSink<?> sink = DataSources.getSink(type);
        ((DataSink<O>) sink).write(df, option);
    }
}
