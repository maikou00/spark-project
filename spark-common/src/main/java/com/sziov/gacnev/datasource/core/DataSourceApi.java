package com.sziov.gacnev.datasource.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Consumer;

/**
 * 数据源统一 API，提供链式调用风格。
 *
 * @author maikou
 * @since 2026-06-10
 */
public class DataSourceApi {

    private final DataSourceType type;
    private ReadOptions.ReadOptionsBuilder readOpts = ReadOptions.builder();
    private WriteOptions.WriteOptionsBuilder writeOpts = WriteOptions.builder();

    DataSourceApi(DataSourceType type) {
        this.type = type;
    }

    /**
     * 配置读取选项。
     *
     * @param configurer 选项配置器
     * @return this
     */
    public DataSourceApi options(Consumer<ReadOptions.ReadOptionsBuilder> configurer) {
        configurer.accept(readOpts);
        return this;
    }

    /**
     * 配置写入选项。
     *
     * @param configurer 选项配置器
     * @return this
     */
    public DataSourceApi writeOptions(Consumer<WriteOptions.WriteOptionsBuilder> configurer) {
        configurer.accept(writeOpts);
        return this;
    }

    /**
     * 读取数据。
     *
     * @param spark    SparkSession
     * @param resource 资源标识（表名/路径/topic 等）
     * @return DataFrame
     */
    @SuppressWarnings("unchecked")
    public Dataset<Row> read(SparkSession spark, String resource) {
        ReadOptions options = readOpts.resource(resource).build();
        return ((DataSource<Object>) DataSourceRegistry.createSource(type)).read(spark, options);
    }

    /**
     * 写入数据（简单模式）。
     *
     * @param df       要写入的 DataFrame
     * @param resource 资源标识
     */
    @SuppressWarnings("unchecked")
    public void write(Dataset<Row> df, String resource) {
        WriteOptions options = writeOpts.resource(resource).build();
        ((DataSink<Object>) DataSourceRegistry.createSink(type)).write(df, options);
    }

    /**
     * 写入数据（带选项）。
     *
     * @param df         要写入的 DataFrame
     * @param configurer 写入选项配置器
     */
    @SuppressWarnings("unchecked")
    public void write(Dataset<Row> df, Consumer<WriteOptions.WriteOptionsBuilder> configurer) {
        configurer.accept(writeOpts);
        WriteOptions options = writeOpts.build();
        ((DataSink<Object>) DataSourceRegistry.createSink(type)).write(df, options);
    }
}
