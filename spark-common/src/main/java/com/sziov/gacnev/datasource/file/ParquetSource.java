package com.sziov.gacnev.datasource.file;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.ReadOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Parquet 文件读取，支持mergeSchema控制。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ParquetSource implements DataSource {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public ParquetSource(DataSourceConfig config) {
        this.config = config;
    }

    private static final String MERGE_SCHEMA_FALSE = "false";

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        String path = options.getResource();
        log.info("读取 Parquet 文件: {}", path);
        return spark.read().option("mergeSchema", MERGE_SCHEMA_FALSE).parquet(path);
    }
}
