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
 * ORC 文件读取。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class OrcSource implements DataSource {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public OrcSource(DataSourceConfig config) {
        this.config = config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        String path = options.getResource();
        log.info("读取 ORC 文件: {}", path);
        return spark.read().orc(path);
    }
}
