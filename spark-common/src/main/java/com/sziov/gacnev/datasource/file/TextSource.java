package com.sziov.gacnev.datasource.file;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.ReadOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 文本文件读取，支持自定义列名。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class TextSource implements DataSource {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public TextSource(DataSourceConfig config) {
        this.config = config;
    }

    private static final String DEFAULT_COLUMN_NAME = "raw_text";

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        String path = options.getResource();
        String columnName = options.getColumnName() != null ? options.getColumnName() : DEFAULT_COLUMN_NAME;
        log.info("读取文本文件: {}, 列名: {}", path, columnName);
        return spark.read().textFile(path).toDF(columnName);
    }
}
