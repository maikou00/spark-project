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
 * CSV 文件读取，支持自定义Schema、分隔符、编码。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class CsvSource implements DataSource {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public CsvSource(DataSourceConfig config) {
        this.config = config;
    }

    private static final String DEFAULT_DELIMITER = ",";
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final String HEADER_FALSE = "false";

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        String path = options.getResource();
        String delimiter = options.getDelimiter() != null ? options.getDelimiter() : DEFAULT_DELIMITER;
        String encoding = options.getEncoding() != null ? options.getEncoding() : DEFAULT_ENCODING;
        StructType schema = options.getSchema();
        log.info("读取 CSV 文件: {}, 分隔符: {}, Schema字段数: {}", path, delimiter,
                schema != null ? schema.fields().length : 0);
        if (schema != null) {
            return spark.read().schema(schema)
                    .option("delimiter", delimiter)
                    .option("header", HEADER_FALSE)
                    .option("encoding", encoding)
                    .csv(path);
        }
        return spark.read().option("header", "true").option("delimiter", delimiter)
                .option("encoding", encoding).csv(path);
    }
}
