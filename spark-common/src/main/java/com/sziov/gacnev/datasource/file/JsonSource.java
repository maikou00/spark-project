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
 * JSON 文件读取，支持自定义Schema、multiline控制。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class JsonSource implements DataSource {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public JsonSource(DataSourceConfig config) {
        this.config = config;
    }

    private static final String MULTILINE_DISABLED = "false";
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final String MERGE_SCHEMA_FALSE = "false";

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        String path = options.getResource();
        String encoding = options.getEncoding() != null ? options.getEncoding() : DEFAULT_ENCODING;
        StructType schema = options.getSchema();
        log.info("读取 JSON 文件: {}, Schema字段数: {}", path,
                schema != null ? schema.fields().length : 0);
        if (schema != null) {
            return spark.read().schema(schema)
                    .option("multiline", MULTILINE_DISABLED)
                    .option("encoding", encoding)
                    .option("mergeSchema", MERGE_SCHEMA_FALSE)
                    .json(path);
        }
        return spark.read().option("multiline", MULTILINE_DISABLED)
                .option("encoding", encoding).json(path);
    }
}
