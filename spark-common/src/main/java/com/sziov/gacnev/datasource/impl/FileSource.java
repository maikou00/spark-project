package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.option.FileOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

/**
 * 文件数据源读取实现，通过 format + options 声明式驱动。
 *
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class FileSource implements DataSource<FileOption> {

    private final String format;
    private final Map<String, String> sparkOpts;
    private final int maxRetries;

    public FileSource(String format, Map<String, String> sparkOpts, String resourceKey, int maxRetries) {
        this.format = format;
        this.sparkOpts = sparkOpts;
        this.maxRetries = maxRetries;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, FileOption options) {
        
    log.info("FileSource 读取数据，format: {}，resource: {}", format, options.getResource());
    Map<String, String> allOpts = new HashMap<>(sparkOpts);
    if (options.getEncoding() != null) {
        allOpts.put("encoding", options.getEncoding());
    }
    StructType schema = options.getSchema();
    if (schema != null) {
        return spark.read().format(format).options(allOpts).schema(schema).load(options.getResource());
    }
    if (options.getDelimiter() != null) {
        allOpts.put("delimiter", options.getDelimiter());
    }
    if (options.getColumnName() != null) {
        allOpts.put("columnName", options.getColumnName());
    }
    return spark.read().format(format).options(allOpts).load(options.getResource());
        
    }
}
