package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.RetryUtils;
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
 * 读取操作内建指数退避重试（默认 3 次，初始等待 1s），应对 HDFS 偶发网络抖动。
 *
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class FileSource implements DataSource<FileOption> {

    private final String format;
    private final Map<String, String> sparkOpts;
    private final int maxRetries;

    public FileSource(String format, Map<String, String> sparkOpts, int maxRetries) {
        this.format = format;
        this.sparkOpts = sparkOpts;
        this.maxRetries = maxRetries;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, FileOption options) {
        return RetryUtils.retry(maxRetries, 1000L, () -> {
            log.info("FileSource 读取数据，format: {}，resource: {}", format, options.getResource());
            Map<String, String> allOpts = new HashMap<>(sparkOpts);
            if (options.getEncoding() != null) {
                allOpts.put("encoding", options.getEncoding());
            }
            StructType schema = options.getSchema();
            Dataset<Row> df;
            if (schema != null) {
                df = spark.read().format(format).options(allOpts).schema(schema).load(options.getResource());
            } else {
                if (options.getDelimiter() != null) {
                    allOpts.put("delimiter", options.getDelimiter());
                }
                if (options.getColumnName() != null) {
                    allOpts.put("columnName", options.getColumnName());
                }
                df = spark.read().format(format).options(allOpts).load(options.getResource());
            }
            int repartitionNum = options.getRepartitionNum();
            return repartitionNum > 0 ? df.repartition(repartitionNum) : df;
        });
    }
}
