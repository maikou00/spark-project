package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.option.FileOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 文件数据源写入实现，通过 format + options 声明式驱动。
 *
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class FileSink implements DataSink<FileOption> {

    private final String format;
    private final Map<String, String> sparkOpts;
    private final int maxRetries;

    public FileSink(String format, Map<String, String> sparkOpts, String resourceKey, int maxRetries) {
        this.format = format;
        this.sparkOpts = sparkOpts;
        this.maxRetries = maxRetries;
    }

    @Override
    public void write(Dataset<Row> df, FileOption options) {
        String resource = options.getResource();
        SaveMode mode = options.getWriteMode() != null ? options.getWriteMode() : SaveMode.Append;

        
    log.info("FileSink 写入数据，format: {}，resource: {}，mode: {}", format, resource, mode);
    Map<String, String> allOpts = buildOptions(options);
    df.write()
            .format(format)
            .mode(mode)
            .options(allOpts)
            .save(resource);
        
    }

    @Override
    public void writeStream(Dataset<Row> df, FileOption options) {
        try {
            df.writeStream().foreachBatch((batchDf, batchId) -> {
                batchDf.write()
                        .format(format)
                        .mode(SaveMode.Append)
                        .options(sparkOpts)
                        .save(options.getResource());
            }).start();
        } catch (TimeoutException e) {
            log.error("FileSink 流式写入启动超时，format: {}", format, e);
            throw new RuntimeException(format + " 流式写入启动失败", e);
        }
    }

    private Map<String, String> buildOptions(FileOption options) {
        Map<String, String> allOpts = new HashMap<>(sparkOpts);
        return allOpts;
    }
}
