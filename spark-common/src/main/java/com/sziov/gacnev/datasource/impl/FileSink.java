package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.RetryUtils;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.option.FileOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Map;

/**
 * 文件数据源写入实现，通过 format + options 声明式驱动。
 * 写入操作内建指数退避重试（应对 HDFS 偶发网络抖动）。
 *
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class FileSink implements DataSink<FileOption> {

    private final String format;
    private final Map<String, String> sparkOpts;
    private final int maxRetries;

    public FileSink(String format, Map<String, String> sparkOpts, int maxRetries) {
        this.format = format;
        this.sparkOpts = sparkOpts;
        this.maxRetries = maxRetries;
    }

    @Override
    public void write(Dataset<Row> df, FileOption options) {
        RetryUtils.retry(maxRetries, 1000L, () -> {
            String resource = options.getResource();
            SaveMode mode = options.getWriteMode() != null ? options.getWriteMode() : SaveMode.Append;

            log.info("FileSink 写入数据，format: {}，resource: {}，mode: {}", format, resource, mode);
            Map<String, String> allOpts = new HashMap<>(sparkOpts);
            int repartitionNum = options.getRepartitionNum();
            if (repartitionNum > 0) {
                df.repartition(repartitionNum).write()
                        .format(format).mode(mode).options(allOpts).save(resource);
            } else {
                df.write().format(format).mode(mode).options(allOpts).save(resource);
            }
            return null;
        });
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
        } catch (java.util.concurrent.TimeoutException e) {
            log.error("FileSink 流式写入启动超时，format: {}", format, e);
            throw new RuntimeException(format + " 流式写入启动失败", e);
        }
    }

}