package com.sziov.gacnev.datasource.core;

import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * 通用数据源写入实现，通过 format + options 声明式驱动。
 * 覆盖 CSV/JSON/Parquet/ORC/Text/Kafka/Redis/MongoDB/Elasticsearch。
 *
 * @param <C> 配置类型
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class GenericSink<C> implements DataSink<C> {

    private final String format;
    private final C config;
    private final Supplier<Map<String, String>> configOptionsSupplier;
    private final String resourceKey;
    private final int maxRetries;

    public GenericSink(String format, C config,
                       Supplier<Map<String, String>> configOptionsSupplier,
                       String resourceKey, int maxRetries) {
        this.format = format;
        this.config = config;
        this.configOptionsSupplier = configOptionsSupplier;
        this.resourceKey = resourceKey;
        this.maxRetries = maxRetries;
    }

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        String writeMode = resolveWriteMode(options);
        String resource = options.getResource();

        RetryUtils.retry(maxRetries, 1000L, () -> {
            log.info("GenericSink 写入数据，format: {}，resource: {}，mode: {}", format, resource, writeMode);
            Map<String, String> allOpts = buildOptions(options);
            df.write()
                    .format(format)
                    .mode(writeMode)
                    .options(allOpts)
                    .save(resource);
            return null;
        });
    }

    @Override
    public void writeStream(Dataset<Row> df, WriteOptions options) {
        try {
            df.writeStream().foreachBatch((batchDf, batchId) -> {
                WriteOptions batchOptions = WriteOptions.builder()
                        .resource(options.getResource())
                        .writeMode("append")
                        .build();
                write(batchDf, batchOptions);
            }).start();
        } catch (TimeoutException e) {
            log.error("GenericSink 流式写入启动超时，format: {}", format, e);
            throw new RuntimeException(format + " 流式写入启动失败", e);
        }
    }

    private Map<String, String> buildOptions(WriteOptions options) {
        Map<String, String> allOpts = new HashMap<>(configOptionsSupplier.get());
        if (resourceKey != null && options.getResource() != null) {
            allOpts.put(resourceKey, options.getResource());
        }
        return allOpts;
    }

    private String resolveWriteMode(WriteOptions options) {
        if (options.getWriteMode() == null) {
            return "append";
        }
        if ("overwrite".equalsIgnoreCase(options.getWriteMode())) {
            return "overwrite";
        }
        return "append";
    }
}
