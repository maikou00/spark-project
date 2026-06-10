package com.sziov.gacnev.datasource.core;

import com.sziov.gacnev.common.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 通用数据源读取实现，通过 format + options 声明式驱动。
 * 覆盖 CSV/JSON/Parquet/ORC/Text/Kafka/Redis/MongoDB/Elasticsearch。
 *
 * @param <C> 配置类型
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class GenericSource<C> implements DataSource<C> {

    private final String format;
    private final C config;
    private final Supplier<Map<String, String>> configOptionsSupplier;
    private final String resourceKey;
    private final int maxRetries;

    public GenericSource(String format, C config,
                         Supplier<Map<String, String>> configOptionsSupplier,
                         String resourceKey, int maxRetries) {
        this.format = format;
        this.config = config;
        this.configOptionsSupplier = configOptionsSupplier;
        this.resourceKey = resourceKey;
        this.maxRetries = maxRetries;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        return RetryUtils.retry(maxRetries, 1000L, () -> {
            log.info("GenericSource 读取数据，format: {}，resource: {}", format, options.getResource());
            Map<String, String> allOpts = buildOptions(options);
            if (resourceKey != null) {
                allOpts.put(resourceKey, options.getResource());
            }
            return spark.read()
                    .format(format)
                    .options(allOpts)
                    .load(options.getResource());
        });
    }

    @Override
    public Dataset<Row> readStream(SparkSession spark, ReadOptions options) {
        Map<String, String> allOpts = buildOptions(options);
        if (resourceKey != null) {
            allOpts.put(resourceKey, options.getResource());
        }
        return spark.readStream()
                .format(format)
                .options(allOpts)
                .load(options.getResource());
    }

    private Map<String, String> buildOptions(ReadOptions options) {
        Map<String, String> allOpts = new HashMap<>(configOptionsSupplier.get());
        String encoding = options.getEncoding();
        if (encoding != null) {
            allOpts.put("encoding", encoding);
        }
        StructType schema = options.getSchema();
        if (schema != null) {
            allOpts.put("schema", schema.toDDL());
        }
        String delimiter = options.getDelimiter();
        if (delimiter != null) {
            allOpts.put("delimiter", delimiter);
        }
        String columnName = options.getColumnName();
        if (columnName != null) {
            allOpts.put("columnName", columnName);
        }
        return allOpts;
    }
}
