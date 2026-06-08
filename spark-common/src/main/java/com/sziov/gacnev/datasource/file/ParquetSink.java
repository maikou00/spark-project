package com.sziov.gacnev.datasource.file;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Parquet 文件写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class ParquetSink implements DataSink {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public ParquetSink(DataSourceConfig config) {
        this.config = config;
    }

    private static final String PARQUET_COMPRESS_CODEC = "spark.sql.parquet.compression.codec";
    private static final String SNAPPY_COMPRESS = "snappy";
    private static final int DEFAULT_REPARTITION = 1;

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        String path = options.getResource();
        String mode = "overwrite".equalsIgnoreCase(options.getWriteMode()) ? "overwrite" : "append";
        int repartitionNum = options.getRepartitionNum() > 0 ? options.getRepartitionNum() : DEFAULT_REPARTITION;
        df.sparkSession().conf().set(PARQUET_COMPRESS_CODEC, SNAPPY_COMPRESS);
        log.info("写入 Parquet 文件: {}，重分区数: {}，模式: {}", path, repartitionNum, mode);
        df.repartition(repartitionNum).write().mode(mode).parquet(path);
    }
}
