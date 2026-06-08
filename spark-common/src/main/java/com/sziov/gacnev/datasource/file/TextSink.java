package com.sziov.gacnev.datasource.file;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Text 文件写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class TextSink implements DataSink {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public TextSink(DataSourceConfig config) {
        this.config = config;
    }

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        String mode = "overwrite".equalsIgnoreCase(options.getWriteMode()) ? "overwrite" : "append";
        log.info("写入 Text 文件: {}，模式: {}", options.getResource(), mode);
        df.write().mode(mode).format("text").save(options.getResource());
    }
}
