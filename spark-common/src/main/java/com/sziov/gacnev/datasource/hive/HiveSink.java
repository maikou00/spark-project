package com.sziov.gacnev.datasource.hive;

import com.sziov.gacnev.datasource.core.DataSink;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
import com.sziov.gacnev.datasource.core.WriteOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Hive 数据写入。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class HiveSink implements DataSink {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public HiveSink(DataSourceConfig config) {
        this.config = config;
    }

    private static final String SNAPPY_COMPRESS = "snappy";
    private static final String HIVE_DYNAMIC_PARTITION = "hive.exec.dynamic.partition";
    private static final String HIVE_DYNAMIC_PARTITION_MODE = "hive.exec.dynamic.partition.mode";
    private static final String PARQUET_COMPRESS_CODEC = "spark.sql.parquet.compression.codec";
    private static final String ORC_COMPRESS_CODEC = "spark.sql.orc.compression.codec";
    private static final String TEMP_VIEW_NAME = "tmp_write_data";
    private static final String DEFAULT_DATABASE = "default";
    private static final String DEFAULT_PARTITION_KEY = "dt";

    @Override
    public void write(Dataset<Row> df, WriteOptions options) {
        String database = config.getExtraOptions().getOrDefault("database", DEFAULT_DATABASE);
        String table = options.getResource();
        String writeMode = options.getWriteMode();

        // 切换数据库
        String currentDb = df.sparkSession().catalog().currentDatabase();
        df.sparkSession().catalog().setCurrentDatabase(database);

        if ("overwrite".equalsIgnoreCase(writeMode)) {
            initHiveConfig(df);
            String partitionKey = config.getExtraOptions().getOrDefault("partitionKey", DEFAULT_PARTITION_KEY);
            String partitionVal = resolvePartitionValue(options);
            df.createOrReplaceTempView(TEMP_VIEW_NAME);
            String sql = String.format(
                    "INSERT OVERWRITE TABLE %s PARTITION (%s='%s') SELECT * FROM %s",
                    table, partitionKey, partitionVal, TEMP_VIEW_NAME
            );
            log.info("执行Hive静态分区写入SQL: {}", sql);
            df.sparkSession().sql(sql);
            log.info("Hive静态分区写入完成，库: {}, 表: {}, 分区: {}={}", database, table, partitionKey, partitionVal);
        } else {
            log.info("执行Hive追加写入，库: {}, 表: {}", database, table);
            df.write().mode(SaveMode.Append).insertInto(table);
        }

        // 恢复数据库
        df.sparkSession().catalog().setCurrentDatabase(currentDb);
    }

    private String resolvePartitionValue(WriteOptions options) {
        if (options.getPartitionValue() != null && !options.getPartitionValue().isEmpty()) {
            return options.getPartitionValue();
        }
        return com.sziov.gacnev.common.DateUtils.getCurrentDate();
    }

    private void initHiveConfig(Dataset<Row> df) {
        df.sparkSession().conf().set(HIVE_DYNAMIC_PARTITION, "true");
        df.sparkSession().conf().set(HIVE_DYNAMIC_PARTITION_MODE, "nonstrict");
        df.sparkSession().conf().set(PARQUET_COMPRESS_CODEC, SNAPPY_COMPRESS);
        df.sparkSession().conf().set(ORC_COMPRESS_CODEC, SNAPPY_COMPRESS);
        log.info("Hive动态分区配置已初始化");
    }
}
