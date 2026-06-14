package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.option.HiveOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Hive 数据写入。
 * <p>一致性语义：insertInto 为 <b>至少一次</b>。</p>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class HiveSink implements DataSink<HiveOption> {

    private static final String DEFAULT_DATABASE = "default";

    @Override
    public void write(Dataset<Row> df, HiveOption options) {
        String database = options.getDatabase() != null ? options.getDatabase() : DEFAULT_DATABASE;
        String table = options.getResource();
        SaveMode writeMode = options.getWriteMode() != null ? options.getWriteMode() : SaveMode.Append;

        SparkSession spark = df.sparkSession();
        spark.sql("SET hive.exec.dynamic.partition=true");
        spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict");
        if (SaveMode.Overwrite.equals(writeMode)) {
            spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic");
        }

        String fullTable = database + "." + table;
        log.info("写入Hive表: {}，模式: {}，动态分区: true", fullTable, writeMode);
        df.write().mode(writeMode).insertInto(fullTable);
    }
}
