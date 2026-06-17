package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.DataSource;
import com.sziov.gacnev.datasource.DataSourceProvider;
import com.sziov.gacnev.datasource.DataSourceType;
import com.sziov.gacnev.datasource.option.HiveOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hive 数据读取。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public class HiveSource implements DataSource<HiveOption>, DataSourceProvider {

    @Override
    public DataSourceType type() { return DataSourceType.HIVE; }

    @Override
    public DataSource<?> createSource() { return this; }

    @Override
    public DataSink<?> createSink() { return new HiveSink(); }

    @Override
    public Dataset<Row> read(SparkSession spark, HiveOption options) {
        String database = options.getDatabase();
        String table = options.getResource();

        // 使用完全限定表名，避免修改全局 Session 状态引发并发问题
        String fullTable = (database != null && !database.isEmpty())
                ? database + "." + table
                : table;

        Dataset<Row> df = spark.table(fullTable);
        String partitionFilter = options.getPartitionFilter();
        if (partitionFilter != null && !partitionFilter.isEmpty()) {
            log.info("Hive 分区过滤: {}", partitionFilter);
            df = df.filter(partitionFilter);
        }
        return df;
    }
}
