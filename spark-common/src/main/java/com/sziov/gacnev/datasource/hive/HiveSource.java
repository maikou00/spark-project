package com.sziov.gacnev.datasource.hive;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.ReadOptions;
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
public class HiveSource implements DataSource<HiveConfig> {

    private final HiveConfig config;

    public HiveSource(HiveConfig config) {
        this.config = config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        String database = options.getDatabase();
        String table = options.getResource();
        String partitionFilter = options.getPartitionFilter();

        if (database != null && !database.isEmpty()) {
            spark.catalog().setCurrentDatabase(database);
        }

        String fullTableName = table;
        if (partitionFilter != null && !partitionFilter.isEmpty()) {
            String sql = String.format("SELECT * FROM %s WHERE %s", fullTableName, partitionFilter);
            log.info("读取Hive表SQL: {}", sql);
            return spark.sql(sql);
        }
        String sql = String.format("SELECT * FROM %s", fullTableName);
        log.info("读取Hive表SQL: {}", sql);
        return spark.sql(sql);
    }
}
