package com.sziov.gacnev.datasource.hive;

import com.sziov.gacnev.datasource.core.DataSource;
import com.sziov.gacnev.datasource.core.DataSourceConfig;
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
public class HiveSource implements DataSource {

    @SuppressWarnings("unused")
    private final DataSourceConfig config;

    public HiveSource(DataSourceConfig config) {
        this.config = config;
    }

    @Override
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        String database = options.getDatabase();
        String table = options.getResource();
        String partitionFilter = options.getPartitionFilter();
        String fullTableName = (database != null && !database.isEmpty())
                ? database + "." + table : table;

        if (partitionFilter != null && !partitionFilter.isEmpty()) {
            String sql = String.format("SELECT * FROM %s WHERE %s", fullTableName, partitionFilter);
            log.info("读取Hive表SQL-带分区过滤: {}", sql);
            return spark.sql(sql);
        }
        String sql = String.format("SELECT * FROM %s", fullTableName);
        log.info("读取Hive表SQL: {}", sql);
        return spark.sql(sql);
    }
}
