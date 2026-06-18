package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * ODS 层处理器：读取 Hive ODS 贴源表。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class OdsProcessor {

    private final SparkSession spark;
    private final String dt;

    public OdsProcessor(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    public Dataset<Row> readOrderEvents() {
        return readOdsTable("ods_order_event");
    }

    public Dataset<Row> readUsers() {
        return readOdsTable("ods_user");
    }

    public Dataset<Row> readProducts() {
        return readOdsTable("ods_product");
    }

    public Dataset<Row> readStores() {
        return readOdsTable("ods_store");
    }

    public Dataset<Row> readRegions() {
        return readOdsTable("ods_region");
    }

    private Dataset<Row> readOdsTable(String tableName) {
        return DataSources.hive()
                .option(o -> o.setDatabase(OrderStatsConfig.DB_ODS)
                        .setPartitionFilter(OrderStatsConfig.PART_DT + "='" + dt + "'"))
                .read(spark, tableName);
    }
}
