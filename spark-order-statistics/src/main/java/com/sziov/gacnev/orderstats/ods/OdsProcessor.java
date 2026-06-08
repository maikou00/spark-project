package com.sziov.gacnev.orderstats.ods;

import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * ODS 层处理器：读取 Hive ODS 表并输出数据质量报告。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class OdsProcessor {

    private static final String DB_ODS = OrderStatsConfig.DB_ODS;

    private final SparkSession spark;
    private final String dt;

    public OdsProcessor(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    public Dataset<Row> readOrderEvents() {
        log.info("[ODS] 读取订单事件表: ods_order_event");
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
                .option(o -> o.setDatabase(DB_ODS)
                        .setPartitionFilter(OrderStatsConfig.PART_DT + "='" + dt + "'"))
                .read(spark, tableName);
    }
}
