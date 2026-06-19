package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * ODS 层处理器：读取 JSON 贴源数据。
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
        String path = OrderStatsConfig.ODS_ORDER_EVENT + "/" + OrderStatsConfig.PART_DT + "=" + dt;
        return DataSources.json().read(spark, path);
    }
}
