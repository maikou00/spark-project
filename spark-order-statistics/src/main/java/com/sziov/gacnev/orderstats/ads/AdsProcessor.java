package com.sziov.gacnev.orderstats.ads;

import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * ADS 层处理器：基于 DWD 订单事实表计算每日核心 KPI 指标。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class AdsProcessor {

    private final SparkSession spark;
    private final String dt;

    public AdsProcessor(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    public void process() {
        long startTime = System.currentTimeMillis();
        log.info("[ADS] 开始KPI计算，日期: {}", dt);

        spark.catalog().setCurrentDatabase("dwd");
        spark.sql("SELECT * FROM dwd_order_fact WHERE dt='" + dt + "'")
                .createOrReplaceTempView("dwd_for_kpi");

        // 使用简单聚合，避免 Spark 3.3 CASE+CAST bug
        Dataset<Row> kpiDf = spark.sql(
                "SELECT "
                        + "COUNT(1) AS total_orders, "
                        + "COALESCE(SUM(order_amount), 0) AS total_gmv, "
                        + "COALESCE(ROUND(SUM(order_amount) / COUNT(1), 2), 0) AS avg_order_amount, "
                        + "COALESCE(SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END), 0) AS paid_orders, "
                        + "COALESCE(ROUND(SUM(CASE WHEN order_status IN ('pay','ship','sign') THEN 1 ELSE 0 END) * 1.0 "
                        + "  / COUNT(1), 4), 0) AS payment_rate, "
                        + "COALESCE(SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END), 0) AS refund_orders, "
                        + "COALESCE(ROUND(SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) * 1.0 "
                        + "  / COUNT(1), 4), 0) AS refund_rate "
                        + "FROM dwd_for_kpi");

        log.info("[ADS] KPI计算完成");
        printKpiReport(kpiDf);
        writeToAds(kpiDf);
        spark.catalog().dropTempView("dwd_for_kpi");

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[ADS] 处理完成, 耗时: {}ms", elapsed);
    }

    private void printKpiReport(Dataset<Row> kpiDf) {
        Row row = kpiDf.first();
        log.info("=================================================");
        log.info("           订单核心KPI日报 (dt={})", dt);
        log.info("=================================================");
        log.info("  总订单量:    {}", row.get(row.fieldIndex("total_orders")));
        log.info("  总GMV:       {}", row.get(row.fieldIndex("total_gmv")));
        log.info("  客单价:      {}", row.get(row.fieldIndex("avg_order_amount")));
        log.info("  已支付订单:  {}", row.get(row.fieldIndex("paid_orders")));
        log.info("  支付转化率:  {}", row.get(row.fieldIndex("payment_rate")));
        log.info("  退单数:      {}", row.get(row.fieldIndex("refund_orders")));
        log.info("  退单率:      {}", row.get(row.fieldIndex("refund_rate")));
        log.info("=================================================");
    }

    private void writeToAds(Dataset<Row> df) {
        DataSources.hive()
                .option(o -> o.setDatabase("ads")
                        .setWriteMode(SaveMode.Overwrite))
                .write(df, "ads_order_kpi_daily");
        log.info("[ADS] 写入Hive完成: ads.ads_order_kpi_daily");
    }
}
