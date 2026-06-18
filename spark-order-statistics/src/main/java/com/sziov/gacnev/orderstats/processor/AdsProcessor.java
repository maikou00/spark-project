package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

/**
 * ADS 层处理器：基于 DWD 订单事实表计算每日核心 KPI 指标。
 * <p>写策略：写入 Doris（应用服务层），Doris 表需为 UNIQUE KEY(dt) 模型以保证幂等 upsert。
 * 一致性语义：Doris Stream Load 为 <b>至少一次</b>（label 去重可幂等）。</p>
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

    public void process(Dataset<Row> dwdDf) {
        String paidStatuses = OrderStatsConfig.PAID_STATUSES;
        String refundEvent = OrderStatsConfig.EVENT_REFUND;

        dwdDf.createOrReplaceTempView("dwd_for_kpi");

        Dataset<Row> kpiDf = spark.sql(
                "SELECT "
                        + "COUNT(1) AS total_orders, "
                        + "COALESCE(SUM(order_amount), 0) AS total_gmv, "
                        + "COALESCE(ROUND(SUM(order_amount) / COUNT(1), 2), 0) AS avg_order_amount, "
                        + "COALESCE(SUM(CASE WHEN order_status IN (" + paidStatuses + ") THEN 1 ELSE 0 END), 0) AS paid_orders, "
                        + "COALESCE(ROUND(SUM(CASE WHEN order_status IN (" + paidStatuses + ") THEN 1 ELSE 0 END) * 1.0 "
                        + "  / COUNT(1), 4), 0) AS payment_rate, "
                        + "COALESCE(SUM(CASE WHEN order_status='" + refundEvent + "' THEN 1 ELSE 0 END), 0) AS refund_orders, "
                        + "COALESCE(ROUND(SUM(CASE WHEN order_status='" + refundEvent + "' THEN 1 ELSE 0 END) * 1.0 "
                        + "  / COUNT(1), 4), 0) AS refund_rate "
                        + "FROM dwd_for_kpi")
                .withColumn("dt", lit(dt).cast(DataTypes.DateType));

        DataSources.doris()
                .option(o -> o.setWriteMode(SaveMode.Append))
                .write(kpiDf, OrderStatsConfig.ADS_ORDER_KPI_DAILY);

        spark.catalog().dropTempView("dwd_for_kpi");
    }
}
