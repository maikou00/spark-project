package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * ADS 层处理器：基于 DWS 汇总表计算每日核心 KPI → Doris。
 * <p>取 user 维度汇总作为全局 KPI 基准，Doris 表需为 UNIQUE KEY(dt) 模型以保证幂等 upsert。</p>
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

    public void process(Dataset<Row> dwsDf) {
        Dataset<Row> userDimKpi = dwsDf
                .filter(col("dim_type").equalTo(OrderStatsConfig.DIM_TYPE_USER))
                .agg(
                        sum("order_count").as("total_orders"),
                        sum("total_amount").as("total_gmv"),
                        sum("paid_count").as("paid_orders"),
                        sum("refund_count").as("refund_orders")
                )
                .withColumn("avg_order_amount",
                        round(col("total_gmv").divide(col("total_orders")), 2))
                .withColumn("payment_rate",
                        round(col("paid_orders").cast("double").divide(col("total_orders")), 4))
                .withColumn("refund_rate",
                        round(col("refund_orders").cast("double").divide(col("total_orders")), 4))
                .withColumn("dt", lit(dt).cast(DataTypes.DateType))
                .select("dt", "total_orders", "total_gmv", "avg_order_amount",
                        "paid_orders", "payment_rate", "refund_orders", "refund_rate");

        DataSources.doris()
                .option(o -> o.setWriteMode(SaveMode.Append))
                .write(userDimKpi, OrderStatsConfig.ADS_ORDER_KPI_DAILY);
    }
}
