package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

/**
 * DWS 层处理器：基于 DWD 进行多粒度日度汇总聚合。
 * <p>一致性语义：Overwrite 分区目录为 <b>最终一致</b>。</p>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class DwsProcessor {

    private final SparkSession spark;
    private final String dt;

    public DwsProcessor(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    public void process(Dataset<Row> dwdDf) {
        long startTime = System.currentTimeMillis();

        String paidStatuses = OrderStatsConfig.PAID_STATUSES;
        String refundEvent = OrderStatsConfig.EVENT_REFUND;

        dwdDf.createOrReplaceTempView("dwd_order_tmp");

        String aggregateSql =
                "SELECT '" + OrderStatsConfig.DIM_TYPE_USER + "' AS dim_type, user_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN order_status IN (" + paidStatuses + ") THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='" + refundEvent + "' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp GROUP BY user_id "
                        + "UNION ALL "
                        + "SELECT '" + OrderStatsConfig.DIM_TYPE_PRODUCT + "' AS dim_type, product_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN order_status IN (" + paidStatuses + ") THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='" + refundEvent + "' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp GROUP BY product_id "
                        + "UNION ALL "
                        + "SELECT '" + OrderStatsConfig.DIM_TYPE_STORE + "' AS dim_type, store_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN order_status IN (" + paidStatuses + ") THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='" + refundEvent + "' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp GROUP BY store_id "
                        + "UNION ALL "
                        + "SELECT '" + OrderStatsConfig.DIM_TYPE_REGION + "' AS dim_type, region_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN order_status IN (" + paidStatuses + ") THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='" + refundEvent + "' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp GROUP BY region_id";

        Dataset<Row> aggregatedDf = spark.sql(aggregateSql);

        String dwsPath = OrderStatsConfig.DWS_ORDER_DAILY + "/" + OrderStatsConfig.PART_DT + "=" + dt;
        DataSources.json()
                .option(o -> o.setWriteMode(SaveMode.Overwrite))
                .write(aggregatedDf.withColumn("dt", lit(dt)), dwsPath);

        spark.catalog().dropTempView("dwd_order_tmp");

        log.info("[DWS] dt={} 汇总完成, 耗时={}ms", dt, System.currentTimeMillis() - startTime);
    }
}
