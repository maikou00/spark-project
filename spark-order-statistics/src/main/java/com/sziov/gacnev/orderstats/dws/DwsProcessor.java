package com.sziov.gacnev.orderstats.dws;

import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DWS 层处理器：基于 DWD 订单事实表进行多粒度日度汇总聚合。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class DwsProcessor {

    private final SparkSession spark;
    private final String dt;

    /** 判断已支付状态：pay/ship/sign 均视为已支付 */
    private static final String PAID_STATUS_CONDITION =
            "order_status IN ('pay', 'ship', 'sign')";

    public DwsProcessor(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    /**
     * 执行多粒度聚合：用户/商品/店铺/地区四个维度 → 写入 dws_order_daily。
     */
    public void process() {
        long startTime = System.currentTimeMillis();
        log.info("[DWS] 开始日度汇总聚合，日期: {}", dt);

        String dwdTable = OrderStatsConfig.DWD_ORDER_FACT;
        spark.catalog().setCurrentDatabase("dwd");

        // Step 1: 创建 DWD 临时视图
        spark.sql("SELECT * FROM " + dwdTable + " WHERE dt='" + dt + "'")
                .createOrReplaceTempView("dwd_order_tmp");

        // Step 2: 四维度聚合 SQL（UNION ALL）
        String aggregateSql =
                "SELECT 'user' AS dim_type, user_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN " + PAID_STATUS_CONDITION + " THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp "
                        + "GROUP BY user_id "
                        + "UNION ALL "
                        + "SELECT 'product' AS dim_type, product_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN " + PAID_STATUS_CONDITION + " THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp "
                        + "GROUP BY product_id "
                        + "UNION ALL "
                        + "SELECT 'store' AS dim_type, store_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN " + PAID_STATUS_CONDITION + " THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp "
                        + "GROUP BY store_id "
                        + "UNION ALL "
                        + "SELECT 'region' AS dim_type, region_id AS dim_id, "
                        + "COUNT(1) AS order_count, "
                        + "COALESCE(SUM(order_amount), 0) AS total_amount, "
                        + "SUM(CASE WHEN " + PAID_STATUS_CONDITION + " THEN 1 ELSE 0 END) AS paid_count, "
                        + "SUM(CASE WHEN order_status='refund' THEN 1 ELSE 0 END) AS refund_count "
                        + "FROM dwd_order_tmp "
                        + "GROUP BY region_id";

        Dataset<Row> aggregatedDf = spark.sql(aggregateSql);
        long count = aggregatedDf.count();
        log.info("[DWS] 聚合完成，汇总行数: {}", count);

        // Step 3: 写入 DWS Hive 表
        writeToDws(aggregatedDf);

        // Step 4: 清理临时视图
        spark.catalog().dropTempView("dwd_order_tmp");

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[DWS] 处理完成, 耗时: {}ms", elapsed);
    }

    private void writeToDws(Dataset<Row> df) {
        DataSources.hive()
                .option(o -> o.setDatabase("dws")
                        .setWriteMode(SaveMode.Overwrite))
                .write(df, "dws_order_daily");
        log.info("[DWS] 写入Hive完成: {}", OrderStatsConfig.DWS_ORDER_DAILY);
    }
}
