package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import com.sziov.gacnev.utils.etl.EtlUtils;
import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import com.sziov.gacnev.orderstats.schema.OrderStatsSchema;import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;

/**
 * DWD 层处理器：JSON 解析 + 脏数据过滤 → 写入 dwd_order_fact。
 * <p>一致性语义：分区级 Append + 前置 DROP PARTITION 为 <b>至少一次</b>（幂等）。</p>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class DwdProcessor {

    private final SparkSession spark;
    private final String dt;

    public DwdProcessor(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    public void process(Dataset<Row> odsOrderEventDf) {
        long startTime = System.currentTimeMillis();
        odsOrderEventDf.cache();
        long rawCount = odsOrderEventDf.count();

        // 过滤事件类型 + 空event_id
        Dataset<Row> filteredDf = odsOrderEventDf
                .filter(col("event_type").isin(
                        OrderStatsConfig.EVENT_CREATE,
                        OrderStatsConfig.EVENT_PAY,
                        OrderStatsConfig.EVENT_SHIP,
                        OrderStatsConfig.EVENT_SIGN,
                        OrderStatsConfig.EVENT_REFUND));
        Dataset<Row> validIdDf = EtlUtils.filterNotNull(filteredDf, "event_id");

        // JSON 解析 + 过滤解析失败
        Dataset<Row> parsedDf = validIdDf
                .withColumn("parsed", from_json(col("event_data"),
                        OrderStatsSchema.ORDER_EVENT));
        Dataset<Row> validJsonDf = parsedDf.filter(col("parsed").isNotNull());

        // 展开 + 核心字段非空过滤 + 去重
        Dataset<Row> expandedDf = validJsonDf
                .select(
                        col("parsed.order_id").as("order_id"),
                        col("parsed.user_id").as("user_id"),
                        col("parsed.product_id").as("product_id"),
                        col("parsed.store_id").as("store_id"),
                        col("parsed.region_id").as("region_id"),
                        col("parsed.order_amount").as("order_amount"),
                        col("parsed.order_status").as("order_status"),
                        col("parsed.create_time").as("create_time"),
                        col("parsed.pay_time").as("pay_time"),
                        col("parsed.ship_time").as("ship_time"),
                        col("parsed.sign_time").as("sign_time"),
                        col("parsed.refund_time").as("refund_time")
                );
        Dataset<Row> cleanedDf = EtlUtils.filterNotNull(expandedDf,
                new String[]{"order_id", "user_id", "order_amount"});
        Dataset<Row> dedupedDf = EtlUtils.dropDuplicates(cleanedDf, new String[]{"order_id"});
        dedupedDf.cache();

        // 写入 DWD
        spark.sql("ALTER TABLE " + OrderStatsConfig.DWD_ORDER_FACT
                + " DROP IF EXISTS PARTITION (dt='" + dt + "')");
        DataSources.hive()
                .option(o -> o.setDatabase("dwd")
                        .setWriteMode(SaveMode.Append))
                .write(dedupedDf.withColumn("dt", lit(dt)), "dwd_order_fact");

        long finalCount = dedupedDf.count();
        log.info("[DWD] dt={} 原始={} 有效={} 丢弃={}({}%) 耗时={}ms",
                dt, rawCount, finalCount, rawCount - finalCount,
                rawCount > 0 ? String.format("%.1f", (rawCount - finalCount) * 100.0 / rawCount) : "0",
                System.currentTimeMillis() - startTime);

        dedupedDf.unpersist();
        odsOrderEventDf.unpersist();
    }
}
