package com.sziov.gacnev.orderstats.dwd;

import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import com.sziov.gacnev.utils.etl.DataQEUtils;
import com.sziov.gacnev.utils.etl.EtlUtils;
import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;

/**
 * DWD 层处理器：JSON 解析 + 脏数据过滤 → 写入 dwd_order_fact。
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

    /**
     * 处理 ODS 订单事件数据：JSON 解析 → 字段展开 → 清洗 → 去重 → 写入 DWD。
     *
     * @param odsOrderEventDf ODS 订单事件 DataFrame
     */
    public void process(Dataset<Row> odsOrderEventDf) {
        long startTime = System.currentTimeMillis();
        long rawCount = odsOrderEventDf.count();
        log.info("[DWD] 处理前总行数: {}", rawCount);

        // Step 1: 仅保留 create/pay 事件，减少无关数据处理
        Dataset<Row> filteredDf = odsOrderEventDf
                .filter(col("event_type").isin(
                        OrderStatsConfig.EVENT_CREATE,
                        OrderStatsConfig.EVENT_PAY,
                        OrderStatsConfig.EVENT_SHIP,
                        OrderStatsConfig.EVENT_SIGN,
                        OrderStatsConfig.EVENT_REFUND));

        // Step 2: 过滤空 event_id（脏数据1）
        long beforeNullFilter = filteredDf.count();
        Dataset<Row> validIdDf = EtlUtils.filterNotNull(filteredDf, "event_id");
        long afterNullFilter = validIdDf.count();
        long dirtyNullId = beforeNullFilter - afterNullFilter;
        if (dirtyNullId > 0) {
            log.info("[DWD] 空event_id脏数据丢弃: {}", dirtyNullId);
        }

        // Step 3: JSON 解析 - 用 from_json + 自定义 Schema
        Dataset<Row> parsedDf = validIdDf
                .withColumn("parsed", from_json(col("event_data"),
                        OrderStatsConfig.ORDER_EVENT_SCHEMA));

        // Step 4: 过滤 JSON 解析失败的行（parsed 为 null 即脏数据2）
        long beforeJsonFilter = parsedDf.count();
        Dataset<Row> validJsonDf = parsedDf.filter(col("parsed").isNotNull());
        long afterJsonFilter = validJsonDf.count();
        long dirtyBadJson = beforeJsonFilter - afterJsonFilter;
        if (dirtyBadJson > 0) {
            log.info("[DWD] 格式错误JSON脏数据丢弃: {}", dirtyBadJson);
        }

        // Step 5: 展开 parsed 结构体为独立列
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

        // Step 6: 核心字段非空过滤（order_id/user_id/order_amount）
        Dataset<Row> cleanedDf = EtlUtils.filterNotNull(expandedDf,
                new String[]{"order_id", "user_id", "order_amount"});
        long cleanCount = cleanedDf.count();
        long dirtyCoreNull = afterJsonFilter - cleanCount;
        if (dirtyCoreNull > 0) {
            log.info("[DWD] 核心字段为空脏数据丢弃: {}", dirtyCoreNull);
        }

        // Step 7: 去重（按 order_id）
        Dataset<Row> dedupedDf = EtlUtils.dropDuplicates(cleanedDf, new String[]{"order_id"});
        long finalCount = dedupedDf.count();
        long duplicateCount = cleanCount - finalCount;
        if (duplicateCount > 0) {
            log.info("[DWD] 重复数据丢弃: {}", duplicateCount);
        }

        long totalDirty = dirtyNullId + dirtyBadJson + dirtyCoreNull + duplicateCount;
        log.info("[DWD] 总脏数据丢弃: {} ({:.1f}%), 有效数据: {}",
                totalDirty, rawCount > 0 ? totalDirty * 100.0 / rawCount : 0, finalCount);

        // Step 8: 写入 DWD Hive 表
        writeToDwd(dedupedDf.drop("dt"));

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[DWD] 处理完成, 耗时: {}ms, 写入行数: {}", elapsed, finalCount);
    }

    private void writeToDwd(Dataset<Row> df) {
        DataSources.hive()
                .option(o -> o.setDatabase("dwd")
                        .setWriteMode(SaveMode.Overwrite))
                .write(df, "dwd_order_fact");
        log.info("[DWD] 写入Hive完成: {}", OrderStatsConfig.DWD_ORDER_FACT);
    }
}
