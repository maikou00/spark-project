package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import com.sziov.gacnev.utils.etl.EtlUtils;
import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * DWD 层处理器：JSON 解析 + MySQL 维度广播 Join → 写入 DWD。
 * <p>一致性语义：Overwrite 分区目录为 <b>最终一致</b>。</p>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class DwdProcessor {

    private static final StructType ORDER_EVENT_SCHEMA = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("user_id", DataTypes.StringType)
            .add("product_id", DataTypes.StringType)
            .add("store_id", DataTypes.StringType)
            .add("region_id", DataTypes.StringType)
            .add("order_amount", DataTypes.StringType)
            .add("order_status", DataTypes.StringType)
            .add("create_time", DataTypes.StringType)
            .add("pay_time", DataTypes.StringType)
            .add("ship_time", DataTypes.StringType)
            .add("sign_time", DataTypes.StringType)
            .add("refund_time", DataTypes.StringType);

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

        // 读取 MySQL 维度表并广播
        Dataset<Row> dimUserDf = DataSources.mysql().read(spark, "spark_test.dim_user");
        Dataset<Row> dimProductDf = DataSources.mysql().read(spark, "spark_test.dim_product");
        Dataset<Row> dimStoreDf = DataSources.mysql().read(spark, "spark_test.dim_store");
        Dataset<Row> dimRegionDf = DataSources.mysql().read(spark, "spark_test.dim_region");

        Dataset<Row> validIdDf = EtlUtils.filterNotNull(odsOrderEventDf, "event_id");

        Dataset<Row> parsedDf = validIdDf
                .withColumn("parsed", from_json(col("event_data"), ORDER_EVENT_SCHEMA));
        Dataset<Row> validJsonDf = parsedDf.filter(col("parsed").isNotNull());

        Dataset<Row> expandedDf = validJsonDf.select(
                col("parsed.order_id"),
                col("parsed.user_id"),
                col("parsed.product_id"),
                col("parsed.store_id"),
                col("parsed.region_id"),
                col("parsed.order_amount"),
                col("parsed.order_status"),
                col("parsed.create_time"),
                col("parsed.pay_time"),
                col("parsed.ship_time"),
                col("parsed.sign_time"),
                col("parsed.refund_time")
        );

        Dataset<Row> enrichedDf = expandedDf
                .join(broadcast(dimUserDf.select(
                        col("user_id").as("du_id"), col("user_name").as("du_name"))),
                        expandedDf.col("user_id").equalTo(col("du_id")), "left")
                .drop("du_id")
                .join(broadcast(dimProductDf.select(
                        col("product_id").as("dp_id"), col("product_name").as("dp_name"),
                        col("category").as("dp_category"))),
                        expandedDf.col("product_id").equalTo(col("dp_id")), "left")
                .drop("dp_id")
                .join(broadcast(dimStoreDf.select(
                        col("store_id").as("ds_id"), col("store_name").as("ds_name"),
                        col("store_type").as("ds_type"))),
                        expandedDf.col("store_id").equalTo(col("ds_id")), "left")
                .drop("ds_id")
                .join(broadcast(dimRegionDf.select(
                        col("region_id").as("dr_id"), col("region_name").as("dr_name"))),
                        expandedDf.col("region_id").equalTo(col("dr_id")), "left")
                .drop("dr_id");

        Dataset<Row> cleanedDf = EtlUtils.filterNotNull(enrichedDf,
                new String[]{"order_id", "user_id", "order_amount"});
        Dataset<Row> dedupedDf = EtlUtils.dropDuplicates(cleanedDf, new String[]{"order_id"});
        dedupedDf.cache();

        String dwdPath = OrderStatsConfig.DWD_ORDER_FACT + "/" + OrderStatsConfig.PART_DT + "=" + dt;
        DataSources.json()
                .option(o -> o.setWriteMode(SaveMode.Overwrite))
                .write(dedupedDf.withColumn("dt", lit(dt)), dwdPath);

        long finalCount = dedupedDf.count();
        log.info("[DWD] dt={} 原始={} 有效={} 丢弃={}({}%) 耗时={}ms",
                dt, rawCount, finalCount, rawCount - finalCount,
                rawCount > 0 ? String.format("%.1f", (rawCount - finalCount) * 100.0 / rawCount) : "0",
                System.currentTimeMillis() - startTime);

        dedupedDf.unpersist();
        odsOrderEventDf.unpersist();
    }
}
