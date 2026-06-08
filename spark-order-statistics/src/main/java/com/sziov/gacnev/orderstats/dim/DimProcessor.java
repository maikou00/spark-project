package com.sziov.gacnev.orderstats.dim;

import com.sziov.gacnev.etl.EtlUtils;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DIM 层处理器：维度数据去重覆盖（SCD Type 1），写入 Hive DIM 层。
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class DimProcessor {

    private final SparkSession spark;
    private final String dt;

    public DimProcessor(SparkSession spark, String dt) {
        this.spark = spark;
        this.dt = dt;
    }

    /**
     * 处理用户维度：去重 → 写入 dim_user。
     *
     * @param odsUserDf ODS 用户 DataFrame
     */
    public void processUserDim(Dataset<Row> odsUserDf) {
        log.info("[DIM] 开始处理用户维度");
        long startTime = System.currentTimeMillis();
        Dataset<Row> cleaned = EtlUtils.cleanData(odsUserDf,
                new String[]{"user_id", "user_name", "phone", "email"});
        Dataset<Row> deduped = EtlUtils.dropDuplicates(cleaned, new String[]{"user_id"}).drop("dt");
        long count = deduped.count();
        writeToDim(deduped, OrderStatsConfig.DIM_USER, "user_id");
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[DIM] 用户维度处理完成，行数: {}, 耗时: {}ms", count, elapsed);
    }

    /**
     * 处理商品维度：去重 → 写入 dim_product。
     *
     * @param odsProductDf ODS 商品 DataFrame
     */
    public void processProductDim(Dataset<Row> odsProductDf) {
        log.info("[DIM] 开始处理商品维度");
        long startTime = System.currentTimeMillis();
        Dataset<Row> cleaned = EtlUtils.cleanData(odsProductDf,
                new String[]{"product_id", "product_name", "category"});
        Dataset<Row> deduped = EtlUtils.dropDuplicates(cleaned, new String[]{"product_id"}).drop("dt");
        long count = deduped.count();
        writeToDim(deduped, OrderStatsConfig.DIM_PRODUCT, "product_id");
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[DIM] 商品维度处理完成，行数: {}, 耗时: {}ms", count, elapsed);
    }

    /**
     * 处理店铺维度：去重 → 写入 dim_store。
     *
     * @param odsStoreDf ODS 店铺 DataFrame
     */
    public void processStoreDim(Dataset<Row> odsStoreDf) {
        log.info("[DIM] 开始处理店铺维度");
        long startTime = System.currentTimeMillis();
        Dataset<Row> cleaned = EtlUtils.cleanData(odsStoreDf,
                new String[]{"store_id", "store_name"});
        Dataset<Row> deduped = EtlUtils.dropDuplicates(cleaned, new String[]{"store_id"}).drop("dt");
        long count = deduped.count();
        writeToDim(deduped, OrderStatsConfig.DIM_STORE, "store_id");
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[DIM] 店铺维度处理完成，行数: {}, 耗时: {}ms", count, elapsed);
    }

    /**
     * 处理地区维度：去重 → 写入 dim_region。
     *
     * @param odsRegionDf ODS 地区 DataFrame
     */
    public void processRegionDim(Dataset<Row> odsRegionDf) {
        log.info("[DIM] 开始处理地区维度");
        long startTime = System.currentTimeMillis();
        Dataset<Row> cleaned = EtlUtils.cleanData(odsRegionDf,
                new String[]{"region_id", "region_name"});
        Dataset<Row> deduped = EtlUtils.dropDuplicates(cleaned, new String[]{"region_id"}).drop("dt");
        long count = deduped.count();
        writeToDim(deduped, OrderStatsConfig.DIM_REGION, "region_id");
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[DIM] 地区维度处理完成，行数: {}, 耗时: {}ms", count, elapsed);
    }

    private void writeToDim(Dataset<Row> df, String tableName, String dimKey) {
        DataSources.hive()
                .option(o -> o.setDatabase("dim")
                        .setWriteMode(SaveMode.Overwrite))
                .write(df, tableName);
        log.info("[DIM] 写入Hive完成: {}", tableName);
    }
}
