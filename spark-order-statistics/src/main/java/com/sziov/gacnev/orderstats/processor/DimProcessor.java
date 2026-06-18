package com.sziov.gacnev.orderstats.processor;

import com.sziov.gacnev.utils.etl.EtlUtils;
import com.sziov.gacnev.datasource.DataSources;
import org.apache.spark.sql.SaveMode;
import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DIM 层处理器：维度数据去重覆盖（SCD Type 1），写入 Hive DIM 层。
 * <p>一致性语义：SaveMode.Overwrite 为 <b>最终一致</b>。</p>
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

    public void processUserDim(Dataset<Row> odsUserDf) {
        processDim(odsUserDf, new String[]{"user_id", "user_name", "phone", "email"},
                new String[]{"user_id"}, OrderStatsConfig.TBL_DIM_USER, "register_date");
    }

    public void processProductDim(Dataset<Row> odsProductDf) {
        processDim(odsProductDf, new String[]{"product_id", "product_name", "category"},
                new String[]{"product_id"}, OrderStatsConfig.TBL_DIM_PRODUCT);
    }

    public void processStoreDim(Dataset<Row> odsStoreDf) {
        processDim(odsStoreDf, new String[]{"store_id", "store_name"},
                new String[]{"store_id"}, OrderStatsConfig.TBL_DIM_STORE);
    }

    public void processRegionDim(Dataset<Row> odsRegionDf) {
        processDim(odsRegionDf, new String[]{"region_id", "region_name"},
                new String[]{"region_id"}, OrderStatsConfig.TBL_DIM_REGION);
    }

    private void processDim(Dataset<Row> odsDf, String[] cleanCols, String[] dedupCols,
                            String tableName, String... extraDropCols) {
        long rawCount = odsDf.count();
        if (rawCount == 0) {
            log.warn("[DIM] {} ODS 源数据为空，跳过写入以避免覆盖现有维度数据", tableName);
            return;
        }
        Dataset<Row> cleaned = EtlUtils.cleanData(odsDf, cleanCols);
        Dataset<Row> deduped = EtlUtils.dropDuplicates(cleaned, dedupCols).drop("dt");
        for (String col : extraDropCols) {
            deduped = deduped.drop(col);
        }
        DataSources.hive()
                .option(o -> o.setDatabase(OrderStatsConfig.DB_DIM)
                        .setWriteMode(SaveMode.Overwrite))
                .write(deduped, tableName);
    }
}
