package com.sziov.gacnev.orderstats;

import java.util.Properties;

import com.sziov.gacnev.orderstats.processor.AdsProcessor;
import com.sziov.gacnev.utils.pipeline.InitUtils;
import com.sziov.gacnev.utils.pipeline.PipelineUtils;
import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import com.sziov.gacnev.orderstats.processor.DimProcessor;
import com.sziov.gacnev.orderstats.processor.DwdProcessor;
import com.sziov.gacnev.orderstats.processor.DwsProcessor;
import com.sziov.gacnev.orderstats.processor.OdsProcessor;
import com.sziov.gacnev.utils.spark.SparkEnvUtils;
import com.sziov.gacnev.utils.spark.SparkParameterTool;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * 订单统计模块入口。
 *
 * <p>参数：
 * <ul>
 *   <li>{@code --date yyyy-MM-dd}：单天执行，默认昨天</li>
 *   <li>{@code --start yyyy-MM-dd --end yyyy-MM-dd}：补数，按天循环，失败跳过</li>
 *   <li>{@code --init}：本地模式建库建表（仅 dev）</li>
 * </ul>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class OrderStatsApp {

    private static final String INIT_SQL = "scripts/init-local.sql";
    private static final String[] DATABASES = {OrderStatsConfig.DB_ODS, OrderStatsConfig.DB_DWD,
            OrderStatsConfig.DB_DIM, OrderStatsConfig.DB_DWS};

    private OrderStatsApp() {}

    public static void main(String[] args) {
        SparkSession spark = SparkEnvUtils.prepare(args, "OrderStatistics");
        try {
            Properties p = SparkParameterTool.fromArgs(args);
            if (InitUtils.initIfNeeded(spark, p, INIT_SQL, DATABASES)) return;
            PipelineUtils.execute(spark, p, OrderStatsApp::runPipeline);
        } finally {
            spark.stop();
        }
    }

    static void runPipeline(SparkSession spark, String dt) {
        long t = System.currentTimeMillis();
        log.info("订单统计任务启动, dt={}", dt);

        OdsProcessor ods = new OdsProcessor(spark, dt);
        Dataset<Row> odsOrder = ods.readOrderEvents();

        new DwdProcessor(spark, dt).process(odsOrder);

        DimProcessor dim = new DimProcessor(spark, dt);
        dim.processUserDim(ods.readUsers());
        dim.processProductDim(ods.readProducts());
        dim.processStoreDim(ods.readStores());
        dim.processRegionDim(ods.readRegions());

        Dataset<Row> dwdDf = spark.table(OrderStatsConfig.DWD_ORDER_FACT)
                .filter(col(OrderStatsConfig.PART_DT).equalTo(dt));
        dwdDf.cache();

        new DwsProcessor(spark, dt).process(dwdDf);
        new AdsProcessor(spark, dt).process(dwdDf);

        dwdDf.unpersist();

        log.info("订单统计任务完成, dt={} 耗时={}ms", dt, System.currentTimeMillis() - t);
    }
}
