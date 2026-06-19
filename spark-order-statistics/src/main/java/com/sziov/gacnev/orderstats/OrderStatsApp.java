package com.sziov.gacnev.orderstats;

import java.util.Properties;

import com.sziov.gacnev.orderstats.datasimulator.DataSimulator;
import com.sziov.gacnev.orderstats.processor.AdsProcessor;
import com.sziov.gacnev.utils.pipeline.InitUtils;
import com.sziov.gacnev.utils.pipeline.PipelineUtils;
import com.sziov.gacnev.orderstats.constant.OrderStatsConfig;
import com.sziov.gacnev.orderstats.processor.DwdProcessor;
import com.sziov.gacnev.orderstats.processor.DwsProcessor;
import com.sziov.gacnev.orderstats.processor.OdsProcessor;
import com.sziov.gacnev.datasource.DataSources;import com.sziov.gacnev.utils.spark.SparkEnvUtils;
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
 * </ul>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class OrderStatsApp {

    private OrderStatsApp() {}

    public static void main(String[] args) {
        SparkSession spark = SparkEnvUtils.prepare(args, "OrderStatistics");
        try {
            Properties p = SparkParameterTool.fromArgs(args);
            PipelineUtils.execute(spark, p, OrderStatsApp::runPipeline);
        } finally {
            spark.stop();
        }
    }

    static void runPipeline(SparkSession spark, String dt) {
        long t = System.currentTimeMillis();
        log.info("订单统计任务启动, dt={}", dt);

        DataSimulator.generate(spark, dt);
        log.info("生产测试数据，dt={}", dt);

        OdsProcessor ods = new OdsProcessor(spark, dt);
        Dataset<Row> odsOrder = ods.readOrderEvents();

        new DwdProcessor(spark, dt).process(odsOrder);

        Dataset<Row> dwdDf = DataSources.json()
                .read(spark, OrderStatsConfig.DWD_ORDER_FACT + "/" + OrderStatsConfig.PART_DT + "=" + dt);
        dwdDf.cache();

        new DwsProcessor(spark, dt).process(dwdDf);

        Dataset<Row> dwsDf = DataSources.json()
                .read(spark, OrderStatsConfig.DWS_ORDER_DAILY + "/" + OrderStatsConfig.PART_DT + "=" + dt);
        dwsDf.cache();

        new AdsProcessor(spark, dt).process(dwsDf);

        dwsDf.unpersist();
        dwdDf.unpersist();

        log.info("订单统计任务完成, dt={} 耗时={}ms", dt, System.currentTimeMillis() - t);
    }
}
