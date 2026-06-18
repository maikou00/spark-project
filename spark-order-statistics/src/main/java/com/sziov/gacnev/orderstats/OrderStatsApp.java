package com.sziov.gacnev.orderstats;

import com.sziov.gacnev.utils.DateUtils;
import com.sziov.gacnev.utils.etl.DataQEUtils;
import com.sziov.gacnev.orderstats.ads.AdsProcessor;
import com.sziov.gacnev.orderstats.config.OrderStatsConfig;
import com.sziov.gacnev.orderstats.datasimulator.DataSimulator;
import com.sziov.gacnev.orderstats.dim.DimProcessor;
import com.sziov.gacnev.orderstats.dwd.DwdProcessor;
import com.sziov.gacnev.orderstats.dws.DwsProcessor;
import com.sziov.gacnev.orderstats.ods.OdsProcessor;
import com.sziov.gacnev.utils.spark.SparkEnvUtils;
import com.sziov.gacnev.utils.spark.SparkParameterTool;
import com.sziov.gacnev.utils.spark.SparkSqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Properties;

/**
 * 订单统计模块入口。按数仓分层（ODS→DWD→DIM→DWS→ADS）编排全链路任务。
 *
 * <p>参数：
 * <ul>
 *   <li>{@code --date yyyy-MM-dd}：业务日期，默认当天</li>
 *   <li>{@code --reset}：重置所有数仓数据库和表</li>
 * </ul>
 *
 * @author maikou
 * @since 2026-06-09
 */
@Slf4j
public final class OrderStatsApp {

    private OrderStatsApp() {}

    public static void main(String[] args) {
        long totalStart = System.currentTimeMillis();

        // 1. 初始化 SparkSession
        SparkSession spark = SparkEnvUtils.prepare(args, "OrderStatistics");
        Properties params = SparkParameterTool.fromArgs(args);
        String dt = SparkParameterTool.get(params, "date", DateUtils.getCurrentDate());
        boolean reset = Boolean.parseBoolean(SparkParameterTool.get(params, "reset", "false"));

        try {
            // 2. 重置模式：删除所有库表
            if (reset) {
                resetAll(spark);
                log.info("重置完成，程序退出");
                return;
            }

            log.info("========== 订单统计任务开始，日期: {} ==========", dt);

            // 3. 初始化数据库和建表
            initDatabases(spark);

            // 4. ODS 层：生成模拟数据
            log.info("===== Phase 1: 数据模拟 =====");
            DataSimulator.generate(spark, dt);

            // 5. ODS 层：读取并校验
            log.info("===== Phase 2: ODS 读取校验 =====");
            OdsProcessor odsProcessor = new OdsProcessor(spark, dt);
            Dataset<Row> odsOrderEventDf = odsProcessor.readOrderEvents();
            Dataset<Row> odsUserDf = odsProcessor.readUsers();
            Dataset<Row> odsProductDf = odsProcessor.readProducts();
            Dataset<Row> odsStoreDf = odsProcessor.readStores();
            Dataset<Row> odsRegionDf = odsProcessor.readRegions();

            // 6. DWD 层：JSON 解析 + 脏数据过滤
            log.info("===== Phase 3: DWD 明细处理 =====");
            DwdProcessor dwdProcessor = new DwdProcessor(spark, dt);
            dwdProcessor.process(odsOrderEventDf);

            // 7. DIM 层：维度去重覆盖
            log.info("===== Phase 4: DIM 维度处理 =====");
            DimProcessor dimProcessor = new DimProcessor(spark, dt);
            dimProcessor.processUserDim(odsUserDf);
            dimProcessor.processProductDim(odsProductDf);
            dimProcessor.processStoreDim(odsStoreDf);
            dimProcessor.processRegionDim(odsRegionDf);

            // 8. DWS 层：多粒度汇总
            log.info("===== Phase 5: DWS 汇总处理 =====");
            DwsProcessor dwsProcessor = new DwsProcessor(spark, dt);
            dwsProcessor.process();

            // 9. ADS 层：KPI 计算
            log.info("===== Phase 6: ADS KPI计算 =====");
            AdsProcessor adsProcessor = new AdsProcessor(spark, dt);
            adsProcessor.process();

            long totalElapsed = System.currentTimeMillis() - totalStart;
            log.info("========== 订单统计任务完成，总耗时: {}ms ==========", totalElapsed);

        } catch (Exception e) {
            log.error("订单统计任务执行失败", e);
            throw new RuntimeException("订单统计任务执行失败", e);
        } finally {
            spark.stop();
            log.info("SparkSession已关闭");
        }
    }

    // ==================== 初始化 ====================

    /**
     * 创建所有分层数据库。
     */
    private static void initDatabases(SparkSession spark) {
        log.info("初始化数仓数据库...");
        for (String db : OrderStatsConfig.DATABASES) {
            SparkSqlUtils.createDatabase(spark, db);
        }
        log.info("数据库初始化完成");

        // 清理残留的托管表目录（上次运行后warehouse目录可能残留）
        cleanupStaleWarehouseDirs(spark);

        // 建表 DDL
        String[] ddls = {
                OrderStatsConfig.DDL_ODS_ORDER_EVENT,
                OrderStatsConfig.DDL_ODS_USER,
                OrderStatsConfig.DDL_ODS_PRODUCT,
                OrderStatsConfig.DDL_ODS_STORE,
                OrderStatsConfig.DDL_ODS_REGION,
                OrderStatsConfig.DDL_DWD_ORDER_FACT,
                OrderStatsConfig.DDL_DIM_USER,
                OrderStatsConfig.DDL_DIM_PRODUCT,
                OrderStatsConfig.DDL_DIM_STORE,
                OrderStatsConfig.DDL_DIM_REGION,
                OrderStatsConfig.DDL_DWS_ORDER_DAILY,
                OrderStatsConfig.DDL_ADS_ORDER_KPI_DAILY
        };

        for (String ddl : ddls) {
            SparkSqlUtils.executeUpdate(spark, ddl);
        }
        log.info("所有Hive表创建完成");
    }

    /**
     * 清理残留的托管表 warehouse 目录。
     * 修复问题：嵌入式 Derby metastore 元数据丢失后，磁盘上 warehouse 目录可能残留，
     * 导致 CREATE TABLE IF NOT EXISTS 在 SessionCatalog.validateTableLocation 校验时失败。
     */
    private static void cleanupStaleWarehouseDirs(SparkSession spark) {
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            String warehouseDir = spark.conf().get("spark.sql.warehouse.dir");
            // 与 initDatabases 中 DDL 数组保持同步
            String[][] tableLocations = {
                    {"ods", "ods_order_event"},
                    {"ods", "ods_user"},
                    {"ods", "ods_product"},
                    {"ods", "ods_store"},
                    {"ods", "ods_region"},
                    {"dwd", "dwd_order_fact"},
                    {"dim", "dim_user"},
                    {"dim", "dim_product"},
                    {"dim", "dim_store"},
                    {"dim", "dim_region"},
                    {"dws", "dws_order_daily"},
                    {"ads", "ads_order_kpi_daily"}
            };

            for (String[] loc : tableLocations) {
                String db = loc[0];
                String table = loc[1];
                // 仅在 metastore 中表不存在时才清理磁盘目录
                if (!spark.catalog().tableExists(db, table)) {
                    Path tablePath = new Path(warehouseDir, db + ".db/" + table);
                    if (fs.exists(tablePath)) {
                        fs.delete(tablePath, true);
                        log.info("已清理残留托管表目录: {}", tablePath);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("清理残留 warehouse 目录时发生异常（不影响建表流程）: {}", e.getMessage());
        }
    }

    /**
     * 重置：删除所有数仓表，然后删除库。
     */
    private static void resetAll(SparkSession spark) {
        log.info("========== 重置模式：删除所有数仓表 ==========");

        String[] tables = {
                OrderStatsConfig.ADS_ORDER_KPI_DAILY,
                OrderStatsConfig.DWS_ORDER_DAILY,
                OrderStatsConfig.DIM_REGION,
                OrderStatsConfig.DIM_STORE,
                OrderStatsConfig.DIM_PRODUCT,
                OrderStatsConfig.DIM_USER,
                OrderStatsConfig.DWD_ORDER_FACT,
                OrderStatsConfig.ODS_REGION,
                OrderStatsConfig.ODS_STORE,
                OrderStatsConfig.ODS_PRODUCT,
                OrderStatsConfig.ODS_USER,
                OrderStatsConfig.ODS_ORDER_EVENT
        };

        for (String table : tables) {
            try {
                SparkSqlUtils.executeUpdate(spark, "DROP TABLE IF EXISTS " + table + " PURGE");
                log.info("已删除表: {}", table);
            } catch (Exception e) {
                log.warn("删除表失败（可能不存在）: {}", table);
            }
        }

        // 反向删除数据库（遵循依赖顺序）
        String[] dbs = {OrderStatsConfig.DB_ADS, OrderStatsConfig.DB_DWS,
                OrderStatsConfig.DB_DIM, OrderStatsConfig.DB_DWD, OrderStatsConfig.DB_ODS};
        for (String db : dbs) {
            try {
                SparkSqlUtils.dropDatabase(spark, db, true);
                log.info("已删除数据库: {}", db);
            } catch (Exception e) {
                log.warn("删除数据库失败（可能不存在）: {}", db);
            }
        }

        log.info("重置完成");
    }
}
