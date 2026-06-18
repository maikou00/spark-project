package com.sziov.gacnev.orderstats.constant;


/**
 * 订单统计模块常量配置：表名、字段名、Schema 定义。
 * <p>DDL 不在此维护，本地开发用 scripts/init-local.sql 一次性建表，
 * 生产环境 DDL 由运维侧独立管理。</p>
 *
 * @author maikou
 * @since 2026-06-09
 */
public final class OrderStatsConfig {

    private OrderStatsConfig() {
        throw new UnsupportedOperationException("常量类不允许实例化");
    }

    // ==================== 数据库名 ====================
    public static final String DB_ODS = "ods";
    public static final String DB_DWD = "dwd";
    public static final String DB_DIM = "dim";
    public static final String DB_DWS = "dws";
    public static final String DB_ADS = "ads";

    // ==================== 分区字段 ====================
    public static final String PART_DT = "dt";

    // ==================== ODS 表名 ====================
    public static final String ODS_ORDER_EVENT = "ods.ods_order_event";
    public static final String ODS_USER = "ods.ods_user";
    public static final String ODS_PRODUCT = "ods.ods_product";
    public static final String ODS_STORE = "ods.ods_store";
    public static final String ODS_REGION = "ods.ods_region";

    // ==================== DWD 表名 ====================
    public static final String DWD_ORDER_FACT = "dwd.dwd_order_fact";

    // ==================== DIM 表名 ====================
    public static final String DIM_USER = "dim.dim_user";
    public static final String DIM_PRODUCT = "dim.dim_product";
    public static final String DIM_STORE = "dim.dim_store";
    public static final String DIM_REGION = "dim.dim_region";

    // ==================== DWS 表名 ====================
    public static final String DWS_ORDER_DAILY = "dws.dws_order_daily";

    // ==================== ADS 表名（Doris） ====================
    public static final String ADS_ORDER_KPI_DAILY = "ads.ads_order_kpi_daily";

    // ==================== 事件类型常量 ====================
    public static final String EVENT_CREATE = "create";
    public static final String EVENT_PAY = "pay";
    public static final String EVENT_SHIP = "ship";
    public static final String EVENT_SIGN = "sign";
    public static final String EVENT_REFUND = "refund";

    // ==================== 维度类型常量 ====================
    public static final String DIM_TYPE_USER = "user";
    public static final String DIM_TYPE_PRODUCT = "product";
    public static final String DIM_TYPE_STORE = "store";
    public static final String DIM_TYPE_REGION = "region";

    // ==================== 已支付状态列表（SQL IN 语句用） ====================
    public static final String PAID_STATUSES = "'pay','ship','sign'";

    // ==================== DIM 表简单名（配合 DataSources API setDatabase 使用） ====================
    public static final String TBL_DIM_USER = "dim_user";
    public static final String TBL_DIM_PRODUCT = "dim_product";
    public static final String TBL_DIM_STORE = "dim_store";
    public static final String TBL_DIM_REGION = "dim_region";
}

