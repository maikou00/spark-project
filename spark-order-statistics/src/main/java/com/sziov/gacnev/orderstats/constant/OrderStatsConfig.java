package com.sziov.gacnev.orderstats.constant;

/**
 * 订单统计模块常量配置。
 *
 * @author maikou
 * @since 2026-06-09
 */
public final class OrderStatsConfig {

    private OrderStatsConfig() {
        throw new UnsupportedOperationException("常量类不允许实例化");
    }

    // ==================== 仓库路径 ====================
    public static final String WAREHOUSE_BASE = "/tmp/spark-warehouse/order-statistics";
    public static final String ODS_ORDER_EVENT = WAREHOUSE_BASE + "/ods/ods_order_event";
    public static final String DWD_ORDER_FACT = WAREHOUSE_BASE + "/dwd/dwd_order_fact";
    public static final String DWS_ORDER_DAILY = WAREHOUSE_BASE + "/dws/dws_order_daily";

    // ==================== 分区字段 ====================
    public static final String PART_DT = "dt";

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

    // ==================== 已支付状态列表 ====================
    public static final String PAID_STATUSES = "'pay','ship','sign'";

    // ==================== Doris ADS 表名 ====================
    public static final String ADS_ORDER_KPI_DAILY = "ads.ads_order_kpi_daily";
}
