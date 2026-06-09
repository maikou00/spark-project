package com.sziov.gacnev.orderstats.config;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * 订单统计模块常量配置：表名、字段名、Schema 定义。
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
    public static final String DIM_DATE = "dim.dim_date";

    // ==================== DWS 表名 ====================
    public static final String DWS_ORDER_DAILY = "dws.dws_order_daily";

    // ==================== ADS 表名 ====================
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

    // ==================== DDL ====================

    public static final String[] DATABASES = {DB_ODS, DB_DWD, DB_DIM, DB_DWS, DB_ADS};

    public static final String DDL_ODS_ORDER_EVENT =
            "CREATE TABLE IF NOT EXISTS ods.ods_order_event (\n"
                    + "    event_id STRING COMMENT '事件ID',\n"
                    + "    event_type STRING COMMENT '事件类型: create/pay/ship/sign/refund',\n"
                    + "    event_data STRING COMMENT 'JSON订单数据',\n"
                    + "    event_time STRING COMMENT '事件时间 yyyy-MM-dd HH:mm:ss'\n"
                    + ")\n"
                    + "COMMENT '订单事件贴源表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                    + "STORED AS TEXTFILE";

    public static final String DDL_ODS_USER =
            "CREATE TABLE IF NOT EXISTS ods.ods_user (\n"
                    + "    user_id STRING COMMENT '用户ID',\n"
                    + "    user_name STRING COMMENT '用户名',\n"
                    + "    phone STRING COMMENT '手机号',\n"
                    + "    email STRING COMMENT '邮箱',\n"
                    + "    register_date STRING COMMENT '注册日期',\n"
                    + "    region_id STRING COMMENT '地区ID'\n"
                    + ")\n"
                    + "COMMENT '用户维度贴源表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                    + "STORED AS TEXTFILE";

    public static final String DDL_ODS_PRODUCT =
            "CREATE TABLE IF NOT EXISTS ods.ods_product (\n"
                    + "    product_id STRING COMMENT '商品ID',\n"
                    + "    product_name STRING COMMENT '商品名称',\n"
                    + "    category STRING COMMENT '类目',\n"
                    + "    unit_price DECIMAL(18,2) COMMENT '单价',\n"
                    + "    stock INT COMMENT '库存'\n"
                    + ")\n"
                    + "COMMENT '商品维度贴源表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                    + "STORED AS TEXTFILE";

    public static final String DDL_ODS_STORE =
            "CREATE TABLE IF NOT EXISTS ods.ods_store (\n"
                    + "    store_id STRING COMMENT '店铺ID',\n"
                    + "    store_name STRING COMMENT '店铺名称',\n"
                    + "    store_type STRING COMMENT '店铺类型: self/third',\n"
                    + "    rating DECIMAL(3,1) COMMENT '评级'\n"
                    + ")\n"
                    + "COMMENT '店铺维度贴源表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                    + "STORED AS TEXTFILE";

    public static final String DDL_ODS_REGION =
            "CREATE TABLE IF NOT EXISTS ods.ods_region (\n"
                    + "    region_id STRING COMMENT '地区ID',\n"
                    + "    region_name STRING COMMENT '地区名称',\n"
                    + "    parent_region_id STRING COMMENT '父地区ID',\n"
                    + "    region_level STRING COMMENT '地区级别: province/city/district'\n"
                    + ")\n"
                    + "COMMENT '地区维度贴源表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                    + "STORED AS TEXTFILE";

    public static final String DDL_DWD_ORDER_FACT =
            "CREATE TABLE IF NOT EXISTS dwd.dwd_order_fact (\n"
                    + "    order_id STRING COMMENT '订单ID',\n"
                    + "    user_id STRING COMMENT '用户ID',\n"
                    + "    product_id STRING COMMENT '商品ID',\n"
                    + "    store_id STRING COMMENT '店铺ID',\n"
                    + "    region_id STRING COMMENT '地区ID',\n"
                    + "    order_amount DECIMAL(18,2) COMMENT '订单金额',\n"
                    + "    order_status STRING COMMENT '订单状态',\n"
                    + "    create_time STRING COMMENT '下单时间',\n"
                    + "    pay_time STRING COMMENT '支付时间',\n"
                    + "    ship_time STRING COMMENT '发货时间',\n"
                    + "    sign_time STRING COMMENT '签收时间',\n"
                    + "    refund_time STRING COMMENT '退款时间'\n"
                    + ")\n"
                    + "COMMENT '订单事实明细宽表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "STORED AS ORC\n"
                    + "TBLPROPERTIES ('orc.compress'='SNAPPY')";

    public static final String DDL_DIM_USER =
            "CREATE TABLE IF NOT EXISTS dim.dim_user (\n"
                    + "    user_id STRING COMMENT '用户ID',\n"
                    + "    user_name STRING COMMENT '用户名',\n"
                    + "    phone STRING COMMENT '手机号',\n"
                    + "    email STRING COMMENT '邮箱',\n"
                    + "    register_date STRING COMMENT '注册日期',\n"
                    + "    region_id STRING COMMENT '地区ID'\n"
                    + ")\n"
                    + "COMMENT '用户维度表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "STORED AS ORC\n"
                    + "TBLPROPERTIES ('orc.compress'='SNAPPY')";

    public static final String DDL_DIM_PRODUCT =
            "CREATE TABLE IF NOT EXISTS dim.dim_product (\n"
                    + "    product_id STRING COMMENT '商品ID',\n"
                    + "    product_name STRING COMMENT '商品名称',\n"
                    + "    category STRING COMMENT '类目',\n"
                    + "    unit_price DECIMAL(18,2) COMMENT '单价',\n"
                    + "    stock INT COMMENT '库存'\n"
                    + ")\n"
                    + "COMMENT '商品维度表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "STORED AS ORC\n"
                    + "TBLPROPERTIES ('orc.compress'='SNAPPY')";

    public static final String DDL_DIM_STORE =
            "CREATE TABLE IF NOT EXISTS dim.dim_store (\n"
                    + "    store_id STRING COMMENT '店铺ID',\n"
                    + "    store_name STRING COMMENT '店铺名称',\n"
                    + "    store_type STRING COMMENT '店铺类型: self/third',\n"
                    + "    rating DECIMAL(3,1) COMMENT '评级'\n"
                    + ")\n"
                    + "COMMENT '店铺维度表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "STORED AS ORC\n"
                    + "TBLPROPERTIES ('orc.compress'='SNAPPY')";

    public static final String DDL_DIM_REGION =
            "CREATE TABLE IF NOT EXISTS dim.dim_region (\n"
                    + "    region_id STRING COMMENT '地区ID',\n"
                    + "    region_name STRING COMMENT '地区名称',\n"
                    + "    parent_region_id STRING COMMENT '父地区ID',\n"
                    + "    region_level STRING COMMENT '地区级别: province/city/district'\n"
                    + ")\n"
                    + "COMMENT '地区维度表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "STORED AS ORC\n"
                    + "TBLPROPERTIES ('orc.compress'='SNAPPY')";

    public static final String DDL_DWS_ORDER_DAILY =
            "CREATE TABLE IF NOT EXISTS dws.dws_order_daily (\n"
                    + "    dim_type STRING COMMENT '维度类型: user/product/store/region',\n"
                    + "    dim_id STRING COMMENT '维度ID',\n"
                    + "    order_count BIGINT COMMENT '订单量',\n"
                    + "    total_amount DECIMAL(18,2) COMMENT '总金额',\n"
                    + "    paid_count BIGINT COMMENT '支付订单数',\n"
                    + "    refund_count BIGINT COMMENT '退单数'\n"
                    + ")\n"
                    + "COMMENT '订单日度汇总表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "STORED AS ORC\n"
                    + "TBLPROPERTIES ('orc.compress'='SNAPPY')";

    public static final String DDL_ADS_ORDER_KPI_DAILY =
            "CREATE TABLE IF NOT EXISTS ads.ads_order_kpi_daily (\n"
                    + "    total_orders BIGINT COMMENT '总订单量',\n"
                    + "    total_gmv DECIMAL(18,2) COMMENT '总GMV',\n"
                    + "    avg_order_amount DECIMAL(18,2) COMMENT '客单价',\n"
                    + "    paid_orders BIGINT COMMENT '已支付订单数',\n"
                    + "    payment_rate DECIMAL(5,4) COMMENT '支付转化率',\n"
                    + "    refund_orders BIGINT COMMENT '退单数',\n"
                    + "    refund_rate DECIMAL(5,4) COMMENT '退单率'\n"
                    + ")\n"
                    + "COMMENT '订单核心KPI日表'\n"
                    + "PARTITIONED BY (dt STRING COMMENT '分区日期')\n"
                    + "STORED AS ORC\n"
                    + "TBLPROPERTIES ('orc.compress'='SNAPPY')";

    // ==================== Schema 定义 ====================

    /**
     * 订单事件 JSON 解析 Schema（对应 event_data 字段）。\
     */
    public static final StructType ORDER_EVENT_SCHEMA = new StructType()
            .add("order_id", DataTypes.StringType, false)
            .add("user_id", DataTypes.StringType, false)
            .add("product_id", DataTypes.StringType, false)
            .add("store_id", DataTypes.StringType, false)
            .add("region_id", DataTypes.StringType, false)
            .add("order_amount", DataTypes.createDecimalType(18, 2), false)
            .add("order_status", DataTypes.StringType, false)
            .add("create_time", DataTypes.StringType, true)
            .add("pay_time", DataTypes.StringType, true)
            .add("ship_time", DataTypes.StringType, true)
            .add("sign_time", DataTypes.StringType, true)
            .add("refund_time", DataTypes.StringType, true);

    /**
     * DWD 订单事实输出 Schema，与 DDL 对应。\
     */
    public static final StructType DWD_ORDER_FACT_SCHEMA = new StructType()
            .add("order_id", DataTypes.StringType, false)
            .add("user_id", DataTypes.StringType, false)
            .add("product_id", DataTypes.StringType, false)
            .add("store_id", DataTypes.StringType, false)
            .add("region_id", DataTypes.StringType, false)
            .add("order_amount", DataTypes.createDecimalType(18, 2), false)
            .add("order_status", DataTypes.StringType, false)
            .add("create_time", DataTypes.StringType, true)
            .add("pay_time", DataTypes.StringType, true)
            .add("ship_time", DataTypes.StringType, true)
            .add("sign_time", DataTypes.StringType, true)
            .add("refund_time", DataTypes.StringType, true);

    /**
     * DWS 订单日度汇总 Schema。\
     */
    public static final StructType DWS_ORDER_DAILY_SCHEMA = new StructType()
            .add("dim_type", DataTypes.StringType, false)
            .add("dim_id", DataTypes.StringType, false)
            .add("order_count", DataTypes.LongType, false)
            .add("total_amount", DataTypes.createDecimalType(18, 2), false)
            .add("paid_count", DataTypes.LongType, false)
            .add("refund_count", DataTypes.LongType, false);

    /**
     * ADS KPI 日表 Schema。\
     */
    public static final StructType ADS_ORDER_KPI_SCHEMA = new StructType()
            .add("total_orders", DataTypes.LongType, false)
            .add("total_gmv", DataTypes.createDecimalType(18, 2), false)
            .add("avg_order_amount", DataTypes.createDecimalType(18, 2), false)
            .add("paid_orders", DataTypes.LongType, false)
            .add("payment_rate", DataTypes.createDecimalType(5, 4), false)
            .add("refund_orders", DataTypes.LongType, false)
            .add("refund_rate", DataTypes.createDecimalType(5, 4), false);
}
