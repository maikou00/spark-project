-- ============================================================================
-- Order Statistics - 本地开发环境一次性初始化脚本
-- 使用方式: spark-sql -f scripts/init-local.sql
-- 生产环境 DDL 由运维侧独立管理，不依赖此脚本
-- ADS 层表由 Doris 独立管理，不在此脚本中创建
-- ============================================================================

CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dim;
CREATE DATABASE IF NOT EXISTS dws;

-- ==================== ODS 贴源层 ====================

CREATE TABLE IF NOT EXISTS ods.ods_order_event (
    event_id STRING COMMENT '事件ID',
    event_type STRING COMMENT '事件类型: create/pay/ship/sign/refund',
    event_data STRING COMMENT 'JSON订单数据',
    event_time STRING COMMENT '事件时间 yyyy-MM-dd HH:mm:ss'
)
COMMENT '订单事件贴源表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ods.ods_user (
    user_id STRING COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    phone STRING COMMENT '手机号',
    email STRING COMMENT '邮箱',
    register_date STRING COMMENT '注册日期',
    region_id STRING COMMENT '地区ID'
)
COMMENT '用户维度贴源表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ods.ods_product (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category STRING COMMENT '类目',
    unit_price DECIMAL(18,2) COMMENT '单价',
    stock INT COMMENT '库存'
)
COMMENT '商品维度贴源表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ods.ods_store (
    store_id STRING COMMENT '店铺ID',
    store_name STRING COMMENT '店铺名称',
    store_type STRING COMMENT '店铺类型: self/third',
    rating DECIMAL(3,1) COMMENT '评级'
)
COMMENT '店铺维度贴源表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ods.ods_region (
    region_id STRING COMMENT '地区ID',
    region_name STRING COMMENT '地区名称',
    parent_region_id STRING COMMENT '父地区ID',
    region_level STRING COMMENT '地区级别: province/city/district'
)
COMMENT '地区维度贴源表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- ==================== DWD 明细层 ====================

CREATE TABLE IF NOT EXISTS dwd.dwd_order_fact (
    order_id STRING COMMENT '订单ID',
    user_id STRING COMMENT '用户ID',
    product_id STRING COMMENT '商品ID',
    store_id STRING COMMENT '店铺ID',
    region_id STRING COMMENT '地区ID',
    order_amount DECIMAL(18,2) COMMENT '订单金额',
    order_status STRING COMMENT '订单状态',
    create_time STRING COMMENT '创建时间',
    pay_time STRING COMMENT '支付时间',
    ship_time STRING COMMENT '发货时间',
    sign_time STRING COMMENT '签收时间',
    refund_time STRING COMMENT '退款时间'
)
COMMENT '订单明细事实表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ==================== DIM 维度层（非分区，SCD Type 1 全量覆盖） ====================

CREATE TABLE IF NOT EXISTS dim.dim_user (
    user_id STRING COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    phone STRING COMMENT '手机号',
    email STRING COMMENT '邮箱',
    region_id STRING COMMENT '地区ID'
)
COMMENT '用户维度表'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS dim.dim_product (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category STRING COMMENT '类目',
    unit_price DECIMAL(18,2) COMMENT '单价',
    stock INT COMMENT '库存'
)
COMMENT '商品维度表'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS dim.dim_store (
    store_id STRING COMMENT '店铺ID',
    store_name STRING COMMENT '店铺名称',
    store_type STRING COMMENT '店铺类型: self/third',
    rating DECIMAL(3,1) COMMENT '评级'
)
COMMENT '店铺维度表'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS dim.dim_region (
    region_id STRING COMMENT '地区ID',
    region_name STRING COMMENT '地区名称',
    parent_region_id STRING COMMENT '父地区ID',
    region_level STRING COMMENT '地区级别: province/city/district'
)
COMMENT '地区维度表'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ==================== DWS 汇总层 ====================

CREATE TABLE IF NOT EXISTS dws.dws_order_daily (
    dim_type STRING COMMENT '维度类型: user/product/store/region',
    dim_id STRING COMMENT '维度ID',
    order_count BIGINT COMMENT '订单量',
    total_amount DECIMAL(18,2) COMMENT '总金额',
    paid_count BIGINT COMMENT '支付订单数',
    refund_count BIGINT COMMENT '退单数'
)
COMMENT '订单日度汇总表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
