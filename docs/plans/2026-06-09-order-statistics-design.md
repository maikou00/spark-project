# 订单统计模块设计文档

- **日期**：2026-06-09
- **作者**：maikou
- **版本**：1.0.0

## 1. 概述

新增 `spark-order-statistics` 模块，实现电商订单全链路数仓统计。数据从 ODS 层 JSON 格式读取，按 ODS → DWD → DIM → DWS → ADS 分层处理，最终写入 Hive。

### 核心设计原则

- **Schema 显式定义**：所有表用 `StructType` 硬编码，不依赖 `inferSchema`
- **DataFrame 处理链**：全程 DataFrame/Dataset 操作 + SQL 聚合
- **配置驱动**：运行模式/Hive集成通过 `app.properties` 配置
- **生产级**：DDL 自动建表、分区管理、数据质量报告、幂等写入、异常分类、资源释放

## 2. 数据模型

### 2.1 ODS 层（贴源层，TEXTFILE）

#### ods_order_event

| 字段 | 类型 | 说明 |
|------|------|------|
| event_id | STRING | 事件ID |
| event_type | STRING | 事件类型：create/pay/ship/sign/refund |
| event_data | STRING | JSON 正文 |
| event_time | STRING | 事件时间 |
| dt | STRING | 分区字段 |

#### ods_user / ods_product / ods_store / ods_region

维度贴源表，均为原始快照。

### 2.2 DWD 层（明细层，ORC+SNAPPY）

#### dwd_order_fact

| 字段 | 类型 |
|------|------|
| order_id | STRING |
| user_id | STRING |
| product_id | STRING |
| store_id | STRING |
| region_id | STRING |
| order_amount | DECIMAL(18,2) |
| order_status | STRING |
| create_time | TIMESTAMP |
| pay_time | TIMESTAMP |
| ship_time | TIMESTAMP |
| sign_time | TIMESTAMP |
| refund_time | TIMESTAMP |
| dt | STRING |

### 2.3 DIM 层（维度层，ORC+SNAPPY）

dim_user / dim_product / dim_store / dim_region / dim_date，全量快照，dt 分区。

### 2.4 DWS 层（汇总层，ORC+SNAPPY）

#### dws_order_daily

粒度：dt + user_id / product_id / store_id / region_id

| 字段 | 类型 |
|------|------|
| dt | STRING |
| dim_type | STRING（user/product/store/region） |
| dim_id | STRING |
| order_count | BIGINT |
| total_amount | DECIMAL(18,2) |
| paid_count | BIGINT |
| refund_count | BIGINT |

### 2.5 ADS 层（应用层，ORC+SNAPPY）

#### ads_order_kpi_daily

| 字段 | 类型 |
|------|------|
| dt | STRING |
| total_orders | BIGINT |
| total_gmv | DECIMAL(18,2) |
| avg_order_amount | DECIMAL(18,2) |
| paid_orders | BIGINT |
| payment_rate | DECIMAL(5,4) |
| refund_orders | BIGINT |
| refund_rate | DECIMAL(5,4) |

## 3. 模块结构

```
spark-order-statistics/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/sziov/gacnev/orderstats/
    │   │   ├── OrderStatsApp.java
    │   │   ├── config/OrderStatsConfig.java
    │   │   ├── datasimulator/DataSimulator.java
    │   │   ├── ods/OdsProcessor.java
    │   │   ├── dwd/DwdProcessor.java
    │   │   ├── dim/DimProcessor.java
    │   │   ├── dws/DwsProcessor.java
    │   │   └── ads/AdsProcessor.java
    │   └── resources/
    │       └── app.properties
    └── test/
        └── java/com/sziov/gacnev/orderstats/
            ├── DataSimulatorTest.java
            ├── DwdProcessorTest.java
            ├── DwsProcessorTest.java
            └── AdsProcessorTest.java
```

## 4. 处理链路

```
OrderStatsApp.prepare(args)
  → initDatabases()        -- 创建 ods/dwd/dim/dws/ads 库
  → DataSimulator.generate(dt)  -- 模拟数据生成 + 写入 ODS
  → OdsProcessor.process(dt)    -- 读取 ODS，校验行数
  → DwdProcessor.process(dt)    -- JSON 解析 + 脏数据过滤 → DWD
  → DimProcessor.process(dt)    -- 维表去重覆盖 → DIM
  → DwsProcessor.process(dt)    -- 多粒度聚合 → DWS
  → AdsProcessor.process(dt)    -- KPI 计算 → ADS
  → finally: sparkSession.stop()
```

## 5. 配置

```properties
# spark-order-statistics/src/main/resources/app.properties
spark.app.name=OrderStatistics
spark.local=true
spark.hive.enabled=true
spark.sql.warehouse.dir=/tmp/spark-warehouse/order-statistics
spark.sql.catalogImplementation=hive
spark.log.level=WARN
```

## 6. 生产特性

| 特性 | 实现方式 |
|------|---------|
| DDL 自动建表 | Processor.init() 执行 CREATE TABLE IF NOT EXISTS |
| 分区管理 | PartitionUtils.addPartition() + INSERT OVERWRITE |
| 数据质量 | DataQEUtils.printQualityReport() |
| 脏数据追踪 | DWD 解析失败 + 核心字段空值 日志计数 |
| 执行追踪 | 每层行数变化 + 耗时日志 |
| 幂等写入 | INSERT OVERWRITE PARTITION |
| 异常处理 | WarehouseException + try-finally 释放 |
| 重置能力 | --reset 参数 DROP TABLE PURGE + DROP DATABASE CASCADE |
