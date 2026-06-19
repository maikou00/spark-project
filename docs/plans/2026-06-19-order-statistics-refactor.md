# spark-order-statistics 架构改造设计文档

> 日期：2026-06-19  
> 版本：v3.1.0  
> 状态：已实现

---

## 1. 改造背景与目标

本次改造立足于**阿里巴巴离线数仓理论**，以生产可投产为标准，对 spark-order-statistics 进行全面架构升级。

| 维度 | 改造前 | 改造后 |
|------|--------|--------|
| **存储引擎** | Hive（需 Metastore） | JSON 文件 + MySQL + Doris |
| **维度管理** | Hive DIM 表全量覆盖 | MySQL 持久化 + 广播 Join，维度表不分区 |
| **JSON 解析** | StructType 手写 schema | POJO + Encoder 自动推导 |
| **包结构** | 按层分子包 | pojo / constant / processor / datasimulator |
| **通用工具** | 模块内 common 包 | spark-common/utils/pipeline（PipelineUtils、InitUtils） |
| **日期路由** | 仅支持 --date | --date 单天 / --start --end 补数 / 默认昨天，T+1 约束 |
| **DataSimulator** | 混入主链路 runPipeline | 独立调用，仅用于本地生成测试数据 |
| **ADS 数据链路** | ADS 基于 DWD 直算 | ADS 基于 DWS 汇总层计算 |
| **生产安全** | InitUtils 混入 main | 须本地模式 + --init 才执行，其他模式 WARN 跳过 |

---

## 2. 分层架构

```text
                            DataSimulator (独立调用)
                          │                      │
               generateOrderEvents()    generateUsers/Products
                          │               /Stores/Regions()
                          ▼                      │
          ┌──────────────────────┐   ┌──────────▼───────────┐
          │    ODS 层 (json)      │   │   MySQL 维度表         │
          │  ods_order_event     │   │  dim_user/product     │
          │  dt={date}/*.json    │   │  dim_store/region     │
          └──────────┬───────────┘   └──────────┬───────────┘
                     │                          │
            DataSources.json()          DataSources.mysql()
                     │                          │
                     ▼                          ▼
          ┌──────────────────────────────────────────────────┐
          │               DwdProcessor                       │
          │  from_json 解析 event_data → 广播 Join 维度表     │
          │  → event_id 非空过滤 → 重复数据去重               │
          │  → DataSources.json().write()                    │
          └────────────────────┬─────────────────────────────┘
                               │
                               ▼
          ┌──────────────────────────────────────────────────┐
          │            DWD 层 (json)                          │
          │  dwd_order_fact/dt={date}/*.json                  │
          └────────────────────┬─────────────────────────────┘
                               │
                      DataSources.json().read()
                               │
                               ▼
          ┌──────────────────────────────────────────────────┐
          │               DwsProcessor                        │
          │  GROUP BY user_id/product_id/store_id/region_id   │
          │  UNION ALL → 4 维度日度汇总                        │
          │  → DataSources.json().write()                     │
          └────────────────────┬─────────────────────────────┘
                               │
                               ▼
          ┌──────────────────────────────────────────────────┐
          │            DWS 层 (json)                          │
          │  dws_order_daily/dt={date}/*.json                 │
          └────────────────────┬─────────────────────────────┘
                               │
                      DataSources.json().read()
                               │
                               ▼
          ┌──────────────────────────────────────────────────┐
          │               AdsProcessor                        │
          │  dim_type='user' 汇总 → KPI 指标                   │
          │  → DataSources.doris().write(ads_order_kpi_daily) │
          └────────────────────┬─────────────────────────────┘
                               │
                               ▼
                       ┌───────────────┐
                       │  Doris ADS    │
                       │  UNIQUE KEY   │
                       │  (dt)         │
                       └───────────────┘
```

---

## 3. 模块结构

### 3.1 spark-order-statistics 包结构

```
com.sziov.gacnev.orderstats/
├── OrderStatsApp.java              # 入口，组装数据链路
├── constant/
│   └── OrderStatsConfig.java       # 仓库路径、事件常量、Doris 表名
├── pojo/
│   ├── OdsOrderEvent.java          # ODS 贴源层
│   ├── DwdOrderFact.java           # DWD 事实层（含维度富化字段）
│   ├── DwsOrderDaily.java          # DWS 日度汇总
│   ├── AdsOrderKpi.java            # ADS 核心 KPI (Doris)
│   ├── DimUser.java                # 用户维度
│   ├── DimProduct.java             # 商品维度
│   ├── DimStore.java               # 店铺维度
│   └── DimRegion.java              # 地区维度
├── processor/
│   ├── OdsProcessor.java           # 读 ODS JSON
│   ├── DwdProcessor.java           # JSON 解析 + 维度广播 Join + ETL
│   ├── DwsProcessor.java           # 4 维度 UNION ALL 聚合
│   └── AdsProcessor.java           # DWS → KPI → Doris
└── datasimulator/
    └── DataSimulator.java          # 独立调用，生成测试数据
```

### 3.2 spark-common 通用工具

| 类 | 位置 | 职责 |
|----|------|------|
| PipelineUtils | utils/pipeline/ | 日期解析、T+1 校验、单天/补数路由 |
| InitUtils | utils/pipeline/ | 本地模式 + --init 才建库建表，生产安全 |
| DataSources | datasource/ | 统一数据源工厂（json/mysql/doris...） |
| EtlUtils | utils/etl/ | filterNotNull、dropDuplicates |

---

## 4. 数据链路详解

### 4.1 ODS 层（贴源层）

**表结构**：`ods_order_event/dt={date}/*.json`

| 字段 | 类型 | 说明 |
|------|------|------|
| event_id | String | 事件 ID（允许空，脏数据标记） |
| event_type | String | 事件类型 (create/pay/ship/sign/refund) |
| event_data | String | 包裹的 JSON 订单数据（DWD 解析） |
| event_time | String | 事件时间 |
| dt | String | 分区日期 |

> ODS 不做任何过滤/验证，保持贴源特性，event_data 不解包直接存入。

### 4.2 DWD 层（明细事实层）

**数据来源**：ODS JSON + MySQL 维度表  
**处理流程**：
1. 过滤 event_id 为空的脏数据（EtlUtils.filterNotNull）
2. from_json(Encoders.bean(DwdOrderFact.class).schema()) 解析 event_data
3. 过滤 JSON 解析失败的脏数据
4. 4 次广播 Join 维度表（dim_user / dim_product / dim_store / dim_region）
5. 核心字段非空过滤（order_id / user_id / order_amount）
6. order_id 去重（EtlUtils.dropDuplicates）
7. DataSources.json().write(SaveMode.Overwrite) → 分区级最终一致

**维度富化字段**（来源于 MySQL）：
| 字段 | 来源表 |
|------|--------|
| du_name | dim_user.user_name |
| dp_name, dp_category | dim_product |
| ds_name, ds_type | dim_store |
| dr_name | dim_region.region_name |

### 4.3 DWS 层（日度汇总层）

**数据来源**：DWD JSON  
**处理流程**：
1. 注册临时视图 dwd_order_tmp
2. 单一 SQL 语句 GROUP BY + UNION ALL 生成 4 个维度汇总行
3. 维度类型常量：user / product / store / region
4. 汇总指标：order_count、total_amount、paid_count、refund_count

### 4.4 ADS 层（应用指标层）

**数据来源**：DWS JSON  
**目标**：Doris（ads_order_kpi_daily，UNIQUE KEY(dt)）  
**一致性语义**：SaveMode.Append → 至少一次，依赖 Doris UNIQUE KEY 实现幂等 upsert  
**计算规则**：取 dim_type='user' 的汇总行作为全局 KPI 基准，因为每个订单都有 user_id，不重复计数

| 指标 | 计算方式 |
|------|----------|
| totalOrders | SUM(order_count) |
| totalGmv | SUM(total_amount) |
| avgOrderAmount | totalGmv / totalOrders (保留 2 位小数) |
| paidOrders | SUM(paid_count) |
| paymentRate | paidOrders / totalOrders (保留 4 位小数) |
| refundOrders | SUM(refund_count) |
| refundRate | refundOrders / totalOrders (保留 4 位小数) |

---

## 5. POJO 设计

每层维护独立 POJO，POJO 属性名与 JSON 字段名一一对应，命名规范为小驼峰。
输出时通过 alias 将驼峰属性名映射为下划线列名以满足数仓规范。

### ODS 层 — OdsOrderEvent

| 字段 | 类型 | 说明 |
|------|------|------|
| eventId | String | 事件 ID |
| eventType | String | 事件类型 |
| eventData | String | JSON 订单数据（不解包） |
| eventTime | String | 事件时间 |
| dt | String | 分区日期 |

### DWD 层 — DwdOrderFact

| 字段 | 类型 | 来源 |
|------|------|------|
| orderId ~ refundTime | String / BigDecimal | from_json 解析 event_data |
| duName | String | dim_user 广播 Join → user_name |
| dpName / dpCategory | String | dim_product 广播 Join |
| dsName / dsType | String | dim_store 广播 Join |
| drName | String | dim_region 广播 Join → region_name |
| dt | String | 分区日期 |

> 仅用于 JSON 解析（Encoder.schema()），写入 DWD 时通过 alias 映射为下划线列名。

### DWS 层 — DwsOrderDaily

| 字段 | 类型 | 说明 |
|------|------|------|
| dimType | String | user/product/store/region |
| dimId | String | 维度 ID |
| orderCount | Long | 订单数 |
| totalAmount | BigDecimal | 总金额 |
| paidCount | Long | 已支付数 |
| refundCount | Long | 退单数 |
| dt | String | 分区日期 |

### ADS 层 — AdsOrderKpi

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | Date | 统计日期（Doris UNIQUE KEY） |
| totalOrders | Long | 总订单量 |
| totalGmv | BigDecimal | 总 GMV |
| avgOrderAmount | BigDecimal | 客单价 |
| paidOrders | Long | 已支付数 |
| paymentRate | BigDecimal | 支付转化率 |
| refundOrders | Long | 退单数 |
| refundRate | BigDecimal | 退单率 |

### 维度 POJO

| POJO | 字段 | 说明 |
|------|------|------|
| DimUser | userId, userName, phone, email, regionId, registerDate | 用户维度 |
| DimProduct | productId, productName, category, unitPrice | 商品维度 |
| DimStore | storeId, storeName, storeType, rating | 店铺维度 |
| DimRegion | regionId, regionName, parentRegionId, regionLevel | 地区维度 |

> 维度 POJO 不直接用于数据流转；DataSimulator 写 MySQL 时用 Row 方式直接构造。

---

## 6. 关键设计决策

### 6.1 日期路由 (PipelineUtils)

| 参数组合 | 行为 |
|----------|------|
| 无参数 | 跑昨天 (T-1) |
| --date yyyy-MM-dd | 跑指定日期 |
| --start yyyy-MM-dd --end yyyy-MM-dd | 补数模式，按天循环 |

**T+1 约束**：date/end 不得超过昨天，今天不跑（数据不完整）。传入 end 为今天则直接抛异常。
**补数模式**：单天失败不中断，跳过继续，最后汇总成功/失败数。

### 6.2 InitUtils 生产安全

- 仅在 **Spark 本地模式** (`local[*]`) **且** 参数含 `--init` 时执行建库建表
- 非本地模式 → WARN 日志 + 跳过
- 生产环境不会误执行，功能仅用于本地开发

### 6.3 DataSimulator 独立调用

- 不在主链路 runPipeline 中自动调用
- 单独执行 `DataSimulator.generate(spark, dt)` 生成测试数据
- ODS JSON 文件 + MySQL 维度数据一起生成，保证数据一致性

### 6.4 CSV/json 替代 Hive

- ODS/DWD/DWS 全部使用 `DataSources.json()` 读写本地文件
- 分区目录：`{path}/dt={date}/*.json`
- 一致性语义：`SaveMode.Overwrite` → 分区级最终一致

### 6.5 维度走 MySQL + 广播 Join

- DataSimulator 将维度数据写入 Docker MySQL（`SaveMode.Overwrite` 幂等）
- DwdProcessor 读取 4 张维度表，`broadcast()` 广播 Join 富化事实表
- 维度表规模很小（用户 100、商品 50、店铺 10、地区 14），适合广播
- 维度表不分区，不存在分区字段

### 6.6 POJO + Encoder 替代手写 Schema

- `from_json(col, Encoders.bean(DwdOrderFact.class).schema())` 自动推导 StructType
- 仅维护 POJO 的 Java 对象定义，无需重复维护 schema 字符串
- 解析后字段名为 POJO 属性名（驼峰），通过 `col("parsed.xxx").as("xxx")` 映射为下划线列名

### 6.7 ADS 基于 DWS 汇总层

- 取 `dim_type='user'` 的汇总行作为全局 KPI 基准
- 符合阿里巴巴数仓规范：ADS 基于 DWS 汇总层计算，不跨层查询 DWD

### 6.8 通用工具复用到 spark-common

- `PipelineUtils`、`InitUtils` 移至 `spark-common/utils/pipeline`
- 其他离线数仓模块可直接复用，无需重复开发日期解析和初始化逻辑

---

## 7. 文件变更清单

### 新增文件

| 文件 | 说明 |
|------|------|
| pojo/OdsOrderEvent.java | ODS 层 POJO |
| pojo/DwdOrderFact.java | DWD 事实层 POJO |
| pojo/DwsOrderDaily.java | DWS 日度汇总 POJO |
| pojo/AdsOrderKpi.java | ADS 核心 KPI POJO |
| pojo/DimUser.java | 用户维度 POJO |
| pojo/DimProduct.java | 商品维度 POJO |
| pojo/DimStore.java | 店铺维度 POJO |
| pojo/DimRegion.java | 地区维度 POJO |
| resources/scripts/init-mysql.sql | MySQL 建库建表脚本（替代原 init-local.sql） |

### 删除文件

| 文件 | 说明 |
|------|------|
| processor/DimProcessor.java | 维度表走 MySQL，不再需要 Hive DIM |
| schema/OrderStatsSchema.java | POJO + Encoder 替代手写 schema |
| resources/scripts/init-local.sql | 替换为 init-mysql.sql |

### 修改文件

| 文件 | 关键变更 |
|------|----------|
| OrderStatsApp.java | 去除 DimProcessor / InitUtils 自动调用；ADS 改为从 DWS 读取 |
| OrderStatsConfig.java | Hive 路径 → JSON 路径；新增 Doris 表名常量 |
| DataSimulator.java | 维度写 MySQL；ODS 写 JSON；内置常量本地化 |
| OdsProcessor.java | 简化 DataSources.json().read() |
| DwdProcessor.java | from_json 解析 + 广播 Join + ETL |
| DwsProcessor.java | GROUP BY + UNION ALL SQL |
| AdsProcessor.java | DWS 源 → Doris (DataSources.doris()) |
| app.properties (spark-order-statistics) | 精简配置 |
| app.properties (spark-common) | Doris fe_nodes 地址更新 |

---

## 8. 运行方式

```bash
# 1. 创建 MySQL 维度表
docker exec -i mysql mysql -uroot spark_test < \
  spark-order-statistics/src/main/resources/scripts/init-mysql.sql

# 2. 编译
mvn package -pl spark-order-statistics -am -Pdev -DskipTests

# 3. 生成测试数据
java -cp "..." com.sziov.gacnev.orderstats.datasimulator.DataSimulator \
  --date 2026-06-18

# 4. 跑数仓链路（昨天）
java -cp "..." com.sziov.gacnev.orderstats.OrderStatsApp

# 5. 跑指定日期
java -cp "..." com.sziov.gacnev.orderstats.OrderStatsApp \
  --date 2026-06-17

# 6. 补数
java -cp "..." com.sziov.gacnev.orderstats.OrderStatsApp \
  --start 2026-06-15 --end 2026-06-17
```

---

## 9. 后续优化方向

- [ ] Doris 自动建表脚本（当前需手动创建 UNIQUE KEY 表）
- [ ] 维度表缓存策略优化，避免每次 DwdProcessor 都全量读 MySQL
- [ ] ODS 脏数据指标告警（空 event_id 占比、JSON 解析失败率）
- [ ] 补数模式支持断点续跑
