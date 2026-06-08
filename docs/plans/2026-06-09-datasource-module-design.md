# 多数据源模块设计文档（修订版）

**日期**：2026-06-09  
**作者**：maikou  
**状态**：已确认（v2 — 合并到 spark-common）

---

## 一、架构决策

**废弃独立 spark-datasource 模块，合并到 spark-common，通过 Maven Profile 隔离外部 connector 依赖。**

### 理由
- 单模块构建路径最短，IDE 体验好
- 一个依赖搞定所有功能
- 通过 `datasource` Maven Profile 按需引入重量级 connector（ES/MongoDB/Redis）

---

## 二、工程结构

```
spark-common/
├── pom.xml
├── data/input/                          # 测试数据（不变）
└── src/
    ├── main/java/com/sziov/gacnev/
    │   ├── App.java                     # 入口
    │   ├── constant/                    # 常量（不变）
    │   │   ├── ParamsKeyConstant.java
    │   │   ├── ParamsDefaultValue.java
    │   │   └── WarehouseConstant.java
    │   ├── example/
    │   │   └── JsonToCsvExample.java
    │   ├── core/                        # 🆕 核心工具层（原 utils/ 重组）
    │   │   ├── spark/
    │   │   │   ├── SparkEnvUtils.java
    │   │   │   ├── SparkParameterTool.java
    │   │   │   └── SparkSqlUtils.java
    │   │   ├── common/
    │   │   │   ├── DateUtils.java
    │   │   │   ├── StringUtils.java
    │   │   │   ├── JsonUtils.java
    │   │   │   ├── RetryUtils.java
    │   │   │   ├── JdbcUtils.java
    │   │   │   └── WarehouseException.java
    │   │   ├── io/
    │   │   │   ├── ReadDataUtils.java
    │   │   │   ├── WriteDataUtils.java
    │   │   │   └── HdfsUtils.java
    │   │   ├── etl/
    │   │   │   ├── EtlUtils.java
    │   │   │   └── DataQEUtils.java
    │   │   └── meta/
    │   │       ├── HiveMetaUtils.java
    │   │       └── PartitionUtils.java
    │   └── datasource/                  # 🆕 数据源统一接口层
    │       ├── core/
    │       │   ├── DataSource.java
    │       │   ├── DataSink.java
    │       │   ├── ReadOptions.java
    │       │   ├── WriteOptions.java
    │       │   ├── DataSourceConfig.java
    │       │   ├── DataSourceType.java
    │       │   └── DataSourceFactory.java
    │       ├── hive/
    │       │   ├── HiveSource.java
    │       │   └── HiveSink.java
    │       ├── file/
    │       │   ├── CsvSource.java
    │       │   ├── CsvSink.java
    │       │   ├── JsonSource.java
    │       │   ├── JsonSink.java
    │       │   ├── ParquetSource.java
    │       │   ├── ParquetSink.java
    │       │   ├── OrcSource.java
    │       │   ├── OrcSink.java
    │       │   ├── TextSource.java
    │       │   └── TextSink.java
    │       ├── clickhouse/
    │       │   ├── ClickHouseConfig.java
    │       │   ├── ClickHouseSource.java
    │       │   └── ClickHouseSink.java
    │       ├── elasticsearch/
    │       │   ├── ElasticsearchConfig.java
    │       │   ├── ElasticsearchSource.java
    │       │   └── ElasticsearchSink.java
    │       ├── mongodb/
    │       │   ├── MongoConfig.java
    │       │   ├── MongoDBSource.java
    │       │   └── MongoDBSink.java
    │       ├── redis/
    │       │   ├── RedisConfig.java
    │       │   ├── RedisSource.java
    │       │   └── RedisSink.java
    │       └── kafka/
    │           ├── KafkaConfig.java
    │           ├── KafkaSource.java
    │           └── KafkaSink.java
    ├── main/resources/
    │   ├── app.properties
    │   └── log4j2.xml
    └── test/java/com/sziov/gacnev/
        ├── core/                        # 测试同步迁移
        │   ├── spark/SparkParameterToolTest.java
        │   ├── common/DateUtilsTest.java, StringUtilsTest.java, RetryUtilsTest.java
        │   └── io/ReadDataUtilsTest.java, EtlUtilsTest.java
        └── datasource/                  # 新增测试
            ├── core/DataSourceFactoryTest.java
            ├── hive/
            ├── file/
            ├── clickhouse/ClickHouseConfigTest.java
            ├── elasticsearch/ElasticsearchConfigTest.java
            ├── mongodb/MongoConfigTest.java
            ├── redis/RedisConfigTest.java
            └── kafka/KafkaConfigTest.java
```

---

## 三、DataSourceType 枚举

```java
public enum DataSourceType {
    // 数仓
    HIVE,
    // 文件格式
    FILE_CSV, FILE_JSON, FILE_PARQUET, FILE_ORC, FILE_TEXT,
    // 关系型数据库
    CLICKHOUSE,
    // 非关系型
    ELASTICSEARCH, MONGODB, REDIS,
    // 消息队列
    KAFKA
}
```

---

## 四、依赖策略（Maven Profile）

| 依赖 | 默认（dev） | datasource profile |
|------|------------|-------------------|
| Spark/Hadoop/Hive | provided | provided |
| clickhouse-jdbc | compile | compile |
| elasticsearch-spark-30 | — | provided |
| mongo-spark-connector | — | provided |
| spark-redis | — | provided |
| kafka-client | 内置（Spark 自带） | 内置 |

激活方式：`mvn compile -Pdatasource`

---

## 五、适配器模式

Hive 和 File 数据源通过 Adapter 实现统一接口，内部委托现有工具类：

```java
// HiveSource → 委托 ReadDataUtils
public class HiveSource implements DataSource {
    public Dataset<Row> read(SparkSession spark, ReadOptions options) {
        return ReadDataUtils.readHiveTable(spark, options.getDatabase(), options.getResource(), options.getPartitionFilter());
    }
}

// FileSource → 委托 ReadDataUtils  
public class CsvSource implements DataSource { ... }
public class JsonSource implements DataSource { ... }
```

不修改 `ReadDataUtils` / `WriteDataUtils` 的现有签名，保持向后兼容。

---

## 六、改造范围

### 新增
- `datasource/core/` — 6 个接口/配置/工厂类
- `datasource/hive/` — HiveSource + HiveSink
- `datasource/file/` — 10 个 File Source/Sink
- `datasource/clickhouse/` — 3 个（从 spark-datasource 迁移）
- `datasource/elasticsearch/` — 3 个（迁移）
- `datasource/mongodb/` — 3 个（迁移）
- `datasource/redis/` — 3 个（迁移）
- `datasource/kafka/` — 3 个（迁移）

### 移动（包重构）
- `utils/` 下 16 个类 → `core/spark/`, `core/common/`, `core/io/`, `core/etl/`, `core/meta/`
- 测试类同步移动

### 修改
- 所有 import 路径更新
- `ReadOptions` 增加 `database`, `partitionFilter`, `format` 字段
- `pom.xml` 新增 `datasource` profile

### 删除
- `spark-datasource/` 模块（整个目录）
- 父 `pom.xml` 中 `spark-datasource` 模块声明
