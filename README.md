# Spark Project

Spark 离线数仓公共工程，基于 Java 8 + Spark 3.3.2。提供统一的数据源读写、环境管理、ETL 工具。

## 模块结构

```
spark-project/
├── pom.xml
├── spark-common/                    # 公共模块
│   └── src/main/java/com/sziov/gacnev/
│       ├── spark/                   # SparkSession管理、参数加载
│       ├── datasource/              # 数据源抽象层（核心）
│       │   ├── option/              #   各数据源配置类
│       │   └── impl/                #   各数据源实现（Hive/ClickHouse/MySQL/Redis/File…）
│       ├── common/                  # 工具（重试/JSON/日期/脱敏/异常）
│       ├── constant/                # 常量（Key/默认值/数仓分层）
│       ├── etl/                     # ETL/数据质量
│       └── meta/                    # Hive元数据/分区
└── spark-order-statistics/          # 订单统计业务模块
```

## 数据源（DataSource）

### 架构

```
DataSource<O> / DataSink<O>     ← 接口（read / write / upsert / execute）
       ↓
DataSourceOption<O>             ← 配置基类
       ↓
DataSourceApi<O>                ← 链式调用入口（.option().read() / .write() / .upsert() / .execute()）
       ↓
DataSources                     ← 统一工厂，SPI 自动发现扩展数据源
```

### 支持的读写操作

```java
// 读
DataSources.mysql().read(spark, "db.table");
DataSources.mysql().option(o -> o.setQuery("SELECT ...")).read(spark, null);
DataSources.mysql().option(o -> o.setPartitionColumn("id").setLowerBound(0L).setUpperBound(100000L)).read(spark, "db.table");
DataSources.mysql().option(o -> o.setPredicates(Arrays.asList("db.table.dt='2026-06-01'", "db.table.dt='2026-06-02'")))
    .read(spark, "db.table");

// 写
DataSources.mysql().write(df, "db.table");                                          // Append
DataSources.mysql().option(o -> o.setWriteMode(SaveMode.Overwrite)).write(df, "db.table");  // Overwrite

// UPSERT（ON DUPLICATE KEY UPDATE）
DataSources.mysql().option(o -> o.setUpsertKeys(Arrays.asList("id"))).upsert(df, "db.table");

// 执行任意SQL
DataSources.mysql().option(o -> o.setQuery("DELETE FROM db.table WHERE id=1")).execute();

// 其他数据源同理
DataSources.hive().read(spark, "orders");
DataSources.clickhouse().write(df, "ck_table");
DataSources.csv().option(o -> o.writeMode(SaveMode.Overwrite)).write(df, "/data/output");
```

### 添加新数据源

以 ClickHouse 为模板，需要创建/修改以下 5 处：

**1. 枚举** — `DataSourceType.java` 加一个字面量

```java
public enum DataSourceType { HIVE, CSV, JSON, PARQUET, ORC, TEXT, CLICKHOUSE, MYSQL, REDIS, KAFKA }
```

**2. 配置** — `option/XxxOption.java` 实现 `DataSourceOption<XxxOption>`

```java
@Data @Accessors(chain = true)
public class XxxOption implements DataSourceOption<XxxOption> {
    private String resource;
    // 其他读/写参数...
}
```

**3. 实现** — `impl/XxxSource.java` 实现 `DataSource<XxxOption>` + `DataSourceProvider`；`impl/XxxSink.java` 实现 `DataSink<XxxOption>`

```java
public class XxxSource implements DataSource<XxxOption>, DataSourceProvider {
    @Override public DataSourceType type() { return DataSourceType.XXX; }
    @Override public DataSource<?> createSource() { return this; }
    @Override public DataSink<?> createSink() { return new XxxSink(); }
    @Override public Dataset<Row> read(SparkSession spark, XxxOption options) { ... }
}
```

**4. 配置项** — `ParamsKeyConstant.java` 加 Key，`ParamsDefaultValue.java` 加默认值，`app.properties` 补配置段

**5. SPI** — `META-INF/services/com.sziov.gacnev.datasource.DataSourceProvider` 加一行 `com.sziov.gacnev.datasource.impl.XxxSource`

完成后 `DataSources.xxx()` 即可使用，无需改 `DataSources.java`（SPI 自动发现）。

## 构建与运行

```bash
mvn clean compile          # 编译
mvn clean test -DforkCount=0 -nsu  # 运行测试
mvn clean package -Pcluster -DskipTests  # 生产包
```

## 开发规约

- Java 8，Lombok 精简代码，Alibaba 开发规范
- 配置外部化 `app.properties`，禁止硬编码
- 异常统一用 `WarehouseException`
- 常量集中维护在 `ParamsKeyConstant` / `ParamsDefaultValue`
- 日志用 `@Slf4j`，禁止 `System.out`
