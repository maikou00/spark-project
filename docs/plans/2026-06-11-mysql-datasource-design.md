# MySQL 数据源设计文档

**日期**: 2026-06-11
**作者**: maikou
**状态**: 设计完成，待实现

---

## 1. 背景与目标

为 T+1 离线批处理场景新增 Spark 读写 MySQL 能力。MySQL 作为业务库数据源，每天从表全量/增量拉取数据到 Spark 处理，并将计算结果写回 MySQL 报表表。

现有 datasource 模块已实现 ClickHouse JDBC 连接器作为范本，MySQL 遵循同一套模式。

---

## 2. 技术选型

选择 **Spark 内置 JDBC 连接器**（`spark.read().jdbc()` / `df.write().jdbc()`）。理由：

- 与 ClickHouse 实现模式完全一致，维护成本最低
- Spark 3.3.2 JDBC 连接器支持分区读取、谓词下推
- `mysql-connector-java:8.0.28` 已在 `spark-common/pom.xml` 中，零额外依赖
- T+1 离线批处理场景性能足够——写入时开启 `rewriteBatchedStatements=true` 可将批量 INSERT 性能提升 5-10 倍

### 不选择的方案

| 方案 | 不选择原因 |
|---|---|
| 原生 JDBC（Connection + PreparedStatement） | 需手写 DataFrame→JDBC 转换，与现有 SPI 模式割裂 |
| 第三方 spark-jdbc 包 | 社区成熟度不足，引入额外依赖，与 ClickHouse 模式不一致 |

---

## 3. 架构分层

```
app.properties              配置层（jdbcUrl / user / password）
       │
       ▼
DataSources.mysql()  ─────── 统一入口（工厂方法 + SPI 自动注册）
       │
       ▼
DataSourceApi<MySqlOption> ─ 链式 API（.option().read/write）
       │
       ├── MySqlSource      读取（实现 DataSource + DataSourceProvider）
       └── MySqlSink        写入（实现 DataSink）
              │
              ▼
       Spark JDBC 连接器    底层（spark.read().jdbc / df.write().jdbc）
```

---

## 4. 接口契约

### 4.1 MySqlOption

```java
@Data @Accessors(chain = true)
@NoArgsConstructor @AllArgsConstructor
public class MySqlOption implements DataSourceOption<MySqlOption> {
    private String resource;           // 表名（必填）
    private String query;              // 自定义 SQL，优先级高于 resource
    private SaveMode writeMode;        // Append（默认） / Overwrite
    // 分区读取参数（可选）
    private String partitionColumn;    // 如 "id"
    private Long lowerBound;
    private Long upperBound;
    private Integer numPartitions;     // 默认 10
}
```

### 4.2 DataSourceType 枚举

新增 `MYSQL`。

### 4.3 app.properties 配置项

```properties
datasource.mysql.url=jdbc:mysql://host:3306/db?rewriteBatchedStatements=true&useSSL=false
datasource.mysql.username=root
datasource.mysql.password=xxx
```

### 4.4 常量（ParamsKeyConstant）

```java
DATASOURCE_MYSQL_URL = "datasource.mysql.url"
DATASOURCE_MYSQL_USERNAME = "datasource.mysql.username"
DATASOURCE_MYSQL_PASSWORD = "datasource.mysql.password"
```

---

## 5. 数据流

### 5.1 读取路径

```
1. DataSources.ensureInitialized() → 加载 app.properties
2. String jdbcUrl = config.get("datasource.mysql.url")
3. String user = config.get("datasource.mysql.username", "root")
4. String pwd  = config.get("datasource.mysql.password", "")
5. Properties jdbcProps = {user, password, driver}
6. String tableOrQuery = query != null ? "(query) t" : resource
7. if partitionColumn set:
     spark.read().jdbc(url, table, partitionColumn, lower, upper, numPartitions, props)
   else:
     spark.read().jdbc(url, table, props)
8. RetryUtils.retry(3, 1000ms) 包裹
```

### 5.2 写入路径

```
1. 读取连接配置（同读路径）
2. SaveMode mode = option.writeMode ?: Append
3. Append  → df.write().mode("append").jdbc(url, table, props)
4. Overwrite → TRUNCATE TABLE {table} + df.write().mode("append").jdbc(url, table, props)
5. RetryUtils.retry(3, 1000ms) 包裹
6. JDBC Properties 中包含：
   - rewriteBatchedStatements=true
   - useServerPrepStmts=false
   - batchsize=5000
```

---

## 6. 错误处理

| 场景 | 处理方式 |
|---|---|
| 连接失败 | `RetryUtils` 3 次指数退避 → `WarehouseException` |
| 配置缺失 | `DataSources.requireConfig()` → `WarehouseException` |
| 表不存在 | Spark JDBC 原生异常，直接上抛 |
| 写入冲突/主键冲突 | `WarehouseException` 包装 |
| 空配置 | `ensureInitialized()` 兜底从 classpath 加载 |

---

## 7. 调用示例

```java
// 读整表
Dataset<Row> df = DataSources.mysql()
    .option(o -> o.numPartitions(10).partitionColumn("id").lowerBound(1L).upperBound(100000L))
    .read(spark, "orders");

// 读自定义 SQL
Dataset<Row> df = DataSources.mysql()
    .option(o -> o.query("SELECT id,name FROM orders WHERE dt='2026-06-11'"))
    .read(spark, null);

// 追加写入
DataSources.mysql()
    .option(o -> o.writeMode(SaveMode.Append))
    .write(resultDf, "report_daily");

// 覆盖写入
DataSources.mysql()
    .option(o -> o.writeMode(SaveMode.Overwrite))
    .write(resultDf, "report_daily");
```

---

## 8. 新增/修改文件清单

| 文件 | 操作 |
|---|---|
| `DataSourceType.java` | 新增 MYSQL 枚举 |
| `MySqlOption.java` | 新增 |
| `MySqlSource.java` | 新增（实现 DataSource + DataSourceProvider） |
| `MySqlSink.java` | 新增 |
| `DataSources.java` | 新增 mysql() 工厂方法 + requireConfig 分支 |
| `ParamsKeyConstant.java` | 新增 MySQL 常量 |
| `META-INF/services/com.sziov.gacnev.datasource.DataSourceProvider` | 新增一行 com.sziov.gacnev.datasource.impl.MySqlSource |
| `app.properties` 模板 | 新增 MySQL 配置项 |

---

## 9. 测试策略

### 集成测试（保留，test 目录）

1. `MySqlSource.read()` — 验证表名和 SQL 两种读取模式
2. `MySqlSink.write(Append)` — 验证追加不覆盖已有数据
3. `MySqlSink.write(Overwrite)` — 验证先清空再写入
4. 配置缺失 — 验证抛 WarehouseException
5. 分区读取 — 验证 numPartitions 生效

### 开发流程

红 → 绿 → 重构：

1. 先写集成测试（红）
2. 写最小实现让测试通过（绿）
3. 消除重复，优化结构（重构）
4. 交付前删除单元测试，仅保留集成测试
