# Doris 数据源集成设计文档

**日期**：2026-06-12
**作者**：maikou
**状态**：已实现

---

## 一、背景

车联网场景下需在 datasource 模块新增 Apache Doris 数据源支持。

## 二、技术选型

| 方案 | 描述 | 结论 |
|------|------|------|
| JDBC | 复用 `spark.read().jdbc()`，Doris 兼容 MySQL 协议 | ❌ 读写均经过 FE 单节点，大数据量时 FE 成为瓶颈 |
| Spark-Doris-Connector | 官方连接器，读直连 BE 并行扫描，写走 Stream Load 批量吞吐 | ✅ 车联网高吞吐场景首选 |

**依赖**：`spark-doris-connector-spark-3.3:26.0.0`，兼容 Spark 3.3.2 + Scala 2.12。

## 三、本地开发网络配置

macOS Docker Desktop 的 bridge 网络无法从宿主机直连容器内部 IP。通过已安装的 `mac-docker-connector` 建立 macOS ↔ Docker VM 的 TUN 隧道解决。

**关键步骤**：

1. Doris 集群使用 `172.20.80.0/24` 子网（`docker-compose.yml` 中静态 IP）
2. macOS 侧 `docker-connector.conf` 已配置 `route 172.20.80.0/24`
3. Spark 进程在 macOS 上可直接访问 Docker 容器 IP（`172.20.80.2`、`172.20.80.3`）

## 四、架构

```
spark-common/src/main/java/com/sziov/gacnev/
├── datasource/
│   ├── DataSourceType.java          # 修改: 新增 DORIS
│   ├── DataSources.java             # 修改: 注册 + doris() 工厂方法
│   ├── constant/
│   │   ├── ParamsKeyConstant.java   # 修改: 新增 7 个 Doris 配置 key
│   │   └── ParamsDefaultValue.java  # 修改: 新增 6 个默认值
│   ├── option/
│   │   └── DorisOption.java         # 新增
│   └── impl/
│       ├── DorisSourceProvider.java # 新增: 工厂注册
│       ├── DorisSource.java         # 新增: Connector 直连 BE 读取
│       └── DorisSink.java           # 新增: Stream Load 写入 + Upsert
```

## 五、实现细节

### DorisSource（读）

```java
spark.read()
    .format("doris")
    .option("doris.fenodes", fenodes)
    .option("doris.query.port", "9030")
    .option("user", username)
    .option("doris.table.identifier", "database.table")
    .load();
```

- 直连 BE 并行扫描，按 Tablet 粒度自动并行
- 支持 `doris.filter.query` 谓词下推（`predicates` 用 `and` 连接）

### DorisSink（写 + Upsert）

```java
df.write()
    .format("doris")
    .option("doris.fenodes", fenodes)
    .option("doris.table.identifier", "database.table")
    .mode("append")  // or overwrite
    .save();
```

- 底层走 Stream Load 批量导入
- `upsert()` 与 `write()` 共用 Stream Load 路径，依赖 Doris Unique Key 模型的 Merge-on-Write
- `execute()` 通过 JDBC（Doris MySQL 协议）执行 DDL/DML

## 六、配置项（app.properties）

```properties
datasource.doris.url=jdbc:mysql://localhost:9030
datasource.doris.username=root
# datasource.doris.password=
datasource.doris.driver=com.mysql.cj.jdbc.Driver
datasource.doris.batch.size=10000
datasource.doris.fenodes=172.20.80.2:8030
```

## 七、测试结果

```
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0 ✅
Total time: 1:16 min（JDBC 方案 2:01 min）
```

- `readFullTable` — Connector 直连 BE 整表读取
- `readCustomQuery` — Connector 直连 BE 读取
- `writeAppend` — Stream Load 追加
- `writeOverwrite` — Stream Load 覆盖
- `executeDdl` — JDBC TRUNCATE TABLE
- `upsertUpdateExisting` — Stream Load + UNIQUE KEY 去重
