# spark-common 合并重构实现计划（v2）

**Goal:** 废弃 spark-datasource 独立模块，将所有代码合并到 spark-common，补充 Hive/File Adapter，utils/ 按领域分包

**Architecture:** 单模块 + Datasource 接口驱动。现有 utils/ 按 spark/common/io/etl/meta 分包，新增 datasource/ 统一接口层（hive/file/clickhouse/es/mongo/redis/kafka），DataSourceType 枚举覆盖全部 12 种类型

---

### Task 1: 清理 — 移除 spark-datasource 独立模块

**Files:**
- Delete: `spark-datasource/`（整个目录）
- Modify: `pom.xml`（移除 `<module>spark-datasource</module>` 和 dependencyManagement 中的 connector 条目）

**Step 1: 删除 spark-datasource 目录**

```bash
rm -rf spark-datasource/
```

**Step 2: 修改父 pom.xml**

- 从 `<modules>` 中移除 `<module>spark-datasource</module>`
- 保留 dependencyManagement 中的 connector 依赖（后续移到 profile 中）

**Step 3: 添加 datasource Maven Profile**

在父 `pom.xml` 的 `<profiles>` 中添加：

```xml
<profile>
    <id>datasource</id>
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.elasticsearch</groupId>
                <artifactId>elasticsearch-spark-30_${scala.binary.version}</artifactId>
                <version>8.12.0</version>
                <scope>provided</scope>
            </dependency>
            <dependency>
                <groupId>org.mongodb.spark</groupId>
                <artifactId>mongo-spark-connector_${scala.binary.version}</artifactId>
                <version>10.3.0</version>
                <scope>provided</scope>
            </dependency>
            <dependency>
                <groupId>com.redislabs</groupId>
                <artifactId>spark-redis_${scala.binary.version}</artifactId>
                <version>3.1.0</version>
                <scope>provided</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch-spark-30_${scala.binary.version}</artifactId>
        </dependency>
        <dependency>
            <groupId>org.mongodb.spark</groupId>
            <artifactId>mongo-spark-connector_${scala.binary.version}</artifactId>
        </dependency>
        <dependency>
            <groupId>com.redislabs</groupId>
            <artifactId>spark-redis_${scala.binary.version}</artifactId>
        </dependency>
    </dependencies>
</profile>
```

**Step 4: 验证编译**

```bash
mvn clean compile
mvn clean compile -Pdatasource
```

**Step 5: Commit**

```bash
git commit -m "refactor: 移除 spark-datasource 模块，添加 datasource profile"
```

---

### Task 2: 创建 datasource/ 目录结构 + 核心接口

**Files:**
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/core/DataSource.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/core/DataSink.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/core/DataSourceConfig.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/core/ReadOptions.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/core/WriteOptions.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/core/DataSourceType.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/core/DataSourceFactory.java`

**DataSink.writeStream 签名变更：**

`writeStream` 方法不直接调用 `.start()`，改为返回 `void` 并内部处理 `TimeoutException`（工厂中统一 try-catch），避免各实现类重复处理。

**DataSourceType 完整枚举：**
```java
HIVE,
FILE_CSV, FILE_JSON, FILE_PARQUET, FILE_ORC, FILE_TEXT,
CLICKHOUSE,
ELASTICSEARCH, MONGODB, REDIS,
KAFKA
```

**ReadOptions 新增字段：**
```java
private String database;           // Hive 数据库名
private String partitionFilter;    // Hive 分区过滤
private String format;             // 文件格式（csv/json/parquet/orc/text）
```

**编译验证：** `mvn clean compile`

**Commit:** `feat: datasource 核心接口 + ReadOptions 扩展`

---

### Task 3: Hive Adapter（委托 ReadDataUtils / WriteDataUtils）

**Files:**
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/hive/HiveSource.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/hive/HiveSink.java`

**HiveSource** — 委托 `ReadDataUtils.readHiveTable(spark, database, table, partitionFilter)`

**HiveSink** — 委托 `WriteDataUtils` 的方法：
- `writeHiveStaticPartition` — overwrite 模式
- `writeHiveDynamicPartition` — append 模式
- `appendHiveTable` — 纯追加

**Compile + Commit:** `feat: HiveSource/HiveSink Adapter`

---

### Task 4: File Adapter（委托 ReadDataUtils / WriteDataUtils）

**Files（10 个）：**
- CsvSource, CsvSink, JsonSource, JsonSink, ParquetSource, ParquetSink, OrcSource, OrcSink, TextSource, TextSink

每个 Source 委托 `ReadDataUtils` 对应方法，每个 Sink 委托 `WriteDataUtils.writeParquetToHdfs` / `writeOrcToHdfs` 或直接 `df.write().format().save()`。

**Compile + Commit:** `feat: File Source/Sink Adapter（CSV/JSON/Parquet/ORC/Text）`

---

### Task 5: 迁移外部数据源（ClickHouse/ES/MongoDB/Redis/Kafka）

**Files:** 从 spark-datasource 已有代码迁移 15 个文件，放入 `datasource/clickhouse/`, `datasource/elasticsearch/`, `datasource/mongodb/`, `datasource/redis/`, `datasource/kafka/`

- 更新 package 声明：`com.sziov.gacnev.datasource.xxx`
- 更新 import 路径（已有 core 接口已到位）
- 保留已有 TimeoutException 修复

**Compile + Commit:** `feat: 迁移外部数据源 Source/Sink`

---

### Task 6: utils/ → core/ 包重构

**移动 16 个工具类到子包：**

| 原路径 | 新路径 |
|--------|--------|
| `utils/SparkEnvUtils.java` | `core/spark/SparkEnvUtils.java` |
| `utils/SparkParameterTool.java` | `core/spark/SparkParameterTool.java` |
| `utils/SparkSqlUtils.java` | `core/spark/SparkSqlUtils.java` |
| `utils/DateUtils.java` | `core/common/DateUtils.java` |
| `utils/StringUtils.java` | `core/common/StringUtils.java` |
| `utils/JsonUtils.java` | `core/common/JsonUtils.java` |
| `utils/RetryUtils.java` | `core/common/RetryUtils.java` |
| `utils/JdbcUtils.java` | `core/common/JdbcUtils.java` |
| `utils/WarehouseException.java` | `core/common/WarehouseException.java` |
| `utils/ReadDataUtils.java` | `core/io/ReadDataUtils.java` |
| `utils/WriteDataUtils.java` | `core/io/WriteDataUtils.java` |
| `utils/HdfsUtils.java` | `core/io/HdfsUtils.java` |
| `utils/EtlUtils.java` | `core/etl/EtlUtils.java` |
| `utils/DataQEUtils.java` | `core/etl/DataQEUtils.java` |
| `utils/HiveMetaUtils.java` | `core/meta/HiveMetaUtils.java` |
| `utils/PartitionUtils.java` | `core/meta/PartitionUtils.java` |

**同步更新所有 import：**
- `App.java`
- `JsonToCsvExample.java`
- 所有测试类
- datasource/ 下各实现类中的 import

**编译验证：** `mvn clean compile`

**Commit:** `refactor: utils/ → core/ 包重构，按领域分包`

---

### Task 7: 测试同步迁移

**移动测试类：**

| 原路径 | 新路径 |
|--------|--------|
| `utils/SparkParameterToolTest.java` | `core/spark/SparkParameterToolTest.java` |
| `utils/DateUtilsTest.java` | `core/common/DateUtilsTest.java` |
| `utils/StringUtilsTest.java` | `core/common/StringUtilsTest.java` |
| `utils/RetryUtilsTest.java` | `core/common/RetryUtilsTest.java` |
| `utils/ReadDataUtilsTest.java` | `core/io/ReadDataUtilsTest.java` |
| `utils/EtlUtilsTest.java` | `core/etl/EtlUtilsTest.java` |

**新增测试文件（从 spark-datasource 迁移）：**
- `datasource/core/DataSourceFactoryTest.java`
- `datasource/clickhouse/ClickHouseConfigTest.java`
- `datasource/elasticsearch/ElasticsearchConfigTest.java`
- `datasource/mongodb/MongoConfigTest.java`
- `datasource/redis/RedisConfigTest.java`
- `datasource/kafka/KafkaConfigTest.java`

**运行测试：**

```bash
cp -a . /tmp/sp-verify && cd /tmp/sp-verify && mvn clean test -DforkCount=0 -Djacoco.skip=true
```

**Commit:** `test: 测试类同步迁移 + 新增数据源配置测试`

---

### Task 8: DataSourceFactory 收尾 — 注册所有类型

更新 `DataSourceFactory`，确保所有 12 个 DataSourceType 都有对应的 createSource/createSink case。

**编译+测试验证：** `mvn clean test -DforkCount=0 -Djacoco.skip=true`  
**打包验证：** `mvn clean package -Pcluster -DskipTests`

**Commit:** `feat: DataSourceFactory 注册全部 12 个数据源类型`

---

## 验证检查清单

- [ ] `mvn clean compile` 通过
- [ ] `mvn clean compile -Pdatasource` 通过（含 ES/Mongo/Redis 依赖）
- [ ] `mvn clean test -DforkCount=0 -Djacoco.skip=true` 全部通过
- [ ] `mvn clean package -Pcluster -DskipTests` 成功
- [ ] 所有 import 路径正确
- [ ] spark-datasource/ 目录已删除
- [ ] 父 pom.xml 模块列表更新
- [ ] 无 broken import
