# Spark Project

Spark 离线数仓公共工程。提供统一的环境初始化、数据读写、ETL 处理、质量检查基础设施。
基于 Java 8 + Apache Spark 3.3.2，开箱即用。

---

## 快速开始

```bash
# 编译
mvn clean compile

# 运行测试（因路径含中文，需拷贝到 /tmp/ 下执行）
cp -a . /tmp/sp-verify && cd /tmp/sp-verify && \
  mvn clean test -DforkCount=0 -Djacoco.skip=true

# 本地运行（需要 Spark 依赖在 classpath 上）
java -cp "$(mvn dependency:build-classpath -Pdev -Dmdep.outputFile=/dev/stdout -q):spark-common/target/classes" \
  com.sziov.gacnev.App

# 打生产包（剔除 Spark/Hadoop/Hive 依赖，适配集群环境）
mvn clean package -Pcluster -DskipTests
```

---

## 模块结构

```
spark-project/
├── pom.xml                          # 父 POM（依赖管理 + 构建配置）
├── README.md
├── .editorconfig
├── .gitignore
└── spark-common/                    # 公共模块
    ├── pom.xml
    ├── data/input/                   # 测试数据（csv/parquet/orc/json/txt）
    └── src/
        ├── main/java/com/sziov/gacnev/
        │   ├── App.java                           # 入口 + WordCount / DataFrame 示例
        │   ├── constant/
        │   │   ├── ParamsKeyConstant.java          # 配置 Key 常量
        │   │   ├── ParamsDefaultValue.java         # 配置默认值
        │   │   └── WarehouseConstant.java          # 数仓分层 / 分区 / 格式常量
        │   ├── example/
        │   │   └── JsonToCsvExample.java           # JSON → ETL → CSV 完整用例
        │   └── utils/
        │       ├── SparkEnvUtils.java              # 环境初始化 + 统一配置管理
        │       ├── SparkParameterTool.java         # 多源参数加载与合并
        │       ├── SparkSqlUtils.java              # SQL 执行 / 视图 / DDL
        │       ├── ReadDataUtils.java              # Hive / JSON / CSV / Parquet / ORC / Text
        │       ├── WriteDataUtils.java             # Hive 静态/动态分区 / HDFS 写入
        │       ├── EtlUtils.java                   # 清洗 / 脱敏 / 过滤 / 转换
        │       ├── DataQEUtils.java                # 空值 / 重复 / 范围 / 一致性检查
        │       ├── RetryUtils.java                 # 指数退避重试
        │       ├── HdfsUtils.java                  # HDFS 文件 CRUD
        │       ├── HiveMetaUtils.java              # 库 / 表 / 分区 / DDL 查询
        │       ├── PartitionUtils.java             # Hive 分区管理 + DataFrame 重分区
        │       ├── JdbcUtils.java                  # JDBC 查询 / 更新 / 批量
        │       ├── JsonUtils.java                  # Jackson JSON 序列化
        │       ├── DateUtils.java                  # 日期格式化 / 偏移 / 计算
        │       ├── StringUtils.java                # 脱敏（手机 / 身份证 / 邮箱）
        │       └── WarehouseException.java         # 带错误码的异常
        ├── main/resources/
        │   ├── app.properties                      # 默认配置
        │   └── log4j2.xml                          # 日志配置
        └── test/java/.../utils/
            ├── SparkParameterToolTest.java         # 参数加载 18 用例
            ├── ReadDataUtilsTest.java              # 数据读取 8 用例
            ├── EtlUtilsTest.java                   # ETL 处理 7 用例
            ├── DateUtilsTest.java                  # 日期工具 11 用例
            ├── StringUtilsTest.java                # 脱敏转换 6 用例
            └── RetryUtilsTest.java                 # 重试机制 6 用例
```

---

## 配置管理

### 加载优先级

```
命令行参数 --key=value   (最高)
    ↓
外部文件 --config /path/to/app.properties
    ↓
classpath app.properties
    ↓
代码默认值 ParamsDefaultValue.java   (最低)
```

### 关键配置项

```properties
# 运行模式
spark.local=true                          # true=本地, false=YARN集群
spark.app.name=MyApp
spark.hive.enabled=false

# 资源
spark.driver.memory=1g
spark.executor.memory=2g
spark.executor.instances=1
spark.sql.shuffle.partitions=200

# 序列化
spark.serializer=org.apache.spark.serializer.KryoSerializer

# 自适应查询（Spark 3+）
spark.sql.adaptive.enabled=true
```

完整配置见 `app.properties`。

---

## 构建命令

| 命令 | 用途 |
|------|------|
| `mvn clean compile` | 编译 |
| `mvn clean test -DforkCount=0 -Djacoco.skip=true` | 运行所有测试（需在 `/tmp/` 下执行）|
| `mvn clean package -Pcluster -DskipTests` | 打生产胖包（剔除 Spark 依赖） |
| `mvn clean package -Pdev -DskipTests` | 打本地开发包（含 Spark） |
| `mvn clean install -DskipTests` | 安装到本地仓库 |
| `mvn enforcer:enforce` | 检查依赖版本收敛 |

---

## Profile 说明

| Profile | 使用场景 | 依赖范围 | 特点 |
|---------|---------|---------|------|
| `dev`（默认） | 本地开发 / 调试 | compile | 包含 Spark/Hadoop/Hive/MySQL 全部依赖 |
| `cluster` | 生产集群提交 | provided | 仅打业务代码 + commons-lang3，Shade 打包 |

```bash
# 开发
mvn clean compile                                # 默认 dev profile

# 生产部署
mvn clean package -Pcluster -DskipTests          # → spark-common-2.0.0.jar
spark-submit --class com.sziov.gacnev.App \
  spark-common/target/spark-common-2.0.0.jar
```

---

## 核心 API 速览

### 环境初始化

```java
// 一行启动，自动加载配置
SparkSession spark = SparkEnvUtils.prepare(args, "MyJob");
```

### 数据读取

```java
Dataset<Row> df = ReadDataUtils.readJsonWithSchema(spark, path, schema);
Dataset<Row> df = ReadDataUtils.readHiveTable(spark, "ods", "users", "dt='2026-06-07'");
Dataset<Row> df = ReadDataUtils.readParquet(spark, "/user/hive/warehouse/ods.db/users");
```

### 数据写入

```java
WriteDataUtils.writeHiveStaticPartition(df, "dwd", "users", "dt", "2026-06-07");
WriteDataUtils.writeParquetToHdfs(df, "/data/output", 100, SaveMode.Overwrite);
```

### ETL 处理

```java
Dataset<Row> cleaned = EtlUtils.cleanData(df);            // trim + null 转换
Dataset<Row> masked  = EtlUtils.maskEmail(df, "email", "email_masked");
Dataset<Row> grouped = EtlUtils.groupByAge(df, "age", "age_group");
```

### 数据质量

```java
DataQEUtils.printQualityReport(spark, "dwd.users");
double nullRatio = DataQEUtils.getNullRatio(df, "age");
boolean isUnique = DataQEUtils.checkUniqueness(df, "user_id");
```

### 重试

```java
String result = RetryUtils.retry(() -> hdfsClient.readFile(path));
RetryUtils.retry(5, 1000, () -> jdbcClient.execute(sql));
```

---

## 依赖版本

| 依赖 | 版本 |
|------|------|
| Apache Spark | 3.3.2 |
| Apache Hadoop | 3.2.2 |
| Apache Hive | 3.1.3 |
| MySQL Connector | 8.0.28 |
| Scala | 2.12.15 |
| Lombok | 1.18.20 |
| commons-lang3 | 3.12.0 |
| JUnit 5 | 5.8.1 |
| Mockito | 4.11.0 |
| AssertJ | 3.24.2 |

---

## 数仓分层（常量参考）

```java
WarehouseConstant.DB_ODS    // 原始数据层
WarehouseConstant.DB_DWD    // 明细数据层
WarehouseConstant.DB_DWS    // 汇总数据层
WarehouseConstant.DB_ADS    // 应用数据层
WarehouseConstant.DB_DIM    // 维度层
```

默认分区：`dt`（天）、`hour`（小时）、`month`（月）、`week`（周）
默认存储：`Parquet` + `Snappy` 压缩
默认时区：`Asia/Shanghai`

---

## 开发规约

- **注释**：Javadoc + 简体中文
- **日志**：`@Slf4j` 注解，禁止 `System.out`
- **异常**：统一使用 `WarehouseException` + 错误码
- **配置**：外部化 Properties，禁止硬编码
- **测试**：JUnit 5 + AssertJ，工具类必须覆盖
- **构建**：`mvn enforcer:enforce` 保证依赖版本一致

---

## 变更记录

### v2.0.0 企业级重构

- **POM 精简**：移除 Checkstyle/SpotBugs/Flatten/Javadoc/Source 等非必要插件，保留 Enforcer + JaCoCo + Surefire
- **代码清理**：移除过设计的 `AuditLogger`、`ConfigValidator`、`ValidationUtils` 三个类
- **API 简化**：`StringUtils` 保留 5 个自定义脱敏/转换方法，删除 8 个 commons-lang3 委托方法；`RetryUtils` 移除 `RetryRunnable` 接口，统一 `Callable<T>` API
- **Bug 修复**：`HiveMetaUtils.extractStringColumn()` 改用 `take(5000)` 替代 `collect()` 防止 OOM；`PartitionUtils.repartition()` 修复多列分区只使用第一列的 Bug
- **异常优化**：`JsonUtils.toMap/fromMap`、`DateUtils.format` 从 `catch (Exception)` 收窄为 `catch (RuntimeException)`，精确表达异常类型
- **未用代码清理**：移除 `StringUtils`、`DataQEUtils` 中未使用的 `import java.util.Objects`
- **测试增强**：新增 `SparkParameterToolTest`(18)、`EtlUtilsTest`(7)、`DateUtilsTest`(11)、`StringUtilsTest`(6)、`RetryUtilsTest`(6)，累计 56 个用例
