# Redis 数据源重构实施计划

**Goal:** 重构 RedisSource/RedisSink，抽取 RedisUtils 工具类（连接池 + URI），引入策略模式支持多种数据模型和写入模式。

**Architecture:** RedisUtils 管理连接池和 URI 构建；RedisSource/RedisSink 通过策略模式委托读写。读支持 hash/string/list/set/zset/stream，写支持 pipeline/lua/transaction/direct/async_callback。

**Tech Stack:** Java 8, Spark 3.3.2, Lettuce 6.x, Commons Pool 2, Lombok, Maven

---

### Task 1: 添加 Maven 依赖

**Files:**
- Modify: `pom.xml` (parent, properties + dependencyManagement)
- Modify: `spark-common/pom.xml` (dependencies)

**Step 1: 添加 Lettuce + Commons Pool 版本属性**

在父 `pom.xml` 的 `<properties>` 中增加：

```xml
<lettuce.version>6.3.2.RELEASE</lettuce.version>
<commons-pool2.version>2.12.0</commons-pool2.version>
```

**Step 2: 在父 POM dependencyManagement 中声明**

```xml
<dependency>
    <groupId>io.lettuce</groupId>
    <artifactId>lettuce-core</artifactId>
    <version>${lettuce.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-pool2</artifactId>
    <version>${commons-pool2.version}</version>
</dependency>
```

**Step 3: 在 spark-common/pom.xml 中添加依赖**

在 spark-common/pom.xml 的 `<dependencies>` 中，Redis 段替换为：

```xml
<!-- Redis -->
<dependency>
    <groupId>io.lettuce</groupId>
    <artifactId>lettuce-core</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-pool2</artifactId>
</dependency>
```

并删除旧的 `com.redislabs:spark-redis` 依赖。

**Step 4: 验证依赖解析**

Run: `mvn dependency:resolve -pl spark-common -q`
Expected: BUILD SUCCESS

---

### Task 2: 创建 RedisUtils 工具类

**Files:**
- Create: `spark-common/src/main/java/com/sziov/gacnev/common/RedisUtils.java`
- Test: `spark-common/src/test/java/com/sziov/gacnev/common/RedisUtilsTest.java`

**Step 1: 写失败的测试**

```java
package com.sziov.gacnev.common;

import com.sziov.gacnev.AbstractSparkTest;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class RedisUtilsTest extends AbstractSparkTest {

    @Test
    void shouldBuildRedisUriFromConfig() {
        RedisURI uri = RedisUtils.buildRedisUri();
        assertThat(uri.getHost()).isEqualTo("localhost");
        assertThat(uri.getPort()).isEqualTo(6379);
        assertThat(uri.getDatabase()).isEqualTo(0);
    }
}
```

Run: `mvn test -pl spark-common -Dtest=RedisUtilsTest -DfailIfNoTests=false`
Expected: FAIL (RedisUtils not defined)

**Step 2: 实现 RedisUtils 最小版本**

```java
package com.sziov.gacnev.common;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.spark.SparkParameterTool;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Properties;

@Slf4j
public final class RedisUtils {

    private static volatile GenericObjectPool<StatefulRedisConnection<String, String>> POOL;
    private static final Object LOCK = new Object();

    private RedisUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    public static RedisURI buildRedisUri() {
        Properties dsConfig = DataSources.getDsConfig();
        String host = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_REDIS_HOST, ParamsDefaultValue.DATASOURCE_REDIS_HOST);
        int port = Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_REDIS_PORT, ParamsDefaultValue.DATASOURCE_REDIS_PORT));
        String auth = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_REDIS_AUTH, null);
        int db = Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_REDIS_DB, ParamsDefaultValue.DATASOURCE_REDIS_DB));

        RedisURI.Builder builder = RedisURI.builder().withHost(host).withPort(port);
        if (auth != null && !auth.isEmpty()) {
            builder.withPassword(auth.toCharArray());
        }
        builder.withDatabase(db);
        return builder.build();
    }

    public static StatefulRedisConnection<String, String> borrowConnection() {
        if (POOL == null) {
            synchronized (LOCK) {
                if (POOL == null) {
                    initPool();
                }
            }
        }
        try {
            return POOL.borrowObject();
        } catch (Exception e) {
            throw new WarehouseException("无法从 Redis 连接池获取连接", e);
        }
    }

    public static void returnConnection(StatefulRedisConnection<String, String> conn) {
        if (conn != null && POOL != null) {
            POOL.returnObject(conn);
        }
    }

    public static void closePool() {
        if (POOL != null) {
            synchronized (LOCK) {
                if (POOL != null) {
                    POOL.close();
                    POOL = null;
                }
            }
        }
    }

    private static void initPool() {
        Properties dsConfig = DataSources.getDsConfig();
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(Integer.parseInt(SparkParameterTool.get(dsConfig,
                "redis.pool.maxTotal", "8")));
        config.setMaxIdle(Integer.parseInt(SparkParameterTool.get(dsConfig,
                "redis.pool.maxIdle", "8")));
        config.setMinIdle(Integer.parseInt(SparkParameterTool.get(dsConfig,
                "redis.pool.minIdle", "2")));
        config.setMaxWaitMillis(Long.parseLong(SparkParameterTool.get(dsConfig,
                "redis.pool.maxWaitMs", "2000")));
        config.setTestOnBorrow(Boolean.parseBoolean(SparkParameterTool.get(dsConfig,
                "redis.pool.testOnBorrow", "true")));

        RedisURI redisUri = buildRedisUri();
        RedisClient client = RedisClient.create(redisUri);
        POOL = ConnectionPoolSupport.createGenericObjectPool(
                () -> {
                    StatefulRedisConnection<String, String> conn = client.connect();
                    conn.setAutoFlushCommands(true);
                    return conn;
                }, config);
        log.info("Redis 连接池初始化完成，maxTotal={}", config.getMaxTotal());
    }
}
```

Run: `mvn test -pl spark-common -Dtest=RedisUtilsTest -DfailIfNoTests=false`
Expected: PASS

**Step 3: 提交**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/common/RedisUtils.java \
        spark-common/src/test/java/com/sziov/gacnev/common/RedisUtilsTest.java \
        pom.xml spark-common/pom.xml
git commit -m "feat: 新增 RedisUtils 连接池工具类"
```

---

### Task 3: 创建 RedisReadStrategy 接口 + HashReadStrategy

**Files:**
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/RedisReadStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/HashReadStrategy.java`
- Test: `spark-common/src/test/java/com/sziov/gacnev/datasource/redis/HashReadStrategyTest.java`

**Step 1: 创建接口**

```java
package com.sziov.gacnev.datasource.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public interface RedisReadStrategy extends Serializable {
    Row readRow(StatefulRedisConnection<String, String> conn, String key, StructType schema);
}
```

**Step 2: 创建 HashReadStrategy**

```java
package com.sziov.gacnev.datasource.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HashReadStrategy implements RedisReadStrategy {
    private static final long serialVersionUID = 1L;
    private static final String KEY_COLUMN = "_key";

    @Override
    public Row readRow(StatefulRedisConnection<String, String> conn, String key, StructType schema) {
        Map<String, String> hash = conn.sync().hgetall(key);
        String[] fieldNames = schema.fieldNames();
        if (hash == null || hash.isEmpty()) {
            return null;
        }
        Object[] values = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (KEY_COLUMN.equals(fieldNames[i])) {
                values[i] = key;
            } else {
                values[i] = hash.getOrDefault(fieldNames[i], null);
            }
        }
        return RowFactory.create(values);
    }
}
```

**Step 3: 提交**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/datasource/redis/
git commit -m "feat: 新增 RedisReadStrategy 接口及 HashReadStrategy"
```

---

### Task 4: 创建 RedisWriteStrategy 接口 + PipelineWriteStrategy

**Files:**
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/RedisWriteStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/PipelineWriteStrategy.java`
- Test: `spark-common/src/test/java/com/sziov/gacnev/datasource/redis/PipelineWriteStrategyTest.java`

**Step 1: 创建接口**

```java
package com.sziov.gacnev.datasource.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public interface RedisWriteStrategy extends Serializable {

    void begin(StatefulRedisConnection<String, String> conn);

    void write(StatefulRedisConnection<String, String> conn, Row row,
               String keyColumn, String resource, int ttl);

    void flush(StatefulRedisConnection<String, String> conn);
}
```

**Step 2: 创建 PipelineWriteStrategy**

```java
package com.sziov.gacnev.datasource.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PipelineWriteStrategy implements RedisWriteStrategy {
    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_BATCH_SIZE = 1000;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private int count = 0;

    @Override
    public void begin(StatefulRedisConnection<String, String> conn) {
        conn.setAutoFlushCommands(false);
    }

    @Override
    public void write(StatefulRedisConnection<String, String> conn, Row row,
                       String keyColumn, String resource, int ttl) {
        Object keyObj = row.getAs(keyColumn);
        if (keyObj == null) {
            return;
        }
        String key = keyObj.toString();
        if (key.isEmpty()) {
            return;
        }
        if (resource != null && !resource.isEmpty()) {
            key = resource + ":" + key;
        }

        Map<String, String> hash = new HashMap<>();
        for (String col : row.schema().fieldNames()) {
            if (col.equals(keyColumn)) {
                continue;
            }
            Object val = row.getAs(col);
            if (val != null) {
                hash.put(col, val.toString());
            }
        }
        conn.async().hset(key, hash);
        if (ttl > 0) {
            conn.async().expire(key, ttl);
        }
        if (++count % batchSize == 0) {
            conn.flushCommands();
        }
    }

    @Override
    public void flush(StatefulRedisConnection<String, String> conn) {
        conn.flushCommands();
    }
}
```

**Step 3: 提交**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/datasource/redis/
git commit -m "feat: 新增 RedisWriteStrategy 接口及 PipelineWriteStrategy"
```

---

### Task 5: 创建剩余读取策略

**Files:**
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/AbstractReadStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/StringReadStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/ListReadStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/SetReadStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/ZSetReadStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/StreamReadStrategy.java`

**实现要点：**

每个策略实现 `RedisReadStrategy`，核心逻辑：

| 策略 | 方法 | 输出 |
|------|------|------|
| StringReadStrategy | `conn.sync().get(key)` | `[key, value]` |
| ListReadStrategy | `conn.sync().lrange(key, 0, -1)` | `[key, json_array(values)]` |
| SetReadStrategy | `conn.sync().smembers(key)` | `[key, json_array(members)]` |
| ZSetReadStrategy | `conn.sync().zrange(key, 0, -1)` | `[key, json_array(scores)]` |
| StreamReadStrategy | `conn.sync().xrange(key, ...)` | `[key, json_array(entries)]` |

**提交：**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/datasource/redis/
git commit -m "feat: 新增 String/List/Set/ZSet/Stream 读取策略"
```

---

### Task 6: 创建剩余写入策略

**Files:**
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/LuaWriteStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/TransactionWriteStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/DirectWriteStrategy.java`
- Create: `spark-common/src/main/java/com/sziov/gacnev/datasource/redis/AsyncCallbackWriteStrategy.java`

**实现要点：**

| 策略 | 核心差异 |
|------|---------|
| LuaWriteStrategy | `conn.sync().scriptLoad(script)` 预加载 → `conn.sync().evalsha(sha, ...)` |
| TransactionWriteStrategy | `conn.async().multi()` → 批量写 → `conn.async().exec()` |
| DirectWriteStrategy | `conn.sync().hset(key, hash)` 逐条同步 |
| AsyncCallbackWriteStrategy | `conn.async().hset()` → 收集 `RedisFuture` → `LettuceFutures.awaitAll()` 等待+回调 |

**提交：**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/datasource/redis/
git commit -m "feat: 新增 Lua/Transaction/Direct/AsyncCallback 写入策略"
```

---

### Task 7: 重构 RedisSource

**Files:**
- Modify: `spark-common/src/main/java/com/sziov/gacnev/datasource/impl/RedisSource.java`

**核心改动：**

1. 删除 `buildRedisUri()` 方法 → 改用 `RedisUtils.buildRedisUri()`
2. 删除 `scanKeys()` → 改用 `RedisUtils.scanAll()` / `RedisUtils.scanBySlot()`
3. `readPartition()` 中：
   - 用 `RedisUtils.borrowConnection()` 替代 `new RedisClient + connect`
   - 用 `readStrategy.readRow(conn, key, schema)` 替代硬编码 `hgetall`
   - `finally { RedisUtils.returnConnection(conn); }`
4. 新增大规模路径（key 数 >= 阈值走 slot 分片）
5. Schema 推断改为扫描前 N 个 key（默认 10），合并所有 field

**提交：**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/datasource/impl/RedisSource.java
git commit -m "refactor: 重构 RedisSource 接入 RedisUtils 和读取策略"
```

---

### Task 8: 重构 RedisSink

**Files:**
- Modify: `spark-common/src/main/java/com/sziov/gacnev/datasource/impl/RedisSink.java`

**核心改动：**

1. 删除 `buildRedisUri()` → `RedisUtils.buildRedisUri()`
2. 删除 `writeRow()` → 委托给 `writeStrategy.write()`
3. `write()` 方法改为根据 `options.getRedisWriteMode()` 选择策略
4. 分区内用 `borrowConnection()` / `returnConnection()` 管理连接
5. 异步策略的错误收集：分区内收集失败行 → 统一抛 `WarehouseException`

**提交：**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/datasource/impl/RedisSink.java
git commit -m "refactor: 重构 RedisSink 接入 RedisUtils 和写入策略"
```

---

### Task 9: 更新 WriteOptions

**Files:**
- Modify: `spark-common/src/main/java/com/sziov/gacnev/datasource/WriteOptions.java`

**改动：** 新增字段

```java
/** Redis 写入模式：pipeline / lua / transaction / direct / async_callback */
private String redisWriteMode;
```

**提交：**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/datasource/WriteOptions.java
git commit -m "feat: WriteOptions 新增 redisWriteMode 字段"
```

---

### Task 10: 更新常量和配置文件

**Files:**
- Modify: `spark-common/src/main/java/com/sziov/gacnev/constant/ParamsKeyConstant.java`
- Modify: `spark-common/src/main/java/com/sziov/gacnev/constant/ParamsDefaultValue.java`
- Modify: `spark-common/src/main/resources/app.properties`

**改动：**

ParamsKeyConstant 新增：
```java
public static final String REDIS_POOL_MAX_TOTAL = "redis.pool.maxTotal";
public static final String REDIS_POOL_MAX_IDLE = "redis.pool.maxIdle";
public static final String REDIS_POOL_MIN_IDLE = "redis.pool.minIdle";
public static final String REDIS_POOL_MAX_WAIT_MS = "redis.pool.maxWaitMs";
public static final String REDIS_POOL_TEST_ON_BORROW = "redis.pool.testOnBorrow";
public static final String REDIS_SCAN_THRESHOLD = "redis.scan.threshold";
```

ParamsDefaultValue 新增对应默认值。

app.properties 新增：
```properties
redis.pool.maxTotal=8
redis.pool.maxIdle=8
redis.pool.minIdle=2
redis.pool.maxWaitMs=2000
redis.pool.testOnBorrow=true
redis.scan.threshold=10000
```

**提交：**

```bash
git add spark-common/src/main/java/com/sziov/gacnev/constant/ \
        spark-common/src/main/resources/app.properties
git commit -m "feat: 新增 Redis 连接池和扫描阈值配置项"
```

---

### Task 11: 更新 SPI 配置

**Files:**
- Create/Modify: `spark-common/src/main/resources/META-INF/services/com.sziov.gacnev.datasource.DataSourceProvider`

确认 `RedisSource` 已在 SPI 文件中注册。

**验证：**

```bash
cat spark-common/src/main/resources/META-INF/services/com.sziov.gacnev.datasource.DataSourceProvider
```

Expected: 包含 `com.sziov.gacnev.datasource.impl.RedisSource`

---

### Task 12: 编写集成测试

**Files:**
- Create: `spark-common/src/test/java/com/sziov/gacnev/datasource/impl/RedisSourceTest.java`
- Create: `spark-common/src/test/java/com/sziov/gacnev/datasource/impl/RedisSinkTest.java`

**测试覆盖：**

- RedisSource 小规模路径读取
- RedisSource 大规模路径读取（slot 分片）
- RedisSink Pipeline 写入
- RedisSink 异步回调写入
- 连接池借还正常
- 空 key 集合处理
- 错误重试机制

**运行：**

Run: `mvn test -pl spark-common`
Expected: ALL TESTS PASS

---

### Task 13: 全量测试 + 格式化检查

**Files:**
- All modified files

Run: `mvn test`
Expected: BUILD SUCCESS, all tests pass

Run: `mvn jacoco:report` (if configured)
Expected: 覆盖率报告生成
