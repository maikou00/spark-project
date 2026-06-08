# Redis 数据源重构设计

## 背景与问题

当前 `RedisSource` 和 `RedisSink` 存在以下核心问题：

1. **连接代码重复** — `buildRedisUri()` 在 Source/Sink 中完全重复
2. **Driver 内存风险** — `scanKeys` 把所有 key 收集到 Driver 的 `List<String>`，百万级 key 会 OOM
3. **多次重复建连** — RedisSource 中 scanKeys、inferSchema、readPartition 各自建连
4. **Schema 推断不健壮** — 只用第一个有数据的 key 推断字段，后续 key field 不一致时丢数据
5. **错误处理不一致** — Source 静默吞异常，Sink 抛异常
6. **写入模式单一** — 只支持 Pipeline 批量写，不支持 Lua 脚本、事务、异步回调等

## 架构分层

```
                          ┌─────────────────────────┐
                          │      RedisUtils          │
                          │  buildRedisUri()         │
                          │  borrowConnection()      │
                          │  returnConnection()      │
                          │  closePool()             │
                          │  scanBySlot()            │
                          │  scanAll()               │
                          └──────────┬──────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
     ┌────────▼────────┐   ┌────────▼────────┐             │
     │  RedisSource     │   │  RedisSink       │             │
     │  read()          │   │  write()         │             │
     │  ├─小规模路径     │   │  ├─getStrategy() │             │
     │  └─大规模路径     │   │  └─foreachPart.  │             │
     └────────┬─────────┘   └────────┬─────────┘             │
              │                      │                       │
     ┌────────▼─────────┐   ┌────────▼─────────────────────────┐
     │ RedisReadStrategy │   │      RedisWriteStrategy          │
     │  ├─HashRead       │   │  ├─PipelineWriteStrategy (默认)   │
     │  ├─StringRead     │   │  ├─LuaWriteStrategy              │
     │  ├─ListRead       │   │  ├─TransactionWriteStrategy      │
     │  ├─SetRead        │   │  ├─DirectWriteStrategy           │
     │  ├─ZSetRead       │   │  └─AsyncCallbackWriteStrategy    │
     │  └─StreamRead     │   └─────────────────────────────────┘
     └───────────────────┘
```

### 模块职责

| 模块 | 职责 |
|------|------|
| `RedisUtils` | 连接池管理、RedisURI 构建、SCAN（全量/按 slot 分片）、脚本预加载 |
| `RedisSource` | 读取入口，选择小规模/大规模路径，委托策略读行 |
| `RedisSink` | 写入入口，根据 `redisWriteMode` 选择策略，委托写入 |
| `RedisReadStrategy` | 读取策略接口，每种 Redis 数据模型一个实现 |
| `RedisWriteStrategy` | 写入策略接口（含 begin/write/flush 生命周期），每种写入模式一个实现 |

## 接口契约

### RedisUtils

```java
public final class RedisUtils {
    // 连接池
    static StatefulRedisConnection<String, String> borrowConnection();
    static void returnConnection(StatefulRedisConnection<String, String> conn);
    static void closePool();

    // SCAN
    static List<String> scanAll(String pattern, int scanCount);
    static List<String> scanBySlot(int slotStart, int slotEnd, String pattern, int scanCount);

    // URI
    static RedisURI buildRedisUri();
}
```

### RedisReadStrategy

```java
interface RedisReadStrategy {
    Row readRow(StatefulRedisConnection<String, String> conn, String key, StructType schema);
}
```

### RedisWriteStrategy

```java
interface RedisWriteStrategy {
    void begin(StatefulRedisConnection<String, String> conn);
    void write(StatefulRedisConnection<String, String> conn, Row row, String keyColumn, String resource, int ttl);
    void flush(StatefulRedisConnection<String, String> conn);
}
```

## 数据流向

### 读取 — 小规模路径（key 数 < 阈值，默认 10000）

```
scanAll(pattern)                    → Driver 端收集 List<String>
createDataset(keys).repartition(n)  → 分发到 Executor
mapPartitions:
  borrowConnection()
  for each key: strategy.readRow(conn, key, schema)
  returnConnection(conn)
```

### 读取 — 大规模路径（key 数 >= 阈值）

```
按 slot 分片 (0..16383)，每个分片一个分区
foreachPartition(slotRange):
  borrowConnection()
  scanBySlot(slotStart, slotEnd, pattern)
  for each key: strategy.readRow(conn, key, schema)
  returnConnection(conn)
  // key 不回流 Driver，分区内直接读+返回 Row
```

### 写入 — 所有策略通用

```
foreachPartition:
  borrowConnection()
  strategy.begin(conn)
  for each row: strategy.write(conn, row, keyColumn, resource, ttl)
  strategy.flush(conn)
  returnConnection(conn)
```

## 策略详细设计

### 读取策略

| 策略 | redisModel | 实现方式 |
|------|-----------|---------|
| `HashReadStrategy` | hash | `hgetall` |
| `StringReadStrategy` | string | `get` |
| `ListReadStrategy` | list | `lrange 0 -1` |
| `SetReadStrategy` | set | `smembers` |
| `ZSetReadStrategy` | zset | `zrange 0 -1 WITHSCORES` |
| `StreamReadStrategy` | stream | `xrange - + COUNT N` |

### 写入策略

| 策略 | redisWriteMode | 实现方式 |
|------|---------------|---------|
| `PipelineWriteStrategy` | pipeline (默认) | async hset + 定期 flushCommands |
| `LuaWriteStrategy` | lua | EVALSHA（脚本预加载）原子执行 |
| `TransactionWriteStrategy` | transaction | MULTI/EXEC 事务批量 |
| `DirectWriteStrategy` | direct | 逐条 sync 写入 |
| `AsyncCallbackWriteStrategy` | async_callback | async + RedisFuture.thenAccept 回调 |

## 技术选型

- **连接池**: Lettuce `ConnectionPoolSupport.createGenericObjectPool()`，基于 Apache Commons Pool 2。
  选择原因：Lettuce 官方推荐，天然支持异步和响应式，单连接多路复用。
- **策略模式**: 读写分别抽象，新增模型/写入模式只需加实现类，不改调用方。
- **双模式读取**: 阈值判断走小规模或大规模路径，兼顾简单性和规模伸缩。
- **错误统一**: 分区内异常统一抛 `WarehouseException`，通过 `RetryUtils` 在 `read()`/`write()` 外层重试。

## 配置项

```properties
# 连接池
redis.pool.maxTotal=8
redis.pool.maxIdle=8
redis.pool.minIdle=2
redis.pool.maxWaitMs=2000
redis.pool.testOnBorrow=true

# 读取阈值（超过此值走大规模 slot 分片路径）
redis.scan.threshold=10000
```

## 文件变更清单

| 操作 | 文件 | 说明 |
|------|------|------|
| 新增 | `common/RedisUtils.java` | 连接池、URI 构建、SCAN 工具 |
| 新增 | `datasource/redis/RedisReadStrategy.java` | 读取策略接口 |
| 新增 | `datasource/redis/HashReadStrategy.java` | hash 读取实现 |
| 新增 | `datasource/redis/StringReadStrategy.java` | string 读取实现 |
| 新增 | `datasource/redis/ListReadStrategy.java` | list 读取实现 |
| 新增 | `datasource/redis/SetReadStrategy.java` | set 读取实现 |
| 新增 | `datasource/redis/ZSetReadStrategy.java` | zset 读取实现 |
| 新增 | `datasource/redis/StreamReadStrategy.java` | stream 读取实现 |
| 新增 | `datasource/redis/RedisWriteStrategy.java` | 写入策略接口 |
| 新增 | `datasource/redis/PipelineWriteStrategy.java` | Pipeline 写实现 |
| 新增 | `datasource/redis/LuaWriteStrategy.java` | Lua 脚本写实现 |
| 新增 | `datasource/redis/TransactionWriteStrategy.java` | 事务写实现 |
| 新增 | `datasource/redis/DirectWriteStrategy.java` | 直接写实现 |
| 新增 | `datasource/redis/AsyncCallbackWriteStrategy.java` | 异步回调写实现 |
| 修改 | `datasource/impl/RedisSource.java` | 接入 RedisUtils + 策略 + 双路径 |
| 修改 | `datasource/impl/RedisSink.java` | 接入 RedisUtils + 策略 |
| 修改 | `datasource/WriteOptions.java` | 新增 `redisWriteMode` 字段 |
| 修改 | `constant/ParamsKeyConstant.java` | 新增连接池常量 |
| 修改 | `constant/ParamsDefaultValue.java` | 新增连接池默认值 |
| 修改 | `src/main/resources/app.properties` | 新增连接池配置 |
