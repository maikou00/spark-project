package com.sziov.gacnev.datasource.redis;

import com.sziov.gacnev.common.JsonUtils;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Redis 写入工具类，按模式分发 begin → write → flush 生命周期。
 * <p>注意：Redis Cluster 不支持 MULTI/EXEC 事务（跨 slot），TRANSACTION 模式已移除，请使用 LUA 模式实现原子操作。</p>
 *
 * @author maikou
 * @since 2026-06-11
 */
@Slf4j
public class RedisWrites {

    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final long ASYNC_AWAIT_TIMEOUT_MS = 30000L;

    private final RedisWriteMode mode;
    private final RedisModel model;
    private final String luaScript;
    private final List<RedisFuture<?>> futures;
    private transient int batchCount;

    public RedisWrites(RedisWriteMode mode, RedisModel model, String luaScript) {
        this.mode = mode;
        this.model = model;
        this.luaScript = luaScript;
        this.futures = new ArrayList<>();
    }

    public void begin(StatefulRedisClusterConnection<String, String> conn) {
        batchCount = 0;
        switch (mode) {
            case PIPELINE:
            case ASYNC_CALLBACK:
                conn.setAutoFlushCommands(false);
                if (mode == RedisWriteMode.ASYNC_CALLBACK) {
                    futures.clear();
                }
                break;
            default:
                break;
        }
    }

    public void write(StatefulRedisClusterConnection<String, String> conn, Row row,
                       String keyColumn, String resource, int ttl, double score) {
        Object fieldObj = row.getAs(keyColumn);
        if (fieldObj == null) {
            return;
        }
        String field = fieldObj.toString();
        if (field.isEmpty()) {
            return;
        }

        String jsonValue = buildJson(row);

        switch (mode) {
            case DIRECT:
                RedisModelCommand.executeSync(model, conn.sync(), resource, field, jsonValue, score);
                if (ttl > 0) {
                    conn.sync().expire(RedisModelCommand.resolveExpireKey(model, resource, field), ttl);
                }
                break;
            case PIPELINE:
                RedisModelCommand.executeAsync(model, conn.async(), resource, field, jsonValue, score);
                if (ttl > 0) {
                    conn.async().expire(RedisModelCommand.resolveExpireKey(model, resource, field), ttl);
                }
                if (++batchCount % DEFAULT_BATCH_SIZE == 0) {
                    conn.flushCommands();
                }
                break;
            case LUA: {
                String script = luaScript != null && !luaScript.isEmpty()
                        ? luaScript
                        : RedisModelCommand.getLuaScript(model);
                String[] keys = RedisModelCommand.getLuaKeys(model, resource, field);
                String[] args = RedisModelCommand.getLuaArgs(model, field, jsonValue, ttl, score);
                conn.sync().eval(script, ScriptOutputType.INTEGER, keys, args);
                break;
            }
            case ASYNC_CALLBACK:
                futures.add(RedisModelCommand.executeAsync(model, conn.async(), resource, field, jsonValue, score));
                if (ttl > 0) {
                    futures.add(conn.async().expire(
                            RedisModelCommand.resolveExpireKey(model, resource, field), ttl));
                }
                break;
            default:
                break;
        }
    }

    public void flush(StatefulRedisClusterConnection<String, String> conn) {
        switch (mode) {
            case PIPELINE:
                conn.flushCommands();
                break;
            case ASYNC_CALLBACK:
                conn.flushCommands();
                for (int i = 0; i < futures.size(); i++) {
                    try {
                        futures.get(i).get(ASYNC_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        log.warn("异步写入第 {} 条失败: {}", i, e.getMessage());
                    }
                }
                break;
            default:
                break;
        }
    }

    private String buildJson(Row row) {
        Map<String, Object> rowMap = new LinkedHashMap<>();
        for (String col : row.schema().fieldNames()) {
            rowMap.put(col, row.getAs(col));
        }
        return JsonUtils.toJson(rowMap);
    }
}
