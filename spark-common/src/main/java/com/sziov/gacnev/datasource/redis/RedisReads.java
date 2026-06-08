package com.sziov.gacnev.datasource.redis;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Redis 读取工具类，按模型分发命令。
 *
 * @author maikou
 * @since 2026-06-11
 */
public final class RedisReads {

    private static final String KEY_COLUMN = "_key";
    private static final String VALUE_COLUMN = "value";
    private static final int STREAM_DEFAULT_COUNT = 100;

    private RedisReads() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    public static Row readRow(RedisModel model, StatefulRedisConnection<String, String> conn,
                               String key, StructType schema) {
        switch (model) {
            case STRING: return readString(conn, key);
            case LIST:   return readList(conn, key);
            case SET:    return readSet(conn, key);
            case ZSET:   return readZSet(conn, key);
            case STREAM: return readStream(conn, key);
            case HASH:   return readHash(conn, key, schema);
            default:     throw new UnsupportedOperationException("不支持的 Redis 模型: " + model);
        }
    }

    private static Row readString(StatefulRedisConnection<String, String> conn, String key) {
        String value = conn.sync().get(key);
        if (value == null) {
            return null;
        }
        return RowFactory.create(key, value);
    }

    private static Row readList(StatefulRedisConnection<String, String> conn, String key) {
        List<String> values = conn.sync().lrange(key, 0, -1);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return RowFactory.create(key, String.join(",", values));
    }

    private static Row readSet(StatefulRedisConnection<String, String> conn, String key) {
        Set<String> members = conn.sync().smembers(key);
        if (members == null || members.isEmpty()) {
            return null;
        }
        return RowFactory.create(key, String.join(",", members));
    }

    private static Row readZSet(StatefulRedisConnection<String, String> conn, String key) {
        List<String> values = conn.sync().zrange(key, 0, -1);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return RowFactory.create(key, String.join(",", values));
    }

    private static Row readStream(StatefulRedisConnection<String, String> conn, String key) {
        Range<String> range = Range.create("-", "+");
        Limit limit = Limit.from(STREAM_DEFAULT_COUNT);
        List<StreamMessage<String, String>> messages = conn.sync().xrange(key, range, limit);
        if (messages == null || messages.isEmpty()) {
            return null;
        }
        String value = messages.stream()
                .map(m -> m.getId() + ":" + m.getBody())
                .collect(Collectors.joining(","));
        return RowFactory.create(key, value);
    }

    private static Row readHash(StatefulRedisConnection<String, String> conn, String key, StructType schema) {
        Map<String, String> hash = conn.sync().hgetall(key);
        if (hash == null || hash.isEmpty()) {
            return null;
        }
        String[] fieldNames = schema.fieldNames();
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
