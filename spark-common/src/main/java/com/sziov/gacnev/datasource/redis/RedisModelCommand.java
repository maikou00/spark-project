package com.sziov.gacnev.datasource.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Redis 模型命令分发器，将模型→Redis 命令的映射集中在一处，消除各写策略中的重复 switch-case。
 *
 * @author maikou
 * @since 2026-06-11
 */
public final class RedisModelCommand {

    private RedisModelCommand() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 同步执行写入命令，返回 expire 目标 key。
     */
    public static String executeSync(RedisModel model, RedisCommands<String, String> sync,
                                      String resource, String field, String json, double score) {
        switch (model) {
            case STRING:
                sync.set(field, json);
                return field;
            case SET:
                sync.sadd(resource, json);
                return resource;
            case ZSET:
                sync.zadd(resource, score, json);
                return resource;
            case HASH:
            default:
                sync.hset(resource, field, json);
                return resource;
        }
    }

    /**
     * 异步执行写入命令，返回 {@link RedisFuture} 供调用方追踪。
     */
    public static RedisFuture<?> executeAsync(RedisModel model, RedisAsyncCommands<String, String> async,
                                               String resource, String field, String json, double score) {
        switch (model) {
            case STRING:
                return async.set(field, json);
            case SET:
                return async.sadd(resource, json);
            case ZSET:
                return async.zadd(resource, score, json);
            case HASH:
            default:
                return async.hset(resource, field, json);
        }
    }

    /**
     * 解析 expire 目标 key：STRING 模型用 field，其他模型用 resource。
     */
    public static String resolveExpireKey(RedisModel model, String resource, String field) {
        return model == RedisModel.STRING ? field : resource;
    }

    /**
     * 获取模型对应的 Lua 脚本。
     */
    public static String getLuaScript(RedisModel model) {
        switch (model) {
            case STRING:
                return LuaScripts.SET;
            case SET:
                return LuaScripts.SADD;
            case ZSET:
                return LuaScripts.ZADD;
            case HASH:
            default:
                return LuaScripts.HSET;
        }
    }

    /**
     * 获取 Lua EVAL 的 KEYS 数组。
     */
    public static String[] getLuaKeys(RedisModel model, String resource, String field) {
        switch (model) {
            case STRING:
                return new String[]{field};
            case SET:
            case ZSET:
            case HASH:
            default:
                return new String[]{resource};
        }
    }

    /**
     * 获取 Lua EVAL 的 ARGV 数组。
     */
    public static String[] getLuaArgs(RedisModel model, String field, String json, int ttl, double score) {
        switch (model) {
            case STRING:
                return new String[]{json, String.valueOf(ttl)};
            case SET:
                return new String[]{json, String.valueOf(ttl)};
            case ZSET:
                return new String[]{String.valueOf(score), json, String.valueOf(ttl)};
            case HASH:
            default:
                return new String[]{field, json, String.valueOf(ttl)};
        }
    }

    /**
     * Lua 脚本常量。
     */
    public static final class LuaScripts {
        public static final String HSET = "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) "
                + "if tonumber(ARGV[3]) > 0 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end "
                + "return 1";
        public static final String SET = "redis.call('SET', KEYS[1], ARGV[1]) "
                + "if tonumber(ARGV[2]) > 0 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end "
                + "return 1";
        public static final String SADD = "redis.call('SADD', KEYS[1], ARGV[1]) "
                + "if tonumber(ARGV[2]) > 0 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end "
                + "return 1";
        public static final String ZADD = "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2]) "
                + "if tonumber(ARGV[3]) > 0 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end "
                + "return 1";

        private LuaScripts() {
        }
    }
}
