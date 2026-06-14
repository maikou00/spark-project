package com.sziov.gacnev.datasource.redis;

import com.sziov.gacnev.utils.WarehouseException;

/**
 * Redis 写入模式枚举。
 * <p>注意：TRANSACTION 模式已移除，Redis Cluster 不支持 MULTI/EXEC 跨 slot 事务，请使用 LUA 模式实现原子操作。</p>
 *
 * @author maikou
 * @since 2026-06-11
 */
public enum RedisWriteMode {

    PIPELINE, LUA, DIRECT, ASYNC_CALLBACK;

    public static RedisWriteMode from(String value) {
        if (value == null || value.isEmpty()) {
            return PIPELINE;
        }
        String upper = value.toUpperCase().replace("-", "_");
        if ("TRANSACTION".equals(upper)) {
            throw new WarehouseException(
                    "TRANSACTION 模式已移除：Redis Cluster 不支持 MULTI/EXEC 跨 slot 事务，请使用 LUA 模式实现原子操作");
        }
        try {
            return valueOf(upper);
        } catch (IllegalArgumentException e) {
            throw new WarehouseException("不支持的 Redis 写入模式: " + value);
        }
    }
}
