package com.sziov.gacnev.datasource.redis;

import com.sziov.gacnev.common.WarehouseException;

/**
 * Redis 写入模式枚举。
 *
 * @author maikou
 * @since 2026-06-11
 */
public enum RedisWriteMode {

    PIPELINE, LUA, TRANSACTION, DIRECT, ASYNC_CALLBACK;

    public static RedisWriteMode from(String value) {
        if (value == null || value.isEmpty()) {
            return PIPELINE;
        }
        try {
            return valueOf(value.toUpperCase().replace("-", "_"));
        } catch (IllegalArgumentException e) {
            throw new WarehouseException("不支持的 Redis 写入模式: " + value);
        }
    }
}
