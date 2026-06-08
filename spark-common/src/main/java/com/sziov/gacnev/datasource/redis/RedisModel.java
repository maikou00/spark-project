package com.sziov.gacnev.datasource.redis;

import com.sziov.gacnev.common.WarehouseException;

/**
 * Redis 数据模型枚举。
 *
 * @author maikou
 * @since 2026-06-11
 */
public enum RedisModel {

    HASH, STRING, LIST, SET, ZSET, STREAM;

    public boolean isHash() {
        return this == HASH;
    }

    public static RedisModel from(String value) {
        if (value == null || value.isEmpty()) {
            return HASH;
        }
        try {
            return valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new WarehouseException("不支持的 Redis 数据模型: " + value);
        }
    }
}
