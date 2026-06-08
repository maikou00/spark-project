package com.sziov.gacnev.datasource.option;

import com.sziov.gacnev.datasource.redis.RedisWriteMode;
import com.sziov.gacnev.datasource.redis.RedisModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Redis 数据源 Option，包含读和写参数。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class RedisOption implements DataSourceOption<RedisOption> {

    /** 资源标识：表名或 key */
    private String resource;

    /** key 列名 */
    private String keyColumn;

    /** 数据模型：HASH / STRING / SET / ZSET / LIST / STREAM（默认 HASH） */
    private RedisModel redisModel;

    /** ZSet 分值列名（仅 ZSET 模型生效，默认 "score"） */
    private String zsetScoreColumn;

    /** key 通配模式（读，如 user:*），与 resource 互斥 */
    private String keysPattern;

    /** SCAN COUNT 参数（读） */
    private int scanCount;

    /** 分区数（读） */
    private int numPartitions;

    /** Pipeline 批量大小 */
    private int maxPipelineSize;

    /** key 过期时间（写，秒） */
    private int ttl;

    /** 自定义 Lua 脚本（仅 LUA 写入模式生效，不传则使用默认脚本） */
    private String luaScript;

    /** 写入模式（默认 PIPELINE） */
    private RedisWriteMode redisWriteMode;
}
