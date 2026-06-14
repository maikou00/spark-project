package com.sziov.gacnev.datasource.impl;

import com.sziov.gacnev.common.RedisUtils;
import com.sziov.gacnev.common.WarehouseException;
import com.sziov.gacnev.datasource.DataSink;
import com.sziov.gacnev.datasource.option.RedisOption;
import com.sziov.gacnev.datasource.redis.RedisModel;
import com.sziov.gacnev.datasource.redis.RedisWriteMode;
import com.sziov.gacnev.datasource.redis.RedisWrites;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Redis 数据写入
 * <p>一致性语义：PIPELINE/DIRECT/LUA 为 <b>至少一次</b>。</p>
 *
 * @author maikou
 * @since 2026-06-10
 */
@Slf4j
public class RedisSink implements DataSink<RedisOption>, java.io.Serializable {

    private static final String DEFAULT_ZSET_SCORE_COLUMN = "score";

    @Override
    public void write(Dataset<Row> df, RedisOption options) {
        String keyColumn = options.getKeyColumn();
        if (keyColumn == null || keyColumn.isEmpty()) {
            throw new WarehouseException("Redis 写入必须指定 keyColumn");
        }

        String resource = options.getResource();
        int ttl = options.getTtl();
        RedisWriteMode writeMode = options.getRedisWriteMode() != null
                ? options.getRedisWriteMode() : RedisWriteMode.PIPELINE;
        String luaScript = options.getLuaScript();
        RedisModel model = options.getRedisModel() != null
                ? options.getRedisModel() : RedisModel.HASH;
        String zsetScoreColumn = options.getZsetScoreColumn() != null
                ? options.getZsetScoreColumn() : DEFAULT_ZSET_SCORE_COLUMN;

        log.info("RedisSink 写入数据，resource: {}，keyColumn: {}，model: {}，mode: {}",
                resource, keyColumn, model, writeMode);

        df.foreachPartition(rows -> {
            RedisWrites writes = new RedisWrites(writeMode, model, luaScript);
            StatefulRedisClusterConnection<String, String> conn = RedisUtils.borrowConnection();
            try {
                writes.begin(conn);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    double score = extractScore(row, model, zsetScoreColumn);
                    writes.write(conn, row, keyColumn, resource, ttl, score);
                }
                writes.flush(conn);
            } catch (Exception e) {
                log.error("RedisSink 分区写入失败", e);
                throw new WarehouseException("Redis 写入失败", e);
            } finally {
                RedisUtils.returnConnection(conn);
            }
        });
    }

    private double extractScore(Row row, RedisModel model, String zsetScoreColumn) {
        if (model != RedisModel.ZSET) {
            return 0.0;
        }
        try {
            Object scoreObj = row.getAs(zsetScoreColumn);
            if (scoreObj == null) {
                return 0.0;
            }
            return Double.parseDouble(scoreObj.toString());
        } catch (Exception e) {
            log.warn("ZSet score 提取失败，列: {}，使用默认值 0.0", zsetScoreColumn);
            return 0.0;
        }
    }
}
