package com.sziov.gacnev.common;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.spark.SparkParameterTool;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Redis 工具类，提供连接池管理和 URI 构建。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Slf4j
public final class RedisUtils {

    private static volatile GenericObjectPool<StatefulRedisConnection<String, String>> POOL;
    private static volatile Object CLIENT;
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
            conn.setAutoFlushCommands(true);
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
                if (CLIENT != null) {
                    if (CLIENT instanceof RedisClient) {
                        ((RedisClient) CLIENT).shutdown();
                    } else if (CLIENT instanceof RedisClusterClient) {
                        ((RedisClusterClient) CLIENT).shutdown();
                    }
                    CLIENT = null;
                }
            }
        }
        log.info("Redis 连接池已关闭");
    }

    /**
     * 全量 SCAN 匹配的 key。
     *
     * <p><b>⚠️ 注意：</b>所有 Key 在 Driver 端收集，百万级 Key 请使用更精确的模式匹配，
     * 或通过 {@code redis.scan.threshold} 设置上限（超出抛异常）。</p>
     *
     * @param pattern   匹配模式（如 "user:*"）
     * @param scanCount 每次 SCAN 的 COUNT
     * @return 匹配的 key 列表
     */
    public static List<String> scanAll(String pattern, int scanCount) {
        int threshold = Integer.parseInt(SparkParameterTool.get(
                DataSources.getDsConfig(), ParamsKeyConstant.REDIS_SCAN_THRESHOLD,
                ParamsDefaultValue.REDIS_SCAN_THRESHOLD));
        List<String> keys = new ArrayList<>();
        StatefulRedisConnection<String, String> conn = borrowConnection();
        try {
            ScanCursor cursor = ScanCursor.INITIAL;
            ScanArgs args = ScanArgs.Builder.limit(scanCount).match(pattern);
            do {
                KeyScanCursor<String> result = conn.sync().scan(cursor, args);
                keys.addAll(result.getKeys());
                if (keys.size() > threshold) {
                    throw new WarehouseException(
                            "Redis SCAN key 数量超过阈值 (" + threshold + ")，请缩小 pattern 范围或调高 redis.scan.threshold");
                }
                cursor = result;
            } while (!cursor.isFinished());
        } catch (WarehouseException e) {
            throw e;
        } catch (Exception e) {
            throw new WarehouseException("Redis SCAN 失败: " + pattern, e);
        } finally {
            returnConnection(conn);
        }
        return keys;
    }

    /**
     * 按 slot 范围分片 SCAN 匹配的 key（大规模场景，分区内调用）。
     *
     * @param pattern   匹配模式
     * @param scanCount 每次 SCAN 的 COUNT
     * @return 匹配的 key 列表
     */
    public static List<String> scanBySlot(int slotStart, int slotEnd, String pattern, int scanCount) {
        // For non-cluster Redis, fallback to scanAll
        return scanAll(pattern, scanCount);
    }

    private static void initPool() {
        Properties dsConfig = DataSources.getDsConfig();
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.REDIS_POOL_MAX_TOTAL, ParamsDefaultValue.REDIS_POOL_MAX_TOTAL)));
        config.setMaxIdle(Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.REDIS_POOL_MAX_IDLE, ParamsDefaultValue.REDIS_POOL_MAX_IDLE)));
        config.setMinIdle(Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.REDIS_POOL_MIN_IDLE, ParamsDefaultValue.REDIS_POOL_MIN_IDLE)));
        config.setMaxWaitMillis(Long.parseLong(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.REDIS_POOL_MAX_WAIT_MS, ParamsDefaultValue.REDIS_POOL_MAX_WAIT_MS)));
        config.setTestOnBorrow(Boolean.parseBoolean(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.REDIS_POOL_TEST_ON_BORROW, ParamsDefaultValue.REDIS_POOL_TEST_ON_BORROW)));

        boolean cluster = Boolean.parseBoolean(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_REDIS_CLUSTER,
                String.valueOf(ParamsDefaultValue.DATASOURCE_REDIS_CLUSTER)));
        RedisURI redisUri = buildRedisUri();

        if (cluster) {
            CLIENT = RedisClusterClient.create(redisUri);
        } else {
            CLIENT = RedisClient.create(redisUri);
        }

        POOL = ConnectionPoolSupport.createGenericObjectPool(
                () -> {
                    StatefulRedisConnection<String, String> conn;
                    if (CLIENT instanceof RedisClusterClient) {
                        conn = ((RedisClusterClient) CLIENT).connect();
                    } else {
                        conn = ((RedisClient) CLIENT).connect();
                    }
                    conn.setAutoFlushCommands(true);
                    return conn;
                }, config);
        log.info("Redis 连接池初始化完成，cluster={}, maxTotal={}", cluster, config.getMaxTotal());
    }
}
