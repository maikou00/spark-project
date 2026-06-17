package com.sziov.gacnev.utils;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.datasource.DataSources;
import com.sziov.gacnev.utils.spark.SparkParameterTool;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Redis 工具类。统一使用 {@link RedisClusterClient}，同时兼容单机和集群部署。
 *
 * @author maikou
 * @since 2026-06-11
 */
@Slf4j
public final class RedisUtils {

    private static volatile GenericObjectPool<StatefulRedisClusterConnection<String, String>> POOL;
    private static volatile RedisClusterClient CLIENT;
    private static final Object LOCK = new Object();

    private RedisUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 从连接池借用一个 Redis 集群连接，连接池不存在时自动初始化。
     *
     * @return Redis 集群连接
     * @throws WarehouseException 连接池耗尽或初始化失败
     */
    public static StatefulRedisClusterConnection<String, String> borrowConnection() {
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


    /**
     * 归还连接到池中，自动恢复 autoFlush 状态。
     *
     * @param conn 待归还的连接
     */
    public static void returnConnection(StatefulRedisClusterConnection<String, String> conn) {
        if (conn != null && POOL != null) {
            conn.setAutoFlushCommands(true);
            POOL.returnObject(conn);
        }
    }


    /**
     * 关闭连接池并释放客户端资源，幂等操作。
     */
    public static void closePool() {
        if (POOL != null) {
            synchronized (LOCK) {
                if (POOL != null) {
                    POOL.close();
                    POOL = null;
                }
                if (CLIENT != null) {
                    CLIENT.shutdown();
                    CLIENT = null;
                }
            }
        }
        log.info("Redis 连接池已关闭");
    }

    /**
     * SCAN 所有匹配 pattern 的 key，集群模式下自动跨节点扫描。
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
        StatefulRedisClusterConnection<String, String> conn = borrowConnection();
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

    private static void initPool() {
        Properties dsConfig = DataSources.getDsConfig();
        GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>> config = new GenericObjectPoolConfig<>();
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

        String host = SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_REDIS_HOST, ParamsDefaultValue.DATASOURCE_REDIS_HOST);
        int port = Integer.parseInt(SparkParameterTool.get(dsConfig,
                ParamsKeyConstant.DATASOURCE_REDIS_PORT, ParamsDefaultValue.DATASOURCE_REDIS_PORT));
        String auth = SparkParameterTool.get(dsConfig, ParamsKeyConstant.DATASOURCE_REDIS_AUTH, null);

        RedisURI.Builder builder = RedisURI.builder().withHost(host).withPort(port);
        if (auth != null && !auth.isEmpty()) {
            builder.withPassword(auth.toCharArray());
        }
        RedisURI redisUri = builder.build();

        CLIENT = RedisClusterClient.create(redisUri);
        POOL = ConnectionPoolSupport.createGenericObjectPool(
                () -> {
                    StatefulRedisClusterConnection<String, String> conn = CLIENT.connect();
                    conn.setAutoFlushCommands(true);
                    return conn;
                }, config);
        log.info("Redis 连接池初始化完成，maxTotal={}", config.getMaxTotal());
    }
}
