package com.sziov.gacnev.common;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.sziov.gacnev.spark.SparkParameterTool;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC 连接池（Executor 级别单例）。
 *
 * <p>每个 JDBC URL + User 组合持有一个 HikariCP 连接池，懒加载初始化。</p>
 *
 * <p><b>Shutdown 说明：</b>YARN 通过 SIGKILL 终止 Executor 时 JVM shutdown hook 不保证执行，
 * 残留连接由数据库端 {@code wait_timeout} 回收。连接数上限已由 {@code resolveDefaultMaxPoolSize()}
 * 限制（优先 Spark executor cores，上限 8），避免高频扩缩容打爆 {@code max_connections}。</p>
 *
 * <p>池参数优先从 app.properties 读取，未配置时使用 {@link ParamsDefaultValue} 默认值。
 * {@code maxSize=0} 时自动按 {@code Runtime.getRuntime().availableProcessors()} 计算。</p>
 *
 * <p><b>YARN 部署注意：</b>{@code maxSize=0} 时按 {@code Runtime.getRuntime().availableProcessors()}
 * 计算池大小。若 YARN 未启用 cgroups，该方法返回物理机核数而非 Executor 分配核数，
 * 可能导致连接数失控。建议在 app.properties 中显式设置 {@code datasource.pool.maxSize}。</p>
 *
 * @author maikou
 * @since 2026-06-13
 */
@Slf4j
public final class JdbcConnectionPool {

    private static final Map<String, HikariDataSource> POOLS = new ConcurrentHashMap<>();

    private static volatile boolean shutdown = false;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown = true;
            log.info("JdbcConnectionPool shutdown hook 触发，关闭 {} 个连接池", POOLS.size());
            POOLS.forEach((key, pool) -> {
                try {
                    pool.close();
                } catch (Exception ignored) {
                }
            });
            POOLS.clear();
        }));
    }

    private JdbcConnectionPool() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 从连接池获取连接。
     *
     * @param jdbcUrl  JDBC 连接地址
     * @param jdbcProps 连接属性（user、password、driver 等）
     * @return 数据库连接
     * @throws SQLException 获取连接失败
     */
    public static Connection getConnection(String jdbcUrl, Properties jdbcProps) throws SQLException {
        String user = jdbcProps.getProperty("user", "");
        if (shutdown) {
            throw new SQLException("JdbcConnectionPool 已关闭，无法获取连接");
        }
        String poolKey = jdbcUrl + "|" + user;
        HikariDataSource pool = POOLS.computeIfAbsent(poolKey, k -> createPool(jdbcUrl, jdbcProps));
        if (pool.isClosed()) {
            POOLS.remove(poolKey);
            throw new SQLException("连接池已关闭: " + poolKey);
        }
        return pool.getConnection();
    }

    private static HikariDataSource createPool(String jdbcUrl, Properties jdbcProps) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);

        String user = jdbcProps.getProperty("user");
        String password = jdbcProps.getProperty("password");
        if (user != null && !user.isEmpty()) {
            config.setUsername(user);
        }
        if (password != null) {
            config.setPassword(password);
        }

        Properties appConfig = loadAppConfig();
        int maxSize = readInt(appConfig, ParamsKeyConstant.DATASOURCE_POOL_MAX_SIZE,
                ParamsDefaultValue.DATASOURCE_POOL_MAX_SIZE);
                config.setMaximumPoolSize(maxSize > 0 ? maxSize : resolveDefaultMaxPoolSize());
        config.setMinimumIdle(readInt(appConfig, ParamsKeyConstant.DATASOURCE_POOL_MIN_IDLE,
                ParamsDefaultValue.DATASOURCE_POOL_MIN_IDLE));
        config.setConnectionTimeout(readInt(appConfig, ParamsKeyConstant.DATASOURCE_POOL_CONNECTION_TIMEOUT,
                ParamsDefaultValue.DATASOURCE_POOL_CONNECTION_TIMEOUT));
        config.setMaxLifetime(readInt(appConfig, ParamsKeyConstant.DATASOURCE_POOL_MAX_LIFETIME,
                ParamsDefaultValue.DATASOURCE_POOL_MAX_LIFETIME));
        config.setIdleTimeout(readInt(appConfig, ParamsKeyConstant.DATASOURCE_POOL_IDLE_TIMEOUT,
                ParamsDefaultValue.DATASOURCE_POOL_IDLE_TIMEOUT));
        config.setConnectionTestQuery("SELECT 1");

        log.info("创建 JDBC 连接池: {}，maxPoolSize={}，minIdle={}，connectionTimeout={}ms",
                jdbcUrl, config.getMaximumPoolSize(), config.getMinimumIdle(), config.getConnectionTimeout());
        return new HikariDataSource(config);
    }

    /**
     * 解析默认连接池大小，优先从 Spark 配置读取 executor cores，
     * 其次按 CPU 核数（上限 8），防止无 cgroups 环境下打爆数据库连接数。
     */
    private static int resolveDefaultMaxPoolSize() {
        try {
            Class<?> sparkEnvClass = Class.forName("org.apache.spark.SparkEnv");
            Object sparkEnv = sparkEnvClass.getMethod("get").invoke(null);
            if (sparkEnv != null) {
                Object conf = sparkEnvClass.getMethod("conf").invoke(sparkEnv);
                Object cores = conf.getClass().getMethod("getInt", String.class, int.class)
                        .invoke(conf, "spark.executor.cores", -1);
                int executorCores = ((Number) cores).intValue();
                if (executorCores > 0) {
                    return executorCores;
                }
            }
        } catch (Exception ignored) {
        }
        return Math.min(Runtime.getRuntime().availableProcessors(), 8);
    }

    private static Properties loadAppConfig() {
        try {
            Properties props = SparkParameterTool.fromClasspath("app.properties");
            return props != null ? props : new Properties();
        } catch (Exception e) {
            return new Properties();
        }
    }

    private static int readInt(Properties props, String key, int defaultValue) {
        String value = SparkParameterTool.get(props, key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("连接池配置 {} 值非法: {}，使用默认值: {}", key, value, defaultValue);
            return defaultValue;
        }
    }
}
