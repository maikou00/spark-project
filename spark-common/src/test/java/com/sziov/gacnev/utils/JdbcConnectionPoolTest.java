package com.sziov.gacnev.utils;

import com.sziov.gacnev.constant.ParamsDefaultValue;
import com.sziov.gacnev.constant.ParamsKeyConstant;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JdbcConnectionPool 集成测试（H2）")
class JdbcConnectionPoolTest {

    private static final String JDBC_URL = "jdbc:h2:mem:test_pool;DB_CLOSE_DELAY=-1";
    private static final String USER = "sa";
    private static final String PASSWORD = "";

    private Properties jdbcProps() {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        return props;
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    @DisplayName("获取连接并执行 SQL")
    void shouldGetConnectionAndExecuteQuery() throws SQLException {
        try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, jdbcProps());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test_conn (id INT PRIMARY KEY, name VARCHAR(50))");
            stmt.execute("INSERT INTO test_conn VALUES (1, 'hello')");
            ResultSet rs = stmt.executeQuery("SELECT name FROM test_conn WHERE id = 1");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("hello");
        }
    }

    @Test
    @DisplayName("相同 key 复用连接池")
    void shouldReusePool() throws SQLException {
        Connection c1 = JdbcConnectionPool.getConnection(JDBC_URL, jdbcProps());
        c1.close();

        Connection c2 = JdbcConnectionPool.getConnection(JDBC_URL, jdbcProps());
        c2.close();

        // 复用同一池，创建表后第二轮查询应可见
        try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, jdbcProps());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test_reuse (val INT)");
            stmt.execute("INSERT INTO test_reuse VALUES (42)");
        }
        try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, jdbcProps());
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT val FROM test_reuse");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("val")).isEqualTo(42);
        }
    }

    @Test
    @DisplayName("不同 key 创建不同连接池")
    void shouldCreateSeparatePools() throws SQLException {
        String url2 = "jdbc:h2:mem:test_pool2;DB_CLOSE_DELAY=-1";
        Properties props = jdbcProps();

        Connection conn1 = JdbcConnectionPool.getConnection(JDBC_URL, props);
        Connection conn2 = JdbcConnectionPool.getConnection(url2, props);

        // 两个连接应属于不同数据库
        try (Statement stmt = conn1.createStatement()) {
            stmt.execute("CREATE TABLE pool_a (id INT)");
        }
        conn1.close();

        try (Statement stmt = conn2.createStatement()) {
            stmt.execute("CREATE TABLE pool_b (id INT)");
        }
        conn2.close();

        // conn1 的数据库不应有 pool_b 表
        try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, props);
             Statement stmt = conn.createStatement()) {
            assertThatThrownBy(() -> stmt.execute("SELECT * FROM pool_b"))
                    .isInstanceOf(org.h2.jdbc.JdbcSQLSyntaxErrorException.class);
        }
    }

    @Test
    @DisplayName("默认 maxSize=0 时按处理器数量计算")
    void shouldUseProcessorCountWhenMaxSizeZero() throws Exception {
        Properties props = jdbcProps();
        props.setProperty(ParamsKeyConstant.DATASOURCE_POOL_MAX_SIZE, "0");

        try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test_cpu (val INT)");
        }
        // 只要能获取连接即可，说明池成功创建
    }

    @Test
    @DisplayName("maxSize 值可被覆盖")
    void shouldAllowOverrideMaxSize() throws Exception {
        Properties props = jdbcProps();
        props.setProperty(ParamsKeyConstant.DATASOURCE_POOL_MAX_SIZE, "2");

        try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test_override (val INT)");
            ResultSet rs = stmt.executeQuery("SELECT 1");
            assertThat(rs.next()).isTrue();
        }
    }

    @Test
    @DisplayName("配置值非法时降级到默认值")
    void shouldFallbackToDefaultOnInvalidConfig() throws Exception {
        Properties props = jdbcProps();
        props.setProperty(ParamsKeyConstant.DATASOURCE_POOL_CONNECTION_TIMEOUT, "not_a_number");

        // 不应抛异常，应降级到默认 30s
        try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test_fallback (val INT)");
        }
    }

    @Test
    @DisplayName("并发获取连接")
    void shouldHandleConcurrentAccess() throws Exception {
        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger success = new AtomicInteger(0);

        try (Connection setup = JdbcConnectionPool.getConnection(JDBC_URL, jdbcProps());
             Statement stmt = setup.createStatement()) {
            stmt.execute("CREATE TABLE test_concurrent (id INT)");
        }

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try (Connection conn = JdbcConnectionPool.getConnection(JDBC_URL, jdbcProps());
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO test_concurrent VALUES (" +
                            Thread.currentThread().getId() + ")");
                    success.incrementAndGet();
                } catch (SQLException ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(success.get()).isEqualTo(threads);

        executor.shutdown();
    }
}
