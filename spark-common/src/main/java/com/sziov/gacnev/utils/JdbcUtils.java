package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * JDBC工具类
 * 提供数据库连接、查询、更新等功能
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class JdbcUtils {

    private JdbcUtils() {}

    /**
     * 获取数据库连接
     *
     * @param url      数据库URL
     * @param username 用户名
     * @param password 密码
     * @return 数据库连接
     */
    public static Connection getConnection(String url, String username, String password) {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            log.error("Failed to get database connection, url: {}", url, e);
            throw new RuntimeException("Failed to get database connection", e);
        }
    }

    /**
     * 获取数据库连接（指定驱动类）
     *
     * @param driverClass 驱动类
     * @param url         数据库URL
     * @param username    用户名
     * @param password    密码
     * @return 数据库连接
     */
    public static Connection getConnection(String driverClass, String url, String username, String password) {
        try {
            Class.forName(driverClass);
            return DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            log.error("Failed to load driver class: {}", driverClass, e);
            throw new RuntimeException("Failed to load driver class: " + driverClass, e);
        } catch (SQLException e) {
            log.error("Failed to get database connection, url: {}", url, e);
            throw new RuntimeException("Failed to get database connection", e);
        }
    }

    /**
     * 关闭数据库连接
     *
     * @param connection 数据库连接
     */
    public static void closeConnection(Connection connection) {
        if (Objects.nonNull(connection)) {
            try {
                connection.close();
                log.debug("Database connection closed successfully");
            } catch (SQLException e) {
                log.error("Failed to close database connection", e);
            }
        }
    }

    /**
     * 关闭Statement
     *
     * @param statement Statement对象
     */
    public static void closeStatement(Statement statement) {
        if (Objects.nonNull(statement)) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.error("Failed to close statement", e);
            }
        }
    }

    /**
     * 关闭ResultSet
     *
     * @param resultSet ResultSet对象
     */
    public static void closeResultSet(ResultSet resultSet) {
        if (Objects.nonNull(resultSet)) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.error("Failed to close result set", e);
            }
        }
    }

    /**
     * 关闭所有资源
     *
     * @param connection 数据库连接
     * @param statement  Statement对象
     * @param resultSet  ResultSet对象
     */
    public static void closeAll(Connection connection, Statement statement, ResultSet resultSet) {
        closeResultSet(resultSet);
        closeStatement(statement);
        closeConnection(connection);
    }

    /**
     * 执行查询SQL
     *
     * @param connection 数据库连接
     * @param sql        SQL语句
     * @param handler    结果集处理器
     * @param <T>        返回类型
     * @return 查询结果
     */
    public static <T> List<T> executeQuery(Connection connection, String sql, ResultSetHandler<T> handler) {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            
            List<T> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(handler.handle(resultSet));
            }
            log.info("Query executed successfully, result count: {}", results.size());
            return results;
        } catch (SQLException e) {
            log.error("Failed to execute query: {}", sql, e);
            throw new RuntimeException("Failed to execute query", e);
        } finally {
            closeAll(null, statement, resultSet);
        }
    }

    /**
     * 执行更新SQL
     *
     * @param connection 数据库连接
     * @param sql        SQL语句
     * @return 影响行数
     */
    public static int executeUpdate(Connection connection, String sql) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            int rows = statement.executeUpdate(sql);
            log.info("Update executed successfully, affected rows: {}", rows);
            return rows;
        } catch (SQLException e) {
            log.error("Failed to execute update: {}", sql, e);
            throw new RuntimeException("Failed to execute update", e);
        } finally {
            closeStatement(statement);
        }
    }

    /**
     * 执行批量更新SQL
     *
     * @param connection 数据库连接
     * @param sqlList    SQL列表
     * @return 影响行数数组
     */
    public static int[] executeBatch(Connection connection, List<String> sqlList) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String sql : sqlList) {
                statement.addBatch(sql);
            }
            int[] rows = statement.executeBatch();
            log.info("Batch executed successfully, SQL count: {}", sqlList.size());
            return rows;
        } catch (SQLException e) {
            log.error("Failed to execute batch", e);
            throw new RuntimeException("Failed to execute batch", e);
        } finally {
            closeStatement(statement);
        }
    }

    /**
     * 结果集处理器接口
     *
     * @param <T> 返回类型
     */
    @FunctionalInterface
    public interface ResultSetHandler<T> {
        /**
         * 处理结果集
         *
         * @param rs 结果集
         * @return 处理结果
         * @throws SQLException SQL异常
         */
        T handle(ResultSet rs) throws SQLException;
    }
}
