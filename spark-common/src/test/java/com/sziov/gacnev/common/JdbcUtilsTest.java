package com.sziov.gacnev.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JdbcUtils JDBC工具测试")
class JdbcUtilsTest {

    @Test
    @DisplayName("getConnection_无效驱动类_抛出RuntimeException")
    void getConnection_invalidDriverClass_throwsRuntimeException() {
        assertThatThrownBy(() -> JdbcUtils.getConnection("com.invalid.Driver", "jdbc:invalid", "sa", ""))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("closeConnection_null连接_不抛异常")
    void closeConnection_nullConnection_noException() {
        JdbcUtils.closeConnection((Connection) null);
    }

    @Test
    @DisplayName("closeStatement_null_不抛异常")
    void closeStatement_nullStatement_noException() {
        JdbcUtils.closeStatement((Statement) null);
    }

    @Test
    @DisplayName("closeResultSet_null_不抛异常")
    void closeResultSet_nullResultSet_noException() {
        JdbcUtils.closeResultSet((ResultSet) null);
    }

    @Test
    @DisplayName("closeAll_null参数_不抛异常")
    void closeAll_allNull_noException() {
        JdbcUtils.closeAll(null, null, null);
    }
}
