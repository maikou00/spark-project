package com.sziov.gacnev.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link WarehouseException} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("WarehouseException 异常类测试")
class WarehouseExceptionTest {

    @Test
    @DisplayName("构造_仅消息_消息正确设置")
    void constructor_messageOnly_messageSet() {
        WarehouseException ex = new WarehouseException("测试异常");
        assertThat(ex.getMessage()).isEqualTo("测试异常");
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("构造_消息加Cause_Cause正确设置")
    void constructor_messageAndCause_causeSet() {
        IllegalArgumentException cause = new IllegalArgumentException("根因");
        WarehouseException ex = new WarehouseException("包装异常", cause);
        assertThat(ex.getMessage()).isEqualTo("包装异常");
        assertThat(ex.getCause()).isEqualTo(cause);
    }
}
