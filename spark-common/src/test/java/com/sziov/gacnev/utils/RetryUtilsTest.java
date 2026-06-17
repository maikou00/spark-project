package com.sziov.gacnev.utils;

import com.sziov.gacnev.utils.RetryUtils.RetryFailedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RetryUtils} 单元测试。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("RetryUtils 重试工具测试")
class RetryUtilsTest {

    @Test
    @DisplayName("retry_一次成功_返回结果")
    void retry_successFirstTime_returnsResult() {
        String result = RetryUtils.retry(() -> "ok");
        assertThat(result).isEqualTo("ok");
    }

    @Test
    @DisplayName("retry_前两次失败第三次成功_返回结果")
    void retry_failTwiceThenSuccess_returnsResult() {
        AtomicInteger attempts = new AtomicInteger(0);
        String result = RetryUtils.retry(3, 10L, () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("attempt " + attempts.get());
            }
            return "success";
        });
        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("retry_全部重试失败_抛出RetryFailedException")
    void retry_allFail_throwsRetryFailedException() {
        AtomicInteger attempts = new AtomicInteger(0);
        Callable<Object> alwaysFail = () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("always fail");
        };

        assertThatThrownBy(() -> RetryUtils.retry(3, 1L, alwaysFail))
                .isInstanceOf(RetryFailedException.class)
                .hasMessageContaining("重试 3 次后仍然失败");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("retry_callable为null_抛IllegalArgumentException")
    void retry_nullCallable_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> RetryUtils.retry(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("retry_maxRetries小于1_抛IllegalArgumentException")
    void retry_maxRetriesLessThan1_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> RetryUtils.retry(0, 1000L, () -> "ok"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("retry_default参数_使用默认3次1s初始等待")
    void retry_defaultParams_usesDefaults() {
        String result = RetryUtils.retry(() -> "default");
        assertThat(result).isEqualTo("default");
    }
}
