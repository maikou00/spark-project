package com.sziov.gacnev.common;

import com.sziov.gacnev.common.RetryUtils.RetryFailedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RetryUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("RetryUtils 重试工具测试")
class RetryUtilsTest {

    @Test
    @DisplayName("retry_首次成功_直接返回结果")
    void retry_firstAttemptSuccess_returnsImmediately() {
        assertThat(RetryUtils.retry(() -> "success")).isEqualTo("success");
    }

    @Test
    @DisplayName("retry_前两次失败第三次成功_返回正确结果")
    void retry_failTwiceThenSucceed_returnsAfterRetries() {
        AtomicInteger counter = new AtomicInteger(0);
        String result = RetryUtils.retry(5, 10, () -> {
            if (counter.incrementAndGet() < 3) {
                throw new RuntimeException("fail");
            }
            return "success";
        });
        assertThat(result).isEqualTo("success");
        assertThat(counter.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("retry_全部失败_抛出RetryFailedException")
    void retry_allFailures_throwsRetryFailedException() {
        assertThatThrownBy(() -> RetryUtils.retry(2, 10, () -> {
            throw new RuntimeException("always fail");
        })).isInstanceOf(RetryFailedException.class);
    }

    @Test
    @DisplayName("retry_无效重试次数_抛出IllegalArgumentException")
    void retry_invalidMaxRetries_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> RetryUtils.retry(0, 100, () -> "test"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("retry_null回调_抛出IllegalArgumentException")
    void retry_nullCallable_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> RetryUtils.retry(3, 100, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("retry_异常包含原始cause")
    void retry_exceptionContainsCause() {
        assertThatThrownBy(() -> RetryUtils.retry(2, 10, () -> {
            throw new IllegalArgumentException("原始错误");
        })).isInstanceOf(RetryFailedException.class)
          .hasCauseInstanceOf(IllegalArgumentException.class);
    }
}
