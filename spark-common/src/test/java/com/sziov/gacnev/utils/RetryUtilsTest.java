package com.sziov.gacnev.utils;

import com.sziov.gacnev.utils.RetryUtils.RetryFailedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryUtils 重试")
class RetryUtilsTest {

    @Test
    void retryFirstAttemptSuccess() {
        assertThat(RetryUtils.retry(() -> "success")).isEqualTo("success");
    }

    @Test
    void retryAfterFailures() {
        AtomicInteger counter = new AtomicInteger(0);
        String result = RetryUtils.retry(5, 10, () -> {
            if (counter.incrementAndGet() < 3) {
                throw new RuntimeException("fail: " + counter.get());
            }
            return "success";
        });
        assertThat(result).isEqualTo("success");
        assertThat(counter.get()).isEqualTo(3);
    }

    @Test
    void retryAllFailures() {
        assertThatThrownBy(() -> RetryUtils.retry(2, 10, () -> {
            throw new RuntimeException("always fail");
        })).isInstanceOf(RetryFailedException.class);
    }

    @Test
    void retryWithInvalidArgs() {
        assertThatThrownBy(() -> RetryUtils.retry(0, 100, () -> "test"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retryWithNullCallable() {
        assertThatThrownBy(() -> RetryUtils.retry(3, 100, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retryExceptionContainsCause() {
        assertThatThrownBy(() -> RetryUtils.retry(2, 10, () -> {
            throw new IllegalArgumentException("原始错误");
        })).isInstanceOf(RetryFailedException.class)
          .hasCauseInstanceOf(IllegalArgumentException.class);
    }
}
