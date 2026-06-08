package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

/**
 * 重试工具 - 指数退避重试。
 *
 * <p>适用场景：HDFS 操作、JDBC 连接等偶发网络抖动场景。</p>
 *
 * @author maikou
 * @since 2026-06-07
 */
@Slf4j
public final class RetryUtils {

    private RetryUtils() {}

    /**
     * 执行带重试的操作（默认 3 次，初始等待 1s）。
     *
     * @param callable 要执行的操作
     * @param <T>      返回类型
     * @return 操作结果
     * @throws RetryFailedException 所有重试均失败
     */
    public static <T> T retry(Callable<T> callable) {
        return retry(3, 1000L, callable);
    }

    /**
     * 执行带重试的操作。
     *
     * @param maxRetries  最大重试次数
     * @param initialWait 初始等待时间（毫秒）
     * @param callable    要执行的操作
     * @param <T>         返回类型
     * @return 操作结果
     * @throws RetryFailedException 所有重试均失败
     */
    public static <T> T retry(int maxRetries, long initialWait, Callable<T> callable) {
        if (callable == null) {
            throw new IllegalArgumentException("callable must not be null");
        }
        if (maxRetries < 1) {
            throw new IllegalArgumentException("maxRetries must be >= 1");
        }

        long waitTime = Math.max(initialWait, 0);
        Throwable lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                T result = callable.call();
                if (attempt > 1) {
                    log.info("操作在第 {} 次重试后成功", attempt);
                }
                return result;
            } catch (Exception e) {
                lastException = e;
                if (attempt < maxRetries) {
                    log.warn("操作失败（第 {}/{} 次），{}ms 后重试: {}", attempt, maxRetries, waitTime, e.getMessage());
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RetryFailedException("重试被中断", ie);
                    }
                    waitTime = Math.min(waitTime * 2, 30000L);
                } else {
                    log.error("操作失败（第 {}/{} 次），不再重试", attempt, maxRetries);
                }
            }
        }

        throw new RetryFailedException("重试 " + maxRetries + " 次后仍然失败", lastException);
    }

    /** 重试失败异常 */
    public static final class RetryFailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public RetryFailedException(String message, Throwable cause) {
            super(message, cause);
        }
        public RetryFailedException(String message) {
            super(message);
        }
    }
}
