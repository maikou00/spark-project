package com.sziov.gacnev.utils.pipeline;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

import com.sziov.gacnev.utils.spark.SparkParameterTool;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.SparkSession;

/**
 * Pipeline 执行工具：日期解析 + T+1 校验 + 单天/补数路由。
 *
 * @author maikou
 * @since 2026-06-18
 */
@Slf4j
public final class PipelineUtils {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String DATE_REGEX = "\\d{4}-\\d{2}-\\d{2}";

    private PipelineUtils() {}

    /**
     * 根据参数自动选择单天或补数模式。
     * --start 存在走补数循环，否则走单天（--date 默认昨天）。
     * 所有日期均受 T+1 约束：不得超过昨天。
     *
     * @param spark  SparkSession
     * @param params 命令行参数
     * @param task   单天任务
     */
    public static void execute(SparkSession spark, Properties params, BiConsumer<SparkSession, String> task) {
        String maxDate = yesterday();
        String start = params.getProperty("start");
        if (start != null) {
            String end = requireDate(params, "end", maxDate);
            runBatch(spark, dateRange(start, end), task);
        } else {
            String dt = requireDate(params, "date", maxDate);
            try {
                task.accept(spark, dt);
            } catch (Exception e) {
                throw new RuntimeException("dt=" + dt + " 执行失败", e);
            }
        }
    }

    /**
     * 获取日期参数：优先取 params 中的值，无值时默认返回 maxDate。
     * 校验 yyyy-MM-dd 格式，且不得超过 maxDate（T+1 约束）。
     *
     * @param params  命令行参数
     * @param key     参数名
     * @param maxDate 允许的最大日期（含）
     * @return 校验通过的日期字符串
     * @throws IllegalArgumentException 格式错误或超过 maxDate
     */
    public static String requireDate(Properties params, String key, String maxDate) {
        String dt = SparkParameterTool.get(params, key, maxDate);
        if (!dt.matches(DATE_REGEX)) {
            throw new IllegalArgumentException(key + " 格式错误，期望 yyyy-MM-dd，实际: " + dt);
        }
        LocalDate parsed = LocalDate.parse(dt, DATE_FMT);
        LocalDate max = LocalDate.parse(maxDate, DATE_FMT);
        if (parsed.isAfter(max)) {
            throw new IllegalArgumentException(
                    key + "=" + dt + " 超过允许范围，T+1 模式下最大日期为 " + maxDate);
        }
        return dt;
    }

    private static String yesterday() {
        return LocalDate.now().minusDays(1).format(DATE_FMT);
    }

    private static List<String> dateRange(String start, String end) {
        LocalDate s = LocalDate.parse(start, DATE_FMT);
        LocalDate e = LocalDate.parse(end, DATE_FMT);
        if (s.isAfter(e)) {
            throw new IllegalArgumentException("start 不能晚于 end: " + start + " > " + end);
        }
        List<String> dates = new ArrayList<>();
        for (LocalDate d = s; !d.isAfter(e); d = d.plusDays(1)) {
            dates.add(d.format(DATE_FMT));
        }
        return dates;
    }

    private static void runBatch(SparkSession spark, List<String> dates, BiConsumer<SparkSession, String> task) {
        int success = 0;
        List<String> failed = new ArrayList<>();
        long totalStart = System.currentTimeMillis();

        log.info("批量任务启动, 共{}天, 范围: {} ~ {}", dates.size(), dates.get(0), dates.get(dates.size() - 1));

        for (String dt : dates) {
            try {
                task.accept(spark, dt);
                success++;
            } catch (Exception e) {
                log.error("dt={} 执行失败，跳过继续", dt, e);
                failed.add(dt);
            }
        }

        log.info("批量任务完成: 成功={}, 失败={}, 总耗时={}ms",
                success, failed.size(),
                System.currentTimeMillis() - totalStart);
        if (!failed.isEmpty()) {
            log.warn("失败日期: {}", failed);
        }
    }
}
