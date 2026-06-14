package com.sziov.gacnev.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * 日期时间工具类
 * 提供日期格式化、日期计算、分区日期处理等功能
 *
 * @author maikou
 * @since 2026-05-17
 */
@Slf4j
public final class DateUtils {

    /**
     * 日期格式常量
     */
    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String TIMESTAMP_PATTERN = "yyyyMMddHHmmss";
    private static final String YEAR_MONTH_PATTERN = "yyyy-MM";
    private static final String YEAR_PATTERN = "yyyy";

    /**
     * FastDateFormat实例（线程安全）
     */
    private static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance(DATE_PATTERN);
    private static final FastDateFormat DATETIME_FORMAT = FastDateFormat.getInstance(DATETIME_PATTERN);
    private static final FastDateFormat TIMESTAMP_FORMAT = FastDateFormat.getInstance(TIMESTAMP_PATTERN);
    private static final FastDateFormat YEAR_MONTH_FORMAT = FastDateFormat.getInstance(YEAR_MONTH_PATTERN);
    private static final FastDateFormat YEAR_FORMAT = FastDateFormat.getInstance(YEAR_PATTERN);

    private DateUtils() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 日期格式化为字符串
     *
     * @param date 日期对象
     * @return 格式化后的日期字符串（yyyy-MM-dd）
     */
    public static String format(Date date) {
        if (Objects.isNull(date)) {
            return null;
        }
        return DATE_FORMAT.format(date);
    }

    /**
     * 日期格式化为字符串
     *
     * @param date    日期对象
     * @param pattern 格式模式
     * @return 格式化后的日期字符串
     */
    public static String format(Date date, String pattern) {
        if (Objects.isNull(date) || StringUtils.isBlank(pattern)) {
            return null;
        }
        try {
            return FastDateFormat.getInstance(pattern).format(date);
        } catch (RuntimeException e) {
            log.error("Failed to format date: {}, pattern: {}", date, pattern, e);
            return null;
        }
    }

    /**
     * 字符串解析为日期
     *
     * @param dateStr 日期字符串
     * @return 日期对象
     */
    public static Date parse(String dateStr) {
        if (StringUtils.isBlank(dateStr)) {
            return null;
        }
        try {
            return DATE_FORMAT.parse(dateStr);
        } catch (ParseException e) {
            log.error("Failed to parse date: {}", dateStr, e);
            return null;
        }
    }

    /**
     * 字符串解析为日期
     *
     * @param dateStr 日期字符串
     * @param pattern 格式模式
     * @return 日期对象
     */
    public static Date parse(String dateStr, String pattern) {
        if (StringUtils.isBlank(dateStr) || StringUtils.isBlank(pattern)) {
            return null;
        }
        try {
            return FastDateFormat.getInstance(pattern).parse(dateStr);
        } catch (ParseException e) {
            log.error("Failed to parse date: {}, pattern: {}", dateStr, pattern, e);
            return null;
        }
    }

    /**
     * 日期加减天数
     *
     * @param date 日期对象
     * @param days 天数（正数为加，负数为减）
     * @return 计算后的日期对象
     */
    public static Date addDays(Date date, int days) {
        if (Objects.isNull(date)) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, days);
        return calendar.getTime();
    }

    /**
     * 日期加减月份
     *
     * @param date   日期对象
     * @param months 月数（正数为加，负数为减）
     * @return 计算后的日期对象
     */
    public static Date addMonths(Date date, int months) {
        if (Objects.isNull(date)) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, months);
        return calendar.getTime();
    }

    /**
     * 获取当前日期字符串
     *
     * @return 当前日期字符串（yyyy-MM-dd）
     */
    public static String getCurrentDate() {
        return format(new Date());
    }

    /**
     * 获取当前日期时间字符串
     *
     * @return 当前日期时间字符串（yyyy-MM-dd HH:mm:ss）
     */
    public static String getCurrentDateTime() {
        return DATETIME_FORMAT.format(new Date());
    }

    /**
     * 获取当前时间戳字符串
     *
     * @return 当前时间戳字符串（yyyyMMddHHmmss）
     */
    public static String getCurrentTimestamp() {
        return TIMESTAMP_FORMAT.format(new Date());
    }

    /**
     * 获取当前年月字符串
     *
     * @return 当前年月字符串（yyyy-MM）
     */
    public static String getCurrentYearMonth() {
        return YEAR_MONTH_FORMAT.format(new Date());
    }

    /**
     * 获取当前年字符串
     *
     * @return 当前年字符串（yyyy）
     */
    public static String getCurrentYear() {
        return YEAR_FORMAT.format(new Date());
    }

    /**
     * 获取两个日期之间的天数差
     *
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @return 天数差
     */
    public static long getDaysBetween(Date startDate, Date endDate) {
        if (Objects.isNull(startDate) || Objects.isNull(endDate)) {
            return 0L;
        }
        long diff = endDate.getTime() - startDate.getTime();
        return diff / (24 * 60 * 60 * 1000);
    }

    /**
     * 判断日期是否在指定范围内
     *
     * @param date      待判断日期
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @return 是否在范围内
     */
    public static boolean isBetween(Date date, Date startDate, Date endDate) {
        if (Objects.isNull(date) || Objects.isNull(startDate) || Objects.isNull(endDate)) {
            return false;
        }
        return date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0;
    }

    /**
     * 判断日期字符串是否合法（yyyy-MM-dd格式）
     *
     * @param dateStr 日期字符串
     * @return 是否合法
     */
    public static boolean isValidDate(String dateStr) {
        if (StringUtils.isBlank(dateStr)) {
            return false;
        }
        try {
            java.time.LocalDate.parse(dateStr);
            return true;
        } catch (java.time.format.DateTimeParseException e) {
            return false;
        }
    }

    /**
     * 判断分区日期是否合法（yyyy-MM-dd格式）
     *
     * @param dt 分区日期字符串
     * @return 是否合法
     */
    public static boolean isValidDt(String dt) {
        return isValidDate(dt);
    }

    /**
     * 日期偏移（按天）
     *
     * @param dateStr 日期字符串（yyyy-MM-dd）
     * @param offset  偏移天数（正数为加，负数为减）
     * @return 偏移后的日期字符串
     */
    public static String offsetDay(String dateStr, int offset) {
        if (StringUtils.isBlank(dateStr)) {
            return null;
        }
        Date date = parse(dateStr);
        if (Objects.isNull(date)) {
            return null;
        }
        Date result = addDays(date, offset);
        return format(result);
    }

    /**
     * 获取日期的年份
     *
     * @param date 日期对象
     * @return 年份
     */
    public static int getYear(Date date) {
        if (Objects.isNull(date)) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR);
    }

    /**
     * 获取日期的月份（1-12）
     *
     * @param date 日期对象
     * @return 月份
     */
    public static int getMonth(Date date) {
        if (Objects.isNull(date)) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MONTH) + 1;
    }

    /**
     * 获取日期的天数（1-31）
     *
     * @param date 日期对象
     * @return 天数
     */
    public static int getDay(Date date) {
        if (Objects.isNull(date)) {
            return 0;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }
}
