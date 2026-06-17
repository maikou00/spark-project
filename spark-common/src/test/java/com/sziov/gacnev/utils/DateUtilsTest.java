package com.sziov.gacnev.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DateUtils} 单元测试。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("DateUtils 日期工具测试")
class DateUtilsTest {

    // ==================== format / parse ====================

    @Test
    @DisplayName("format_有效Date_返回yyyy-MM-dd格式字符串")
    void format_validDate_returnsFormattedString() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        assertThat(DateUtils.format(date)).isEqualTo("2026-06-18");
    }

    @Test
    @DisplayName("format_null_返回null")
    void format_null_returnsNull() {
        assertThat(DateUtils.format(null)).isNull();
    }

    @Test
    @DisplayName("parse_合法日期字符串_返回Date")
    void parse_validDateString_returnsDate() {
        Date date = DateUtils.parse("2026-06-18");
        assertThat(date).isNotNull();
        assertThat(DateUtils.format(date)).isEqualTo("2026-06-18");
    }

    @Test
    @DisplayName("parse_空字符串_返回null")
    void parse_emptyString_returnsNull() {
        assertThat(DateUtils.parse("")).isNull();
    }

    @Test
    @DisplayName("parse_非法字符串_返回null")
    void parse_invalidString_returnsNull() {
        assertThat(DateUtils.parse("not_a_date")).isNull();
    }

    @Test
    @DisplayName("format_自定义pattern_正确格式化")
    void format_customPattern_returnsCorrectFormat() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2026-06-18 10:30:00");
        assertThat(DateUtils.format(date, "yyyyMMdd")).isEqualTo("20260618");
    }

    // ==================== addDays / addMonths ====================

    @Test
    @DisplayName("addDays_加1天_日期增加1天")
    void addDays_plusOne_returnsNextDay() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        Date result = DateUtils.addDays(date, 1);
        assertThat(DateUtils.format(result)).isEqualTo("2026-06-19");
    }

    @Test
    @DisplayName("addDays_减1天_日期减少1天")
    void addDays_minusOne_returnsPreviousDay() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        Date result = DateUtils.addDays(date, -1);
        assertThat(DateUtils.format(result)).isEqualTo("2026-06-17");
    }

    @Test
    @DisplayName("addDays_null_返回null")
    void addDays_null_returnsNull() {
        assertThat(DateUtils.addDays(null, 1)).isNull();
    }

    @Test
    @DisplayName("addMonths_加1月_日期增加1月")
    void addMonths_plusOne_returnsNextMonth() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        Date result = DateUtils.addMonths(date, 1);
        assertThat(DateUtils.format(result)).isEqualTo("2026-07-18");
    }

    // ==================== getCurrent* ====================

    @Test
    @DisplayName("getCurrentDate_返回当前日期")
    void getCurrentDate_returnsCurrentDate() {
        String result = DateUtils.getCurrentDate();
        assertThat(result).isNotNull();
        assertThat(result).matches("\\d{4}-\\d{2}-\\d{2}");
    }

    @Test
    @DisplayName("getCurrentDateTime_返回当前日期时间")
    void getCurrentDateTime_returnsCurrentDateTime() {
        String result = DateUtils.getCurrentDateTime();
        assertThat(result).isNotNull();
        assertThat(result).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    }

    @Test
    @DisplayName("getCurrentTimestamp_返回时间戳格式")
    void getCurrentTimestamp_returnsTimestampFormat() {
        String result = DateUtils.getCurrentTimestamp();
        assertThat(result).isNotNull();
        assertThat(result).matches("\\d{14}");
    }

    @Test
    @DisplayName("getCurrentYearMonth_返回yyyy-MM格式")
    void getCurrentYearMonth_returnsYearMonthFormat() {
        String result = DateUtils.getCurrentYearMonth();
        assertThat(result).matches("\\d{4}-\\d{2}");
    }

    @Test
    @DisplayName("getCurrentYear_返回yyyy格式")
    void getCurrentYear_returnsYearFormat() {
        String result = DateUtils.getCurrentYear();
        assertThat(result).matches("\\d{4}");
    }

    // ==================== getDaysBetween ====================

    @Test
    @DisplayName("getDaysBetween_相差3天_返回3")
    void getDaysBetween_threeDays_returnsThree() throws ParseException {
        Date start = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-15");
        Date end = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        assertThat(DateUtils.getDaysBetween(start, end)).isEqualTo(3L);
    }

    @Test
    @DisplayName("getDaysBetween_null参数_返回0")
    void getDaysBetween_nullParam_returnsZero() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        assertThat(DateUtils.getDaysBetween(null, date)).isEqualTo(0L);
        assertThat(DateUtils.getDaysBetween(date, null)).isEqualTo(0L);
    }

    // ==================== isBetween / isValidDate ====================

    @Test
    @DisplayName("isBetween_日期在范围内_返回true")
    void isBetween_inRange_returnsTrue() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        Date start = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-01");
        Date end = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-30");
        assertThat(DateUtils.isBetween(date, start, end)).isTrue();
    }

    @Test
    @DisplayName("isBetween_日期不在范围内_返回false")
    void isBetween_outOfRange_returnsFalse() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-07-01");
        Date start = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-01");
        Date end = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-30");
        assertThat(DateUtils.isBetween(date, start, end)).isFalse();
    }

    @Test
    @DisplayName("isValidDate_合法日期_返回true")
    void isValidDate_validDate_returnsTrue() {
        assertThat(DateUtils.isValidDate("2026-06-18")).isTrue();
    }

    @Test
    @DisplayName("isValidDate_非法日期_返回false")
    void isValidDate_invalidDate_returnsFalse() {
        assertThat(DateUtils.isValidDate("2026-13-01")).isFalse();
        assertThat(DateUtils.isValidDate("abc")).isFalse();
        assertThat(DateUtils.isValidDate("")).isFalse();
    }

    // ==================== offsetDay ====================

    @Test
    @DisplayName("offsetDay_加3天_返回3天后日期")
    void offsetDay_plusThree_returnsThreeDaysLater() {
        assertThat(DateUtils.offsetDay("2026-06-18", 3)).isEqualTo("2026-06-21");
    }

    @Test
    @DisplayName("offsetDay_减3天_返回3天前日期")
    void offsetDay_minusThree_returnsThreeDaysEarlier() {
        assertThat(DateUtils.offsetDay("2026-06-18", -3)).isEqualTo("2026-06-15");
    }

    @Test
    @DisplayName("offsetDay_空字符串_返回null")
    void offsetDay_emptyString_returnsNull() {
        assertThat(DateUtils.offsetDay("", 1)).isNull();
    }

    // ==================== getYear / getMonth / getDay ====================

    @Test
    @DisplayName("getYear_有效日期_返回年份")
    void getYear_validDate_returnsYear() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        assertThat(DateUtils.getYear(date)).isEqualTo(2026);
    }

    @Test
    @DisplayName("getMonth_有效日期_返回月份")
    void getMonth_validDate_returnsMonth() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        assertThat(DateUtils.getMonth(date)).isEqualTo(6);
    }

    @Test
    @DisplayName("getDay_有效日期_返回天数")
    void getDay_validDate_returnsDay() throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-06-18");
        assertThat(DateUtils.getDay(date)).isEqualTo(18);
    }
}
