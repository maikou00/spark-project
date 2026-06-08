package com.sziov.gacnev.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DateUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("DateUtils 日期工具测试")
class DateUtilsTest {

    @Test
    @DisplayName("format_有效日期_返回yyyy-MM-dd格式")
    void format_validDate_returnsDefaultPattern() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.format(date)).isEqualTo("2026-06-07");
    }

    @Test
    @DisplayName("format_null输入_返回null")
    void format_nullInput_returnsNull() {
        assertThat(DateUtils.format((Date) null)).isNull();
    }

    @Test
    @DisplayName("format_自定义格式_返回对应格式字符串")
    void format_customPattern_returnsFormattedString() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.format(date, "yyyy/MM/dd")).isEqualTo("2026/06/07");
        assertThat(DateUtils.format(date, "yyyyMMdd")).isEqualTo("20260607");
    }

    @Test
    @DisplayName("parse_有效日期字符串_返回Date对象")
    void parse_validDateString_returnsDate() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(date).isNotNull();
        assertThat(DateUtils.format(date)).isEqualTo("2026-06-07");
    }

    @Test
    @DisplayName("parse_null或空字符串_返回null")
    void parse_nullOrBlank_returnsNull() {
        assertThat(DateUtils.parse(null)).isNull();
        assertThat(DateUtils.parse("")).isNull();
    }

    @Test
    @DisplayName("parse_无效字符串_返回null")
    void parse_invalidString_returnsNull() {
        assertThat(DateUtils.parse("not-a-date")).isNull();
    }

    @Test
    @DisplayName("addDays_正数天数_日期向后推移")
    void addDays_positiveValue_dateMovesForward() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.format(DateUtils.addDays(date, 3))).isEqualTo("2026-06-10");
    }

    @Test
    @DisplayName("addDays_负数天数_日期向前推移")
    void addDays_negativeValue_dateMovesBackward() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.format(DateUtils.addDays(date, -3))).isEqualTo("2026-06-04");
    }

    @Test
    @DisplayName("getDaysBetween_两个日期_返回相差天数")
    void getDaysBetween_twoDates_returnsDayDifference() {
        Date start = DateUtils.parse("2026-06-01");
        Date end = DateUtils.parse("2026-06-10");
        assertThat(DateUtils.getDaysBetween(start, end)).isEqualTo(9);
    }

    @Test
    @DisplayName("isBetween_日期在范围内_返回true")
    void isBetween_dateWithinRange_returnsTrue() {
        Date date = DateUtils.parse("2026-06-07");
        Date start = DateUtils.parse("2026-06-01");
        Date end = DateUtils.parse("2026-06-30");
        assertThat(DateUtils.isBetween(date, start, end)).isTrue();
    }

    @Test
    @DisplayName("isBetween_日期在范围外_返回false")
    void isBetween_dateOutsideRange_returnsFalse() {
        Date start = DateUtils.parse("2026-06-01");
        Date end = DateUtils.parse("2026-06-30");
        assertThat(DateUtils.isBetween(DateUtils.parse("2026-05-31"), start, end)).isFalse();
        assertThat(DateUtils.isBetween(DateUtils.parse("2026-07-01"), start, end)).isFalse();
    }

    @Test
    @DisplayName("isValidDate_合法日期_返回true")
    void isValidDate_validDate_returnsTrue() {
        assertThat(DateUtils.isValidDate("2026-06-07")).isTrue();
        assertThat(DateUtils.isValidDate("2026-12-31")).isTrue();
    }

    @Test
    @DisplayName("isValidDate_非法日期_返回false")
    void isValidDate_invalidDate_returnsFalse() {
        assertThat(DateUtils.isValidDate("2026-13-01")).isFalse();
        assertThat(DateUtils.isValidDate("")).isFalse();
        assertThat(DateUtils.isValidDate(null)).isFalse();
    }

    @Test
    @DisplayName("offsetDay_分区日期偏移_返回偏移后日期")
    void offsetDay_partitionOffset_returnsOffsetDay() {
        assertThat(DateUtils.offsetDay("2026-06-07", 1)).isEqualTo("2026-06-08");
        assertThat(DateUtils.offsetDay("2026-06-07", -7)).isEqualTo("2026-05-31");
    }

    @Test
    @DisplayName("getYearMonthDay_日期对象_返回年月日分量")
    void getYearMonthDay_dateObject_returnsComponents() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.getYear(date)).isEqualTo(2026);
        assertThat(DateUtils.getMonth(date)).isEqualTo(6);
        assertThat(DateUtils.getDay(date)).isEqualTo(7);
    }

    @Test
    @DisplayName("getCurrentDate_无参调用_返回当日分区字符串")
    void getCurrentDate_noArgs_returnsTodayPartition() {
        String today = DateUtils.getCurrentDate();
        assertThat(today).isNotNull();
        assertThat(today).matches("\\d{4}-\\d{2}-\\d{2}");
    }
}
