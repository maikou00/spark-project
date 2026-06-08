package com.sziov.gacnev.utils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DateUtils} 测试用例。
 */
@DisplayName("DateUtils 日期工具测试")
class DateUtilsTest {

    @Test
    @DisplayName("日期格式化为 yyyy-MM-dd")
    void format() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.format(date)).isEqualTo("2026-06-07");
    }

    @Test
    @DisplayName("自定义格式")
    void formatWithPattern() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.format(date, "yyyy/MM/dd")).isEqualTo("2026/06/07");
    }

    @Test
    @DisplayName("字符串解析为日期")
    void parse() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(date).isNotNull();
        assertThat(DateUtils.format(date)).isEqualTo("2026-06-07");
    }

    @Test
    @DisplayName("空字符串解析应返回 null")
    void parseBlankShouldReturnNull() {
        assertThat(DateUtils.parse(null)).isNull();
        assertThat(DateUtils.parse("")).isNull();
    }

    @Test
    @DisplayName("无效日期字符串应返回 null")
    void parseInvalidShouldReturnNull() {
        assertThat(DateUtils.parse("not-a-date")).isNull();
    }

    @Test
    @DisplayName("日期天数加减")
    void addDays() {
        Date date = DateUtils.parse("2026-06-07");
        Date result = DateUtils.addDays(date, 3);
        assertThat(DateUtils.format(result)).isEqualTo("2026-06-10");

        result = DateUtils.addDays(date, -3);
        assertThat(DateUtils.format(result)).isEqualTo("2026-06-04");
    }

    @Test
    @DisplayName("日期差（天数）")
    void getDaysBetween() {
        Date start = DateUtils.parse("2026-06-01");
        Date end = DateUtils.parse("2026-06-10");
        assertThat(DateUtils.getDaysBetween(start, end)).isEqualTo(9);
    }

    @Test
    @DisplayName("判断日期是否在范围内")
    void isBetween() {
        Date date = DateUtils.parse("2026-06-07");
        Date start = DateUtils.parse("2026-06-01");
        Date end = DateUtils.parse("2026-06-30");
        assertThat(DateUtils.isBetween(date, start, end)).isTrue();
        assertThat(DateUtils.isBetween(DateUtils.parse("2026-05-31"), start, end)).isFalse();
    }

    @Test
    @DisplayName("判断日期字符串是否合法")
    void isValidDate() {
        assertThat(DateUtils.isValidDate("2026-06-07")).isTrue();
        assertThat(DateUtils.isValidDate("2026-13-01")).isFalse();
        assertThat(DateUtils.isValidDate("")).isFalse();
    }

    @Test
    @DisplayName("分区日期偏移")
    void offsetDay() {
        assertThat(DateUtils.offsetDay("2026-06-07", 1)).isEqualTo("2026-06-08");
        assertThat(DateUtils.offsetDay("2026-06-07", -7)).isEqualTo("2026-05-31");
    }

    @Test
    @DisplayName("获取年份/月份/天数")
    void getYearMonthDay() {
        Date date = DateUtils.parse("2026-06-07");
        assertThat(DateUtils.getYear(date)).isEqualTo(2026);
        assertThat(DateUtils.getMonth(date)).isEqualTo(6);
        assertThat(DateUtils.getDay(date)).isEqualTo(7);
    }
}
